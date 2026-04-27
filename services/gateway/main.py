import os
import io
import httpx
import numpy as np
import uuid
import json
from celery import Celery

from fastapi import FastAPI, HTTPException, Depends, UploadFile, File
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sentence_transformers import SentenceTransformer
from pydantic import BaseModel, HttpUrl, ConfigDict
from typing import Optional, List

from sqlalchemy import Column, String, Integer, Text, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pgvector.sqlalchemy import Vector

from umap import UMAP
from sklearn.cluster import KMeans





OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
celery_app = Celery("aletheia_tasks", broker=REDIS_URL, backend=REDIS_URL)

print("Director: Loading the BGE Faculty for real-time perception...")
model = SentenceTransformer('BAAI/bge-small-en-v1.5')

#database krke dekhte
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


#the main model
class IngestionRecord(Base):
    __tablename__="ingestions"
    id = Column(String, primary_key=True, index=True)
    user_id = Column(String)
    content = Column(Text, nullable=True)
    source_url = Column(String, nullable=True)
    embedding = Column(Vector(384))
    collection = Column(String, default="general", index=True)

class IngestionRequest(BaseModel):
    source_url: Optional[HttpUrl] = None
    content: str
    user_id: str = "guest_aryan"

class IngestionResponse(BaseModel):
    id: str
    user_id: str
    content: str
    source_url: Optional[str]
    model_config=ConfigDict(from_attributes=True)
    
class SearchRequest(BaseModel):
    query: str
    limit: int = 5
    collection: str = "general"

with engine.connect() as connection:
    connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    connection.commit()

# Build the blueprint
Base.metadata.create_all(bind=engine)
print("Director: Schema confirmed. The Ledger is ready.")

app = FastAPI(
    title="Aletheia Sovereign Engine",
    description="Enterprise Knowledge Synthesis Gateway",
    version="0.4.0"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try: 
        yield db
    finally:
        db.close()

@app.get("/constellations")
def get_constellation(db:Session = Depends(get_db)):
    """
    HAHAHA SABKUCH TARA HAI
    """
    records = db.query(IngestionRecord).all()
    if len(records) < 10:
        raise HTTPException(
            status_code=400,
            detail="Archives need more data"
        )
    embeddings=np.array([r.embedding for r in records])

    reducer = UMAP(n_components=3, n_neighbors=15, min_dist=0.1, metric='cosine', random_state=42)
    coords_3d = reducer.fit_transform(embeddings)

    num_cluster = min(len(records) // 5+1, 8)
    kmeans = KMeans(n_clusters=num_cluster, n_init=10)
    clusters = kmeans.fit_predict(coords_3d)

    points = []
    for i, r in enumerate(records):
        points.append({
            "id": r.id,
            "text": r.content[:100] + "...",
            "pos": coords_3d[i].tolist(),
            "cluster": int(clusters[i])
                    
        })

    return {"points": points, "clusters_found": num_cluster}

@app.post("/ingest", status_code=202)
async def ingest_data(request: IngestionRequest):
    job_id = str(uuid.uuid4())
    celery_app.send_task(
        "tasks.process_ingestion",
        args=[job_id, request.user_id, request.content, str(request.source_url) if request.source_url else None]
    )
    return{"id": job_id, "status": "Avvepted for backrgound processing"}



@app.post("/search")
def semantic_search(query: str, db: Session = Depends(get_db)):
  
  pass

        


@app.post("/synthesize")
def synthesize_knowledge(request: SearchRequest):
    """
    The Director commissions a Synthesis.
    It waits (blocks) for the Worker to return the result from the 'Voice'.
    """
    try:
        task = celery_app.send_task(
            "tasks.process_synthesis",
              args=[request.query],
              queue="synthesis_queue"
        )
        result = task.get(timeout=120) 
        judge_task = celery_app.send_task(
            "tasks.judge_integrity",
            args=[result['context'], result['answer']],
            queue="judge_queue"
        )

        return{
            "answer": result['answer'],
            "judge_id": judge_task.id,
            "status": "Synthesized and under review"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
        
    
@app.post("/upload")
async def upload_document(
    file: UploadFile = File(...),
    user_id: str="Aryan",
    collection: str = "general"
):
    try:
        content = await file.read()
        filename = file.filename

        task = celery_app.send_task(
            "tasks.process_document", 
            args=[filename, content, user_id, collection],
            queue="ingestion_queue")

        return {
            "status": "Document queued for processing",
            "task_id": task.id,
            "filename": filename,
            "collection": collection
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read: {str(e)}")
    


@app.post("/stream")
async def stream_synthesis(request: SearchRequest, collection: str = "general", db: Session = Depends(get_db)):
    """
    Async Synthesis
    Lets have near zero latency via SSE
    """

    query_vector = model.encode(request.query).tolist()
    records = db.query(IngestionRecord).filter(
        IngestionRecord.collection == collection
    ).order_by(
        IngestionRecord.embedding.cosine_distance(query_vector)
    ).limit(5).all()

    active_star_ids = [r.id for r in records]

    if not records:
        async def silent_generator():
            yield "data: {\"token\": \"The archives are silent on this matter.\", \"done\": true}\n\n"
        return StreamingResponse(silent_generator(), media_type="text/event-stream")
    
    context = '\n'.join([r.content for r in records])
    prompt = f"Context: {context}\n\nQuestion: {request.query}"

    async def token_generator():
        yield f"data: {json.dumps({'active_stars': active_star_ids})}\n\n"
        full_response = ""
        async with httpx.AsyncClient(timeout=120.0) as client:
            async with client.stream(
                "POST",
                f"{OLLAMA_URL}/api/generate",
                json={"model": "llama3.2:1b", "prompt": prompt, "stream": True}
            ) as response:
                async for line in response.aiter_lines():
                    if line:
                        chunk = json.loads(line)
                        token = chunk.get("response", "")
                        full_response += token
                        yield f"data: {json.dumps({'token': token})}\n\n"
                        if chunk.get("done"):
                            judge_task = celery_app.send_task(
                                "tasks.judge_integrity",
                                args=[context, full_response],
                                queue="judge_queue"
                            )
    return StreamingResponse(token_generator(), media_type="text/event-stream")


@app.get("/verdict/{task_id}")
def get_judge_verdict(task_id: str):
    """
    CHeck if High COurt is open for peasants
    """
    res=celery_app.AsyncResult(task_id)
    if res.ready():
        return res.result
    return{"status": "Deliverating...."}
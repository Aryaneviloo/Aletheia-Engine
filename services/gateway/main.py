import os
import io
import httpx
import numpy as np
import uuid
import matplotlib.pyplot as plt
import json
from celery import Celery

from fastapi import FastAPI, HTTPException, Depends, UploadFile, File
from fastapi.responses import StreamingResponse
from sentence_transformers import SentenceTransformer
from pydantic import BaseModel, HttpUrl, ConfigDict
from typing import Optional, List

from sqlalchemy import Column, String, Integer, Text, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pgvector.sqlalchemy import Vector
from sklearn.decomposition import PCA

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

def get_db():
    db = SessionLocal()
    try: 
        yield db
    finally:
        db.close()

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

        
@app.get("/visualize")
def visualize_knowledge(db: Session = Depends(get_db)):
    records = db.query(IngestionRecord).all()
    if len(records) < 2:
        raise HTTPException(status_code=400, detail="Insufficient")
    
    embeddings=np.array([r.embedding for r in records])
    labels=[r.content[:30] + "..." for r in records]

    pca=PCA(n_components=2)
    coords=pca.fit_transform(embeddings)

    plt.figure(figsize=(10,6))
    plt.scatter(coords[:, 0], coords[:, 1], c='blue', edgecolors='white')
    for i, label in enumerate(labels):
        plt.annotate(label, (coords[i, 0], coords[i, 1]), fontsize=8, alpha=0.7)
    
    plt.title("Aletheia Knowlegde Constellations")
    plt.grid(True, linestyle='--', alpha = 0.6)

    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    return StreamingResponse(buf, media_type="image/png")

@app.post("/synthesize")
def synthesize_knowledge(request: SearchRequest):
    """
    The Director commissions a Synthesis.
    It waits (blocks) for the Worker to return the result from the 'Voice'.
    """
    try:
        # 1. Dispatch the task to the Fabric
        # We use .get(timeout=) because Synthesis is a synchronous demand for the user
        task = celery_app.send_task("tasks.process_synthesis", args=[request.query])
        
        # 2. Await the Worker's labor (Ollama takes time)
        answer = task.get(timeout=120) 
        
        return {"answer": answer, "status": "Synthesized from background labor"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"The Worker failed to synthesize: {str(e)}")
    
@app.post("/upload")
async def upload_document(file: UploadFile = File(...), user_id: str="Aryan"):
    try:
        content = await file.read()
        filename = file.filename

        task = celery_app.send_task("tasks.process_document", args=[filename, content, user_id])

        return {
            "status": "Document queued for processing",
            "task_id": task.id,
            "filename": filename
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read: {str(e)}")
    


@app.post("/stream")
async def stream_synthesis(request: SearchRequest, db: Session = Depends(get_db)):
    """
    Async Synthesis
    Lets have near zero latency via SSE
    """

    query_vector = model.encode(request.query).tolist()
    records = db.query(IngestionRecord).order_by(
        IngestionRecord.embedding.cosine_distance(query_vector)
    ).limit(3).all()

    if not records:
        async def silent_generator():
            yield "data: {\"token\": \"The archives are silent on this matter.\", \"done\": true}\n\n"
        return StreamingResponse(silent_generator(), media_type="text/event-stream")
    
    context = '\n'.join([r.content for r in records])
    prompt = f"Context: {context}\n\nQuestion: {request.query}"

    async def token_generator():
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
                        yield f"data: {json.dumps({'token': token})}\n\n"
                        if chunk.get("done"):
                            break
    return StreamingResponse(token_generator(), media_type="text/event-stream")
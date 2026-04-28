
import os
import io
import httpx
import numpy as np
import uuid
import json
import datetime
from celery import Celery

from fastapi import FastAPI, HTTPException, Depends, UploadFile, File
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sentence_transformers import SentenceTransformer, CrossEncoder
from pydantic import BaseModel, HttpUrl, ConfigDict
from typing import Optional, List

from sqlalchemy import Column, String, Integer, Text, create_engine, text, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pgvector.sqlalchemy import Vector
from sqlalchemy import DateTime


from umap import UMAP
from sklearn.cluster import KMeans

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
celery_app = Celery("aletheia_tasks", broker=REDIS_URL, backend=REDIS_URL)

print("Director: Loading the BGE Faculty for real-time perception...")
model = SentenceTransformer('BAAI/bge-small-en-v1.5')
alchemist = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')

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

class Dialogue(Base):
    __tablename__ = "dialogues"
    id = Column(Integer, primary_key=True)
    session_id = Column(String, index=True)
    role = Column(String) # 'user' or 'assistant'
    content = Column(Text)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)

@event.listens_for(Base.metadata, "before_create")
def create_vector_extension(target, connection, **kw):
    connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))

try:
    print("Director: Establishing Imperial Ledger...")
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        print("Optimizing traversal using HSNW Index")
        conn.execute(text("""
                          CREATE INDEX IF NOT EXISTS idx_ingestions_embedding 
            ON ingestions USING hnsw (embedding vector_cosine_ops) 
            WITH (m = 16, ef_construction = 64); 
                         """ ))
        
    Base.metadata.create_all(bind=engine)
    print("Director: Schema confirmed. Vector faculty active.")
except Exception as e:
    print(f"Director: Initialization Warning: {e}")


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
    collection: List[str] = []
    session_id: str = "default_session"



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


async def generate_blueprint(query: str, availaible_provinces: List[str]):
    """
    The Strategist analyzes the query and maps it to the Empire's provinces.
    """
    prompt = f"""
    [SYSTEM: IMPERIAL STRATEGIST]
    Your task is to decompose a complex query into sub-inquiries for specific knowledge provinces.
    AVAILABLE PROVINCES: {available_provinces}

    USER QUERY: "{query}"

    Respond ONLY in raw JSON format:
    {{
      "provinces": ["list of relevant provinces"],
      "sub_queries": [
        {{"province": "name", "question": "targeted question for this province"}}
      ]
    }}
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        try: 
            resp = await client.post(
                f"{OLLAMA_URL}/api/generate",
                json={
                    "model": "llama3.2:1b",
                    "prompt": prompt,
                    "stream": False,
                    "format": json,
                    "options": {"temperature": 0, "num_predict": 128, "num_thread": 8},
                    "keep_alive": "60m"
                }
            )

            blueprint_str = resp.json().get("response", "{}")
            return json.loads(blueprint_str)
        except Exception as e:
            print("Strategist Error{e} getting back to general")
            return{"provinces": ["general"], "sub_queries": [{"province": "general",
                                                              "question": query}]}

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
              args=[request.query, request.collection],
              queue="synthesis_queue"
        )
        result = task.get(timeout=120)
        context = result['context']
        initial_answer = result['answer']

        judge_task = celery_app.send_task(
            "tasks.judge_integrity",
            args=[context, initial_answer],
            queue="judge_queue"
        )
        verdict = judge_task.get(timeout=120)
        faithfulness = verdict.get('faithfulness_score', 0)

        if faithfulness < 0.6:
            print(f"Sovereign Alert: Integrity at {faithfulness}. Demanding Correction...")
        
             # We craft a "Refining Fire" prompt
            correction_query = f"""
            [SYSTEM: RECURSIVE CORRECTION]
            The High Court found your previous response lacked archival integrity.
            REASONING: {verdict.get('reasoning')}
            HALLUCINATIONS DETECTED: {verdict.get('hallucinations')}
        
            ORIGINAL QUESTION: {request.query}
        
            TASK: Synthesize a new answer. Be hyper-literal. 
            If the context does not support a claim, omit it.
             """
            retry_task = celery_app.send_task(
                "tasks.process_synthesis",
                args=[correction_query, request.collection])
            retry_result = retry_task.get(timeout=120)

            final_judge_task = celery_app.send_task(
                "tasks.judge_integrity",
                args=[context, retry_result['answer']]
            )
            final_verdict = final_judge_task.get(timeout=60)

            return{
                "answer": retry_result['answer'],
                "verdict": final_verdict,
                "iterations": 2,
                "status": "Purified by Recursive Justice"
            }
        
        return{
            "answer": initial_answer,
            "verdict": verdict,
            "iterations": 2,
            "status": "purified by judge again"
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
async def stream_synthesis(
    request: SearchRequest,
    session_id: str = "default_session",
    db: Session = Depends(get_db)
):
    """
    Async Synthesis
    Lets have near zero latency via SSE
    """
    history_records = db.query(Dialogue).filter(
        Dialogue.session_id == session_id
    ).order_by(Dialogue.timestamp.desc()).limit(5).all()
    chat_history = [{"role": h.role, "content": h.content} for h in reversed(history_records)]
    

    # II. STRATEGIST: Decide Provinces
    all_p = [c[0] for c in db.query(IngestionRecord.collection).distinct().all()]
    if not request.collections:
        blueprint = await generate_blueprint(request.query, all_p)
    else:
        blueprint = {"provinces": request.collections, 
                     "sub_queries": [{"province": p, "question": request.query} for p in request.collections]}


    merged_candidates=[]
    for sub in blueprint.get('sub_queries', []):
        sub_vector = model.encode(sub['question']).tolist()
        shards = db.query(IngestionRecord).filter(
            IngestionRecord.collection == sub['province']
        ).order_by(IngestionRecord.embedding.cosine_distance(sub_vector)).limit(10).all()
        merged_candidates.extend(shards)

    if not merged_candidates:
        async def silent_gen(): yield f"data: {json.dumps({'token': 'The archives are silent.'})}\n\n"
        return StreamingResponse(silent_gen(), media_type="text/event-stream")

    pairs = [[request.query, c.content] for c in merged_candidates]
    scores = alchemist.predict(pairs)
    scored_candidates = sorted(zip(scores, merged_candidates), key=lambda x: x[0], reverse=True)
    records = [c for score, c in scored_candidates[:5]]
    
    active_star_ids = [r.id for r in records]
    context = '\n'.join([r.content for r in records])
    history_str = "\n".join([f"{m['role'].upper()}: {m['content']}" for m in chat_history])
   
    prompt = f"""
[SYSTEM: SOVEREIGN VOICE]
Treat the CONTEXT as your primary reality. Use the CONVERSATION HISTORY for continuity.

[ARCHIVAL CONTEXT]
{context}

[CONVERSATION HISTORY]
{history_str}

[CURRENT INQUIRY]
USER: {request.query}

ASSISTANT:"""
    
    db.add(Dialogue(session_id=session_id, role="user", content=request.query))
    db.commit()
    
    async def token_generator():
        yield f"data: {json.dumps({'active_stars': active_star_ids})}\n\n"
        full_response = ""

        async with httpx.AsyncClient(timeout=120.0) as client:
            async with client.stream(
                "POST",
                f"{OLLAMA_URL}/api/generate",
                json={"model": "llama3.2:1b",
                       "prompt": prompt,
                        "stream": True,
                        "options": {"num_thread": 8, "temperature": 0.3},
                        "keep_alive": "60m"}
            ) as response:
                async for line in response.aiter_lines():
                    if line:
                        chunk = json.loads(line)
                        token = chunk.get("response", "")
                        full_response += token
                        yield f"data: {json.dumps({'token': token})}\n\n"

                        if chunk.get("done"):

                            with SessionLocal() as final_db:
                                assistant_msg = Dialogue(
                                    session_id=session_id,
                                    role="assistant",
                                    content=full_response
                                )
                                final_db.add(assistant_msg)
                                final_db.commit()

                            judge_task = celery_app.send_task(
                                "tasks.judge_integrity",
                                args=[context, full_response],
                                queue="judge_queue"
                            )
                            yield f"data: {json.dumps({'done': True, 'judge_id': judge_task.id})}\n\n"
                            break
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
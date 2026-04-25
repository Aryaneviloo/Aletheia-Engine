import os
import io
import httpx
import numpy as np
import uuid
import matplotlib.pyplot as plt

from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, HttpUrl, ConfigDict
from typing import Optional, List

from sqlalchemy import Column, String, Integer, Text, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pgvector.sqlalchemy import Vector
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA



#database krke dekhte
DATABASE_URL = os.getenv("DATABASE_URL")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

print("Loading BGE MODEL into memory....")
model = SentenceTransformer('BAAI/bge-small-en-v1.5')

#the main model
class IngestionRecord(Base):
    __tablename__="ingestions"
    id = Column(String, primary_key=True, index=True)
    user_id = Column(String)
    content = Column(Text, nullable=True)
    source_url = Column(String, nullable=True)
    embedding = Column(Vector(384))

with engine.connect() as connection:
    connection.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    connection.commit()
Base.metadata.create_all(bind=engine)


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

app = FastAPI(
    title="Aletheia Sovereign Engine",
    description="Enterprise Knowledge Synthesis Gateway",
    version="0.3.0"
)

def get_db():
    db = SessionLocal()
    try: 
        yield db
    finally:
        db.close()

@app.post("/ingest", status_code=201, response_model=IngestionResponse)
async def ingest_data(request: IngestionRequest, db: Session = Depends(get_db)):
    job_id = str(uuid.uuid4())

    vector_coords = model.encode(request.content).tolist()
    new_record = IngestionRecord(
        id=job_id,
        user_id=request.user_id,
        content=request.content,
        source_url=str(request.source_url) if request.source_url else None,
        embedding=vector_coords
    )
    db.add(new_record)
    db.commit()
    db.refresh(new_record)
    return new_record

@app.get("/records", response_model=List[IngestionResponse])
def get_all_records(db: Session = Depends(get_db)):
    return db.query(IngestionRecord).all()
@app.post("/search", response_model=List[IngestionResponse])

def semantic_search(request: SearchRequest, db: Session = Depends(get_db)):

    query_vector = model.encode(request.query).tolist()
    results = db.query(IngestionRecord).order_by(
        IngestionRecord.embedding.cosine_distance(query_vector)
    ).limit(request.limit).all()
    
    return results

@app.post("/synthesize")
async def synthesize_knowledge(request: SearchRequest, db: Session = Depends(get_db)):
    query_vector= model.encode(request_query).tolist()
    records = db.query(IngestionRecord).order_by(
        IngestionRecord.embedding.cosine_distance(query_vector)
    ).limit(3).all()

    if not records:
        return {"answer" : "Apologies the archives are silent"}
    
    context_text = "\n".join([f" - {r.content}" for r in records])

    prompt = f"Context_observations: \n{context_text}\n\nQuestion: {request.query} \nAnswer based only on context provided"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": "llama3.1", "prompt": prompt, "stream": False},
                timeout=60.0
            )
            return {"answer": response.json().get("response"), "sources": [r.id for r in records]}
        except Exception as e:
            raise HTTPException(status_code = 500, detail=f"Ollama unreachable: {str(e)}")
        
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

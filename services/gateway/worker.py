import os
import requests 
import httpx
import fitz
import uuid
from celery import Celery
from sentence_transformers import SentenceTransformer
from sqlalchemy import create_engine, Column, String, Text, text
from sqlalchemy.orm import sessionmaker, declarative_base
from pgvector.sqlalchemy import Vector


REDIS_URL=os.getenv("REDIS_URL", "redis://redis:6379/0")
celery_app=Celery("aletheia_tasks", broker=REDIS_URL, backend=REDIS_URL)

DATABASE_URL = os.getenv("DATABASE_URL")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class IngestionRecord(Base):
    __tablename__ = "ingestions"
    id = Column(String, primary_key=True)
    user_id = Column(String)
    content = Column(Text)
    embedding = Column(Vector(384))

print("Worker: Loading the BGE")
model=SentenceTransformer('BAAI/bge-small-en-v1.5')

@celery_app.task(name="tasks.process_ingestion")
def process_ingestion(job_id, user_id, content, source_url):
    print(f"Starting Ingestion for {job_id}")

    vector_coords = model.encode(content).tolist()
    db = SessionLocal()
    try:
        new_record = IngestionRecord(
            id=job_id,
            user_id=user_id,
            content=content,
            embedding=vector_coords
        )
        db.add(new_record)
        db.commit()
        print(f"Worker: Job {job_id} successfully etched into the DB.")
    except Exception as e:
        print(f"Worker Error: {str(e)}")
        db.rollback()
    finally:
        db.close()

@celery_app.task(name="tasks.process_synthesis")
def process_synthesis(query):
    query_vector = model.encode(query).tolist()

    db = SessionLocal()
    records = db.query(IngestionRecord).order_by(
        IngestionRecord.embedding.cosine_distance(query_vector)
    ).limit(3).all()
    db.close()
    if not records:
        return "The archives are silent on this matter."

    context = '\n'.join([r.content for r in records])
    prompt = f"Context: {context}\n\Question: {query}"

    try:
      response = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={"model": "llama3.2:1b", "prompt": prompt, "stream": False},
        timeout=90.0
      )
      answer = response.json().get("response")
      print(f"Synthesis Complete: {answer}")
      return answer
    except Exception as e:
        print(f"Ollama Communication Error: {str(e)}")
        return "The Voice is currently unreachable."
    
@celery_app.task(name="tasks.process_document")
def process_document(filename, content_bytes, user_id):
    print(f"Muscle of '{filename}' ...")
    text_content = ""

    try: 

      if filename.lower().endswith(".pdf"):
        with fitz.open(stream=content_bytes, filetype="pdf") as doc:
            for page in doc:
                text_content += page.get_text()
      else:
        text_content = content_bytes.decode('utf-8')

      chunk_size = 1200
      overlap = 200
      chunks = [text_content[i:i + chunk_size] for i in range(0, len(text_content), chunk_size - overlap)]
      print(f"Muscle: fragmented '{filename}' into {len(chunks)}")


      db = SessionLocal()
      for i, chunk in enumerate(chunks):
          embedding = model.encode(chunk).tolist()
          new_id=str(uuid.uuid4())
          new_record = IngestionRecord(
            id=new_id,
            user_id=user_id,
            content=f"[{filename} | Chunk{i}] {chunk}",
            embedding=embedding
          )
          db.add(new_record)
      db.commit()
      db.close()
      print(f"Muscle '{filename}' added to the base")
      return f"Processed {len(chunks)} fragments."

    except Exception as e:
        print(f"Dissection Error on {filename}: {str(e)}")
        return f"Failure: {str(e)}"
    



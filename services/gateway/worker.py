import os
import requests 
import httpx
import fitz
import uuid
import json
from celery import Celery
from sentence_transformers import SentenceTransformer
from sqlalchemy import create_engine, Column, String, Text, text
from sqlalchemy.orm import sessionmaker, declarative_base
from pgvector.sqlalchemy import Vector
from sentence_transformers import CrossEncoder


#   Configuration

REDIS_URL=os.getenv("REDIS_URL", "redis://redis:6379/0")
celery_app=Celery("aletheia_tasks", broker=REDIS_URL, backend=REDIS_URL)

DATABASE_URL = os.getenv("DATABASE_URL")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

#  MODELS

class IngestionRecord(Base):
    __tablename__ = "ingestions"
    id = Column(String, primary_key=True)
    user_id = Column(String)
    content = Column(Text)
    embedding = Column(Vector(384))
    collection = Column(String, default="general", index=True)

print("Worker: Loading the BGE")
model=SentenceTransformer('BAAI/bge-small-en-v1.5')
print("Worker: The Alchemist is needed")
alchemist = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')


# TASKS


@celery_app.task(name="tasks.process_ingestion")
def process_ingestion(job_id, user_id, content, source_url, collection="general"):
    print(f"Starting Ingestion for {job_id} in province: {collection}")

    vector_coords = model.encode(content).tolist()
    db = SessionLocal()
    try:
        new_record = IngestionRecord(
            id=job_id,
            user_id=user_id,
            content=content,
            embedding=vector_coords,
            collection=collection
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
def process_synthesis(query, collection="general"):
    query_vector = model.encode(query).tolist()

    db = SessionLocal()
    candidates = db.query(IngestionRecord).filter(
        IngestionRecord.collection == collection
    ).order_by(IngestionRecord.embedding.cosine_distance(query_vector)).limit(15).all()
    db.close()

    if not candidates:
        return "The archives are silent on this matter."
    
    pairs = [[query, c.content] for c in candidates]
    scores = alchemist.predict(pairs)
    scored_candidates = sorted(zip(scores, candidates), key=lambda x: x[0], reverse=True)
    refined_shards = [c for score, c in scored_candidates[:3]]

    context = '\n'.join([r.content for r in refined_shards])
    prompt = f"Context: {context}\n\Question: {query}"

    try:
      response = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={"model": "llama3.2:1b", "prompt": prompt, "stream": False, "options": {"num_thread": 8, "temperature": 0.3},
                "keep_alive": "60m"},
        timeout=90.0
      )
      answer = response.json().get("response")
      print(f"Synthesis Complete: {answer}")
      return {"answer": answer, "context": context}
    except Exception as e:
        print(f"Ollama Communication Error: {str(e)}")
        return "The Voice is currently unreachable."
    
@celery_app.task(name="tasks.process_document")
def process_document(filename, content_bytes, user_id, collection="general"):
    print(f"Muscle of '{filename}' ...")
    text_content = ""

    try: 

      if filename.lower().endswith(".pdf"):
        with fitz.open(stream=content_bytes, filetype="pdf") as doc:
            for page in doc:
                text_content += page.get_text()
      else:
        text_content = content_bytes.decode('utf-8')

      separators = ["\n\n", "\n", ".", " ", ""]
      chunk_size=1000
      overlap=150

      final_chunks = []
      def recursive_split(text, seps):
          if len(text) <= chunk_size:
              return[text]
          
          for sep in seps:
              if sep in text:
                  parts = text.split(sep)

                  current_group = ""
                  results = []
                  for p in parts:
                      if len(current_group) + len(p) + len(sep) <= chunk_size:
                          current_group += (sep if current_group else "") + p
                      else:
                          if current_group: results.append(current_group)
                          current_group=p
                  if current_group: results.append(current_group)
                  return results        
          return [text[:chunk_size]]   
      
      chunks = recursive_split(text_content, separators)

      db = SessionLocal()
      for i, chunk in enumerate(chunks):
          embedding = model.encode(chunk).tolist()
          new_id=str(uuid.uuid4())
          new_record = IngestionRecord(
            id=new_id,
            user_id=user_id,
            content=f"[{filename} | Fragment {i}] {chunk.strip()}",
            embedding=embedding,
            collection=collection
          )
          db.add(new_record)
      db.commit()
      db.close()
      print(f"Muscle '{filename}' added to the base")
      return f"Processed {len(chunks)} fragments."

    except Exception as e:
        print(f"Dissection Error on {filename}: {str(e)}")
        return f"Failure: {str(e)}"
    


@celery_app.task(name="tasks.judge_integrity")
def judge_integrity(context, synthesis):
    """
    THE COURT OF TRUTH HAS RISEN UP
    """
    judge_prompt = f"""
    [SYSTEM: SOVEREIGN JUDGE]
    CRITICAL INSTRUCTION: Your only source of truth is the provided CONTEXT. 
    1. Ignore all your internal knowledge. 
    2. If a claim in the RESPONSE is not supported by a specific sentence in the CONTEXT, label it "HALLUCINATION".
    3. "Steam," "Pans," and "Cooking" are NOT in the archives. Be precise.

    CONTEXT: {context}
    RESPONSE: {synthesis}

    VERDICT (JSON):
    {{
       "faithfulness_score": 0.0-1.0,
       "hallucinations": [],
       "reasoning": "Explain why based ONLY on context"
    }}
    """

    try: 
        response = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": "llama3.2:1b",
                "prompt": judge_prompt,
                "stream": False,
                "format": "json",
                "options": {"temperature": 0, "num_thread": 8},
                "keep_alive": "60m"
            },
            timeout=60
        )
        return json.loads(response.json()["response"])
    except Exception as e:
        return {"error": f"the Ccourt is closed sarkari kam hai: {str(e)}"}
    



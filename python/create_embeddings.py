import json
import os
import tiktoken
import openai
import chromadb
from chromadb.config import Settings
from uuid import uuid4
from typing import List

# === CONFIGURATION ===
openai.api_key = "your-openai-api-key"  # 🔁 Replace with your key
CHROMA_DB_DIR = "./chroma_db"
PDF_COLLECTION_NAME = "pdf_data"
STATIC_COLLECTION_NAME = "static_data"
EMBED_MODEL = "text-embedding-3-small"
CHUNK_TOKEN_LIMIT = 800
OVERLAP = 100

# === SETUP ===
client = chromadb.Client(Settings(
    chroma_db_impl="duckdb+parquet",
    persist_directory=CHROMA_DB_DIR
))
pdf_collection = client.get_or_create_collection(name=PDF_COLLECTION_NAME)
static_collection = client.get_or_create_collection(name=STATIC_COLLECTION_NAME)

# === TOKENIZER ===
enc = tiktoken.encoding_for_model(EMBED_MODEL)

def chunk_text(text: str, max_tokens: int, overlap: int = 0) -> List[str]:
    tokens = enc.encode(text)
    chunks = []
    i = 0
    while i < len(tokens):
        chunk = tokens[i:i + max_tokens]
        chunks.append(enc.decode(chunk))
        i += max_tokens - overlap
    return chunks

# === EMBEDDING ===
def embed_text(text: str) -> List[float]:
    response = openai.Embedding.create(
        model=EMBED_MODEL,
        input=text
    )
    return response["data"][0]["embedding"]

# === PROCESSING ===
def process_jsonl(filepath: str, collection, tag: str):
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line.strip())
            base_id = record.get("id", str(uuid4()))
            text = record.get("text")
            if not text:
                continue

            metadata = record.get("metadata", {})
            metadata["source"] = tag

            chunks = chunk_text(text, max_tokens=CHUNK_TOKEN_LIMIT, overlap=OVERLAP)

            for i, chunk in enumerate(chunks):
                embedding = embed_text(chunk)
                doc_id = f"{base_id}-chunk-{i}"
                collection.add(
                    documents=[chunk],
                    metadatas=[metadata],
                    embeddings=[embedding],
                    ids=[doc_id]
                )
                print(f"[{tag}] Inserted {doc_id}")

# === RUN ===
process_jsonl("mosdac_pdfs_text.jsonl", pdf_collection, tag="pdf")
process_jsonl("llm_ready_output.jsonl", static_collection, tag="static")

print("All data embedded and inserted into ChromaDB.")

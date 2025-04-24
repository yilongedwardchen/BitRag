"""
REST API for the Bitcoin RAG system.
Provides an interface for the Spring AI hot-swapping system to connect to.
"""
import os
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
import uvicorn
from rag_engine import BitcoinRAGEngine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI(title="BitRag API", description="Bitcoin RAG Analysis API")

# Initialize RAG engine
rag_engine = None

class QueryRequest(BaseModel):
    query: str
    params: Dict[str, Any] = {}

class QueryResponse(BaseModel):
    answer: str
    sources: list = []
    metadata: Dict[str, Any] = {}

@app.on_event("startup")
async def startup_event():
    global rag_engine
    try:
        logger.info("Initializing RAG engine...")
        rag_engine = BitcoinRAGEngine()
        logger.info("RAG engine initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing RAG engine: {e}")

@app.get("/")
async def root():
    return {"message": "Bitcoin RAG API is running"}

@app.get("/health")
async def health_check():
    if rag_engine is not None:
        return {"status": "healthy", "initialized": True}
    return {"status": "healthy", "initialized": False}

@app.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    global rag_engine
    if rag_engine is None:
        try:
            logger.info("Initializing RAG engine on demand...")
            rag_engine = BitcoinRAGEngine()
        except Exception as e:
            logger.error(f"Error initializing RAG engine: {e}")
            raise HTTPException(status_code=500, detail=f"RAG engine initialization failed: {str(e)}")
    
    try:
        logger.info(f"Processing query: {request.query}")
        result = rag_engine.answer_question(request.query)
        answer = result.get("result", "No answer generated")
        sources = []
        if "source_documents" in result:
            for doc in result["source_documents"]:
                if hasattr(doc, "metadata"):
                    source = {
                        "title": doc.metadata.get("title", "Unknown"),
                        "source": doc.metadata.get("source", "Unknown"),
                        "link": doc.metadata.get("link", ""),
                        "published_date": doc.metadata.get("published_date", "")
                    }
                    sources.append(source)
        return QueryResponse(
            answer=answer,
            sources=sources,
            metadata={"query_time": request.params.get("query_time", "")}
        )
    except Exception as e:
        logger.error(f"Error processing query: {e}")
        raise HTTPException(status_code=500, detail=f"Query processing failed: {str(e)}")

@app.get("/recent-price")
async def get_recent_price():
    global rag_engine
    if rag_engine is None:
        try:
            rag_engine = BitcoinRAGEngine()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"RAG engine initialization failed: {str(e)}")
    
    try:
        price_data = rag_engine.get_recent_price_data()
        return {"price_data": price_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch price data: {str(e)}")

@app.get("/recent-whales")
async def get_recent_whales():
    global rag_engine
    if rag_engine is None:
        try:
            rag_engine = BitcoinRAGEngine()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"RAG engine initialization failed: {str(e)}")
    
    try:
        whale_data = rag_engine.get_recent_whale_transactions()
        return {"whale_data": whale_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch whale data: {str(e)}")

def start():
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=False)

if __name__ == "__main__":
    start()

"""
RAG (Retrieval-Augmented Generation) implementation for Bitcoin analysis.
Uses LangChain, DeepSeek-RAG-66B, and Milvus to provide context-aware responses.
"""
import os
import logging
import pandas as pd
from pymilvus import Collection, connections
from langchain_community.vectorstores import Milvus
import psycopg2
from psycopg2.extras import RealDictCursor
from langchain_community.llms import HuggingFacePipeline
from langchain.prompts import PromptTemplate
from langchain.chains import RetrievalQA
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline, BitsAndBytesConfig
import torch
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Connection parameters
MILVUS_HOST = os.environ.get('MILVUS_HOST', 'bitrag-milvus')
MILVUS_PORT = int(os.environ.get('MILVUS_PORT', 19530))
PG_HOST = os.environ.get('PG_HOST', 'bitrag-postgres')
PG_PORT = int(os.environ.get('PG_PORT', 5432))
PG_USER = os.environ.get('PG_USER', 'bitrag')
PG_PASS = os.environ.get('PG_PASS', 'bitragpassword')
PG_DB = os.environ.get('PG_DB', 'bitrag_db')

# Model parameters
PRIMARY_MODEL = "DeepSeek/DeepSeek-RAG-66B"
FALLBACK_MODEL = "deepseek-ai/deepseek-coder-6.7b-instruct"
EMBEDDING_MODEL = 'sentence-transformers/all-MiniLM-L6-v2'

class BitcoinRAGEngine:
    def __init__(self):
        self.pg_conn = None
        self.model = None
        self.tokenizer = None
        self.llm = None
        self.retriever = None
        self.qa_chain = None
        self.setup_connections()
        self.setup_retriever()
        self.setup_llm()
        self.setup_qa_chain()
    
    def setup_connections(self, max_retries=5, retry_delay=10):
        for attempt in range(max_retries):
            try:
                self.pg_conn = psycopg2.connect(
                    host=PG_HOST,
                    port=PG_PORT,
                    user=PG_USER,
                    password=PG_PASS,
                    dbname=PG_DB
                )
                logger.info("Connected to PostgreSQL")
                connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)
                logger.info("Connected to Milvus")
                return
            except Exception as e:
                logger.error(f"Connection attempt {attempt+1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        raise Exception("Failed to connect to databases after max retries")
    
    def setup_retriever(self):
        try:
            from langchain_community.embeddings import HuggingFaceEmbeddings
            embeddings = HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL)
            collection = Collection("news_embeddings")
            collection.load()
            vector_store = Milvus(
                collection_name="news_embeddings",
                embedding_function=embeddings,
                connection_args={"host": MILVUS_HOST, "port": MILVUS_PORT}
            )
            self.retriever = vector_store.as_retriever(search_kwargs={"k": 5})
            logger.info("Retriever setup complete")
        except Exception as e:
            logger.error(f"Error setting up retriever: {e}")
            raise
    
    def setup_llm(self):
        try:
            bnb_config = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_use_double_quant=True,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_compute_dtype=torch.bfloat16
            )
            model_name = PRIMARY_MODEL
            try:
                self.tokenizer = AutoTokenizer.from_pretrained(model_name)
                self.model = AutoModelForCausalLM.from_pretrained(
                    model_name,
                    quantization_config=bnb_config,
                    device_map="auto",
                    torch_dtype=torch.bfloat16
                )
                logger.info(f"Loaded primary model: {model_name}")
            except Exception as e:
                logger.warning(f"Failed to load primary model {model_name}: {e}. Falling back to {FALLBACK_MODEL}")
                model_name = FALLBACK_MODEL
                self.tokenizer = AutoTokenizer.from_pretrained(model_name)
                self.model = AutoModelForCausalLM.from_pretrained(
                    model_name,
                    quantization_config=bnb_config,
                    device_map="auto",
                    torch_dtype=torch.bfloat16
                )
            text_pipeline = pipeline(
                "text-generation",
                model=self.model,
                tokenizer=self.tokenizer,
                max_length=2048,  # Increased for larger model
                temperature=0.1,
                top_p=0.95,
                repetition_penalty=1.15
            )
            self.llm = HuggingFacePipeline(pipeline=text_pipeline)
            logger.info("LLM setup complete")
        except Exception as e:
            logger.error(f"Error setting up LLM: {e}")
            raise
    
    def setup_qa_chain(self):
        try:
            template = """
            You are an expert Bitcoin analyst with access to real-time data and news.

            Context from recent news (if available):
            {context}

            Bitcoin price data (last 7 days, if available):
            {price_data}

            Recent whale transactions (if available):
            {whale_data}

            If any data is missing, provide an analysis based on available information and note the absence of data.
            Question: {question}

            Answer:
            """
            prompt = PromptTemplate(
                template=template,
                input_variables=["context", "price_data", "whale_data", "question"]
            )
            self.qa_chain = RetrievalQA.from_chain_type(
                llm=self.llm,
                chain_type="stuff",
                retriever=self.retriever,
                chain_type_kwargs={"prompt": prompt}
            )
            logger.info("QA chain setup complete")
        except Exception as e:
            logger.error(f"Error setting up QA chain: {e}")
            raise
    
    def get_recent_price_data(self, days=7):
        if not self.pg_conn:
            logger.error("No PostgreSQL connection available")
            return "No price data available"
        
        try:
            with self.pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT date, price, volume
                    FROM bitcoin_prices
                    WHERE date >= %s
                    ORDER BY date
                """, [(datetime.now() - timedelta(days=days)).date()])
                
                rows = cursor.fetchall()
                
                if not rows:
                    return "No recent price data available"
                
                price_text = "Date | Price (USD) | Volume (USD)\n"
                price_text += "--- | --- | ---\n"
                
                for row in rows:
                    volume = f"${row['volume']:,.2f}" if row['volume'] is not None else 'N/A'
                    price_text += f"{row['date']} | ${row['price']:,.2f} | {volume}\n"
                
                return price_text
        except Exception as e:
            logger.error(f"Error fetching price data: {e}")
            return "Error fetching price data"
    
    def get_recent_whale_transactions(self, count=5):
        if not self.pg_conn:
            logger.error("No PostgreSQL connection available")
            return "No whale transaction data available"
        
        try:
            with self.pg_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT tx_hash, timestamp, value_btc
                    FROM whale_transactions
                    ORDER BY timestamp DESC
                    LIMIT %s
                """, [count])
                
                rows = cursor.fetchall()
                
                if not rows:
                    return "No recent whale transactions available"
                
                tx_text = "Transaction | Time | BTC Value\n"
                tx_text += "--- | --- | ---\n"
                
                for row in rows:
                    tx_hash = row['tx_hash'][:10] + "..."
                    timestamp = row['timestamp'].strftime("%Y-%m-%d %H:%M") if row['timestamp'] else "Unknown"
                    tx_text += f"{tx_hash} | {timestamp} | {row['value_btc']:,.2f} BTC\n"
                
                return tx_text
        except Exception as e:
            logger.error(f"Error fetching whale transactions: {e}")
            return "Error fetching whale transactions"
    
    def answer_question(self, question):
        try:
            price_data = self.get_recent_price_data()
            whale_data = self.get_recent_whale_transactions()
            result = self.qa_chain.invoke({
                "question": question,
                "price_data": price_data,
                "whale_data": whale_data
            })
            return result
        except Exception as e:
            logger.error(f"Error answering question: {e}")
            return {"result": f"Error processing question: {str(e)}"}

if __name__ == "__main__":
    try:
        rag_engine = BitcoinRAGEngine()
        question = "What factors are influencing Bitcoin's price this week?"
        print(f"Question: {question}")
        answer = rag_engine.answer_question(question)
        print("\nAnswer:")
        print(answer["result"])
    except Exception as e:
        logger.error(f"Error in example: {e}")

# oracle_chatbot.py



import os
import json
import re  # ← Add this line
import asyncio
# ... other imports

import os
import json
import time
from datetime import datetime, timedelta
import traceback
from typing import List, Optional, Dict, Any
import hashlib
import asyncio
import concurrent.futures
import threading 
import logging
from dotenv import load_dotenv
import google.generativeai as genai
from connection import connect_to_jsondb, connect_to_vectdb, get_jsondb_connection, get_vectdb_connection

# Langchain imports
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.chains.question_answering import load_qa_chain
from langchain.prompts import PromptTemplate
from langchain_community.vectorstores import oraclevs
from langchain_community.vectorstores.utils import DistanceStrategy
from langchain.docstore.document import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter
from shared_utils import get_cache_key, prefetched_contexts, CACHE_TTL_MEDIUM

# Load environment variables
load_dotenv()

# Get API keys and other constants
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
VALID_API_KEYS = os.getenv("VALID_API_KEYS", "").split(",")
VECTDB_TABLE_NAME = os.getenv("VECTDB_TABLE_NAME", "vector_files_with_10000_chunk_new")
VM_MODE = os.getenv('VM_MODE', 'true').lower() in ('true', '1', 'yes')


# Adjust these constants based on VM mode
if VM_MODE:
    # Smaller settings for VM
    CHUNK_SIZE = 5000  # Reduced from 10000
    CHUNK_OVERLAP = 150  # Reduced from 300
    MAX_CACHE_ENTRIES = 50  # Small cache for VM
    CACHE_TTL_SHORT = 300    # 5 minutes
    CACHE_TTL_MEDIUM = 1800  # 30 minutes
    CACHE_TTL_LONG = 7200    # 2 hours
    VECTOR_CACHE_TTL = 900   # 15 minutes
else:
    # Original settings for non-VM
    CHUNK_SIZE = 10000
    CHUNK_OVERLAP = 300
    MAX_CACHE_ENTRIES = 1000
    CACHE_TTL_SHORT = 600    # 10 minutes
    CACHE_TTL_MEDIUM = 3600  # 1 hour
    CACHE_TTL_LONG = 86400   # 24 hours
    VECTOR_CACHE_TTL = 1800  # 30 minutes



# Enhanced cache configuration with tiered expiration times
response_cache = {}
vector_search_cache = {}
embedding_cache = {}  # Cache for embeddings to avoid recomputing
model_cache = {}      # Cache for model instances


# Global embeddings instance for reuse
_global_embeddings = None

# Cache lock for thread safety
cache_lock = threading.RLock()
_embeddings_lock = threading.Lock()

# Data Models
class Message:
    def __init__(self, role: str, content: str, timestamp: datetime = None):
        self.role = role
        self.content = content
        self.timestamp = timestamp or datetime.utcnow()
    
    def to_dict(self):
        return {
            "role": self.role,
            "content": self.content,
            "timestamp": self.timestamp.isoformat()
        }

class ChatSession:
    def __init__(self, session_id: str, title: str = "New Conversation", user_id: str = None):
        self.session_id = session_id
        self.messages = []
        self.created_at = datetime.utcnow()
        self.last_updated = datetime.utcnow()
        self.title = title
        self.user_id = user_id
    
    def to_dict(self):
        return {
            "session_id": self.session_id,
            "messages": [msg.to_dict() for msg in self.messages],
            "created_at": self.created_at.isoformat(),
            "last_updated": self.last_updated.isoformat(),
            "title": self.title,
            "user_id": self.user_id
        }

class ChatResponse:
    def __init__(self, answer: str, sources: List[str], session_id: str, title: str):
        self.answer = answer
        self.sources = sources
        self.session_id = session_id
        self.title = title
    
    def to_dict(self):
        return {
            "answer": self.answer,
            "sources": self.sources,
            "session_id": self.session_id,
            "title": self.title
        }

def start_cache_cleaning_daemon():
    """Start a daemon thread to periodically clean the cache"""
    def cleaning_worker():
        while True:
            try:
                time.sleep(CACHE_TTL_MEDIUM // 2)  # Clean cache at half TTL interval
                clean_old_cache_entries()
            except Exception as e:
                print(f"Error in cache cleaning worker: {e}")
                time.sleep(60)  # Wait a minute and try again
                
    # Start daemon thread
    thread = threading.Thread(target=cleaning_worker)
    thread.daemon = True
    thread.start()
    print("Started cache cleaning daemon")

def clean_old_cache_entries():
    """Clean expired cache entries to prevent memory bloat"""
    try:
        current_time = time.time()
        
        with cache_lock:
            # Clean response cache - use more aggressive cleaning in VM mode
            ttl_factor = 0.5 if VM_MODE else 1.0  # In VM mode, clean at half the TTL
            
            response_keys_to_remove = []
            for key, (_, timestamp, ttl) in response_cache.items():
                if current_time - timestamp > ttl * ttl_factor:
                    response_keys_to_remove.append(key)
                    
            for key in response_keys_to_remove:
                del response_cache[key]
                
            # Clean vector search cache
            vector_keys_to_remove = []
            for key, (_, timestamp) in vector_search_cache.items():
                if current_time - timestamp > VECTOR_CACHE_TTL * ttl_factor:
                    vector_keys_to_remove.append(key)
                    
            for key in vector_keys_to_remove:
                del vector_search_cache[key]
                
            # Clean model cache - always clean in VM mode
            if VM_MODE or len(model_cache) > 0 and current_time % (3600) < 60:  # Every ~1 hour
                model_cache.clear()
                
            # Force garbage collection in VM mode
            if VM_MODE and (len(response_keys_to_remove) > 5 or len(vector_keys_to_remove) > 5):
                import gc
                gc.collect()
                
        print(f"Cache cleaned: removed {len(response_keys_to_remove)} response entries and {len(vector_keys_to_remove)} vector search entries")
    except Exception as e:
        print(f"Error during cache cleaning: {e}")

def get_embeddings():
    """Thread-safe singleton pattern for embeddings"""
    global _global_embeddings
    if _global_embeddings is None:
        with _embeddings_lock:
            if _global_embeddings is None:  # Double-check under lock
                _global_embeddings = GoogleGenerativeAIEmbeddings(
                    google_api_key=GOOGLE_API_KEY,
                    model="models/text-embedding-004"
                )
    return _global_embeddings

def get_gemini_model(model_name="gemini-2.0-flash", temperature=0.3):
    """Get a Gemini model instance with caching"""
    model_key = f"{model_name}:{temperature}"
    
    with cache_lock:
        if model_key not in model_cache:
            model_cache[model_key] = ChatGoogleGenerativeAI(
                model=model_name,
                temperature=temperature
            )
        return model_cache[model_key]

def get_enhanced_cache_key(message, source_level_url=None, user_id=None):
    """Generate a more specific cache key incorporating message context"""
    # Create a more targeted key that includes key components but is normalized
    message_normalized = message.strip().lower()
    source_key = hashlib.md5(source_level_url.encode()).hexdigest()[:8] if source_level_url else "no_source"
    user_key = hashlib.md5(str(user_id).encode()).hexdigest()[:8] if user_id else "no_user"
    
    # Include full message text in key to prevent collisions
    message_key = hashlib.md5(message_normalized.encode()).hexdigest()
    # Include first few characters of the message for easier debugging
    message_prefix = message_normalized[:20].replace(' ', '_')
    
    return f"{message_prefix}:{message_key}:{source_key}:{user_key}"

async def get_or_create_session(db_conn, user_id, source_level_url, existing_session_id=None):
    """
    Retrieve or create a chat session with robust error handling and flexible retrieval

    Args:
        db_conn: Database connection
        user_id: User identifier
        source_level_url: Source URL for the conversation context
        existing_session_id: Optional existing session ID to retrieve

    Returns:
        Tuple of (session_id, session_data)
    """
    cursor = None
    try:
        cursor = db_conn.cursor()
        
        # Sanitize and default inputs
        if not source_level_url:
            source_level_url = "Unknown source"
        
        # Truncate source_level_url if too long
        source_level_url = source_level_url[:2000]
        
        # Validate user_id
        if not user_id:
            raise ValueError("User ID is required")
        
        # If an existing session_id is provided, try to retrieve it first
        if existing_session_id:
            try:
                cursor.execute("""
                    SELECT session_id, json_data, source_url, user_id
                    FROM chat_sessions
                    WHERE session_id = :session_id
                """, {'session_id': existing_session_id})
                
                result = cursor.fetchone()
                
                if result:
                    session_id, data, existing_source_url, existing_user_id = result
                    
                    # Verify user_id matches
                    if existing_user_id != user_id:
                        print(f"Warning: Session user mismatch. Requested: {user_id}, Existing: {existing_user_id}")
                    
                    # Handle different data types for json_data
                    if hasattr(data, 'read'):
                        data = data.read()
                    
                    try:
                        session_data = json.loads(data)
                        return session_id, session_data
                    except json.JSONDecodeError:
                        print(f"Error decoding JSON for session {session_id}")
            except Exception as retrieve_error:
                print(f"Error retrieving existing session: {retrieve_error}")
        
        # Try to find an existing session for the user and source_url
        try:
            cursor.execute("""
                SELECT session_id, json_data, source_url
                FROM chat_sessions
                WHERE user_id = :user_id AND source_url = :source_url
                ORDER BY created_at DESC
                FETCH FIRST 1 ROW ONLY
            """, {'user_id': user_id, 'source_url': source_level_url})
            
            result = cursor.fetchone()
            
            if result:
                # Session exists, return it
                existing_session_id, data, existing_source_url = result
                
                # Handle different data types for json_data
                if hasattr(data, 'read'):
                    data = data.read()
                
                try:
                    session_data = json.loads(data)
                    return existing_session_id, session_data
                except json.JSONDecodeError:
                    print(f"Error decoding JSON for existing session")
        except Exception as find_session_error:
            print(f"Error finding existing session: {find_session_error}")
        
        # Generate a new session_id if no existing session is found
        session_id = existing_session_id or str(int(time.time()))
        
        # Create a new session
        title = f"Conversation about {source_level_url}"
        
        new_session = {
            "session_id": session_id,
            "title": title,
            "user_id": user_id,
            "source_url": source_level_url,
            "messages": [],
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        
        # Insert the new session with separate columns
        try:
            cursor.execute("""
                INSERT INTO chat_sessions 
                (session_id, user_id, source_url, json_data, created_at, updated_at)
                VALUES (:session_id, :user_id, :source_url, :json_data, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, 
                session_id=session_id, 
                user_id=user_id, 
                source_url=source_level_url,
                json_data=json.dumps(new_session)
            )
            
            db_conn.commit()
            print(f"Created new session {session_id} for user {user_id}")
            
            return session_id, new_session
        
        except Exception as insert_error:
            # Attempt to rollback in case of error
            try:
                db_conn.rollback()
            except:
                pass
            
            print(f"Error inserting new session: {insert_error}")
            raise
        
    except Exception as e:
        # Comprehensive error handling
        print(f"Comprehensive error in get_or_create_session: {e}")
        traceback.print_exc()
        
        # Attempt to rollback in case of error
        try:
            if db_conn:
                db_conn.rollback()
        except:
            pass
        
        # Decide how to handle the error - you might want to raise or return a default session
        raise
    
    finally:
        # Ensure cursor is closed
        if cursor:
            try:
                cursor.close()
            except Exception as close_error:
                print(f"Error closing cursor: {close_error}")

async def save_message(db_conn, session_id, user_message, assistant_response, user_id, source_level_url):
    """
    Save messages to a session with enhanced error handling and transaction management
    
    Args:
        db_conn: Oracle database connection
        session_id: Session ID to update
        user_message: User's message
        assistant_response: Assistant's response
        user_id: User ID from JWT token
        source_level_url: Source URL context
        
    Returns:
        bool: Success status
    """
    cursor = None
    try:
        print(f"Saving messages for session ID: {session_id}, user ID: {user_id}")
        
        # Ensure source_level_url is not None
        if not source_level_url:
            source_level_url = "Unknown source"
            
        # First get the current session data
        cursor = db_conn.cursor()
        
        # Query by session_id first
        query = """
            SELECT ID, JSON_DATA 
            FROM CHAT_SESSIONS 
            WHERE JSON_VALUE(JSON_DATA, '$.session_id') = :session_id
        """
        cursor.execute(query, session_id=session_id)
        result = cursor.fetchone()
        
        if not result:
            # If session not found by session_id, try a more direct approach
            print(f"Session {session_id} not found by JSON_VALUE, trying direct query...")
            fallback_query = """
                SELECT ID, JSON_DATA
                FROM CHAT_SESSIONS
                WHERE JSON_DATA LIKE '%"session_id":"' || :session_id || '"%'
                   OR JSON_DATA LIKE '%"session_id": "' || :session_id || '"%'
            """
            cursor.execute(fallback_query, session_id=session_id)
            result = cursor.fetchone()
            
        if not result:
            # Session not found - create a new one with the messages
            print(f"Session {session_id} not found, creating new session")
            new_session = {
                "session_id": session_id,
                "title": f"Conversation about {source_level_url}",
                "user_id": user_id,
                "source_url": source_level_url,
                "messages": [
                    {
                        "role": "user",
                        "content": user_message,
                        "timestamp": datetime.utcnow().isoformat()
                    },
                    {
                        "role": "assistant",
                        "content": assistant_response,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                ],
                "created_at": datetime.utcnow().isoformat(),
                "last_updated": datetime.utcnow().isoformat()
            }
            
            insert_query = """
                INSERT INTO CHAT_SESSIONS 
                (SESSION_ID, USER_ID, SOURCE_URL, JSON_DATA, CREATED_AT, UPDATED_AT)
                VALUES (:session_id, :user_id, :source_url, :json_data, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            
            cursor.execute(
                insert_query, 
                session_id=session_id,
                user_id=user_id,
                source_url=source_level_url,
                json_data=json.dumps(new_session)
            )
            
            db_conn.commit()
            print(f"Created new session {session_id} with initial messages")
            return True
        
        # Session exists - update it
        row_id, data = result
        
        # Handle different data types for json_data
        if hasattr(data, 'read'):
            data = data.read()
        
        # Parse the existing data    
        if isinstance(data, str):
            session_data = json.loads(data)
        elif isinstance(data, bytes):
            session_data = json.loads(data.decode('utf-8'))
        else:
            session_data = data
        
        # Make a copy of the original data for debugging
        original_data = json.dumps(session_data)
        
        # Ensure messages array exists
        if 'messages' not in session_data:
            session_data['messages'] = []
        
        # Create message objects
        user_message_obj = {
            "role": "user",
            "content": user_message,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        assistant_message_obj = {
            "role": "assistant",
            "content": assistant_response,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Append new messages to the existing array
        session_data['messages'].append(user_message_obj)
        session_data['messages'].append(assistant_message_obj)
        
        # Update the last_updated timestamp
        session_data['last_updated'] = datetime.utcnow().isoformat()
        
        # Ensure source_url is set
        if not session_data.get('source_url'):
            session_data['source_url'] = source_level_url
            
        # Ensure user_id is set
        if not session_data.get('user_id') and user_id:
            session_data['user_id'] = user_id
        
        # Prepare updated JSON    
        updated_json = json.dumps(session_data)
        
        print(f"Updating session {session_id} (Row ID: {row_id})")
        print(f"Original message count: {len(json.loads(original_data).get('messages', []))}")
        print(f"Updated message count: {len(session_data['messages'])}")
        
        # Update the session in the database
        update_query = """
            UPDATE CHAT_SESSIONS 
            SET JSON_DATA = :json_data,
                UPDATED_AT = CURRENT_TIMESTAMP
            WHERE ID = :id
        """
        
        cursor.execute(update_query, json_data=updated_json, id=row_id)
        rows_updated = cursor.rowcount
        
        if rows_updated == 0:
            print(f"Warning: No rows updated for session {session_id}")
            # Try an alternative approach with the session_id
            alt_update_query = """
                UPDATE CHAT_SESSIONS 
                SET JSON_DATA = :json_data,
                    UPDATED_AT = CURRENT_TIMESTAMP
                WHERE JSON_DATA LIKE '%"session_id":"' || :session_id || '"%'
                   OR JSON_DATA LIKE '%"session_id": "' || :session_id || '"%'
            """
            
            cursor.execute(alt_update_query, json_data=updated_json, session_id=session_id)
            rows_updated = cursor.rowcount
            
            if rows_updated == 0:
                print(f"Error: Could not update session {session_id} with either method")
                return False
        
        # Commit the transaction
        db_conn.commit()
        
        # Verify the update worked by reading back the data
        verify_query = "SELECT JSON_DATA FROM CHAT_SESSIONS WHERE ID = :id"
        cursor.execute(verify_query, id=row_id)
        verify_result = cursor.fetchone()
        
        if verify_result:
            verify_data = verify_result[0]
            if hasattr(verify_data, 'read'):
                verify_data = verify_data.read()
                
            if isinstance(verify_data, str):
                verify_json = json.loads(verify_data)
            elif isinstance(verify_data, bytes):
                verify_json = json.loads(verify_data.decode('utf-8'))
            else:
                verify_json = verify_data
                
            verify_messages = verify_json.get('messages', [])
            print(f"Verification: Session now has {len(verify_messages)} messages")
            
            if len(verify_messages) != len(session_data['messages']):
                print(f"Warning: Message count mismatch after update")
                
        print(f"Successfully updated session {session_id} with new messages")
        return True
        
    except Exception as e:
        print(f"Error saving messages: {e}")
        traceback.print_exc()
        
        try:
            if db_conn:
                db_conn.rollback()
        except Exception as rollback_error:
            print(f"Error during rollback: {rollback_error}")
            
        return False
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as close_error:
                print(f"Error closing cursor: {close_error}")
            
# Modified cache_response function
def cache_response(key, response, ttl=CACHE_TTL_MEDIUM):
    """Cache a response with thread safety and size limits"""
    with cache_lock:
        # Check if this is a greeting response, don't cache unless the key shows it's a greeting query
        is_greeting_response = False
        if hasattr(response, 'answer'):
            is_greeting_response = "hello! i'm your ai assistant" in response.answer.lower()
            
        query_prefix = key.split(':', 1)[0].lower() if ':' in key else ""
        is_greeting_query = any(pattern in query_prefix for pattern in ["hello", "hi", "hey", "thanks", "thank", "goodbye", "bye"])
        
        # Don't cache greeting responses for non-greeting queries
        if is_greeting_response and not is_greeting_query and len(query_prefix) > 5:
            print(f"Not caching greeting response for non-greeting query: {query_prefix}")
            return
            
        # Implement LRU-like eviction if cache is too large
        if len(response_cache) >= MAX_CACHE_ENTRIES:
            # Remove oldest 10% of entries
            entries = sorted(response_cache.items(), key=lambda x: x[1][1])
            for old_key, _ in entries[:MAX_CACHE_ENTRIES // 10]:
                del response_cache[old_key]
        
        # Store with current timestamp
        response_cache[key] = (response, time.time(), ttl)
        print(f"Cached response with key: {key}, TTL: {ttl}s")

# Updated get_cached_response function 
def get_cached_response(key):
    """Retrieve cached response if it exists and is fresh"""
    with cache_lock:
        if key in response_cache:
            response, timestamp, ttl = response_cache[key]
            if (time.time() - timestamp) < ttl:
                # Validate this is not a greeting response for a non-greeting query
                query_prefix = key.split(':', 1)[0].lower() if ':' in key else ""
                is_greeting_query = any(pattern in query_prefix for pattern in ["hello", "hi", "hey", "thanks", "thank", "goodbye", "bye"])
                
                if hasattr(response, 'answer'):
                    is_greeting_response = "hello! i'm your ai assistant" in response.answer.lower()
                    
                    # If mismatch between query type and response type, invalidate cache
                    if is_greeting_response and not is_greeting_query and len(query_prefix) > 5:
                        print(f"Cache validation failed - greeting response for non-greeting query: {query_prefix}")
                        del response_cache[key]
                        return None
                
                return response
            else:
                # Clean up expired entry
                del response_cache[key]
    return None

async def parallel_vector_search(vector_store, query, k=10, source_level_url=None):
    """
    Execute enhanced vector search in a separate thread to avoid blocking
    with improved performance and quality
    
    Args:
        vector_store: The vector store to search 
        query: The user's query
        k: Number of documents to retrieve (increased from 8 to 10)
        source_level_url: Optional URL to filter results
        
    Returns:
        List of relevant Document objects
    """
    # Reduce k value in VM mode to improve performance
    if VM_MODE:
        k = 5  # Reduced from 10
    
    # Check cache first
    cache_key = f"vecsr:{hashlib.md5((query + str(source_level_url)).encode()).hexdigest()}"
    
    # Check cache with minimized lock contention
    cached_result = None
    with cache_lock:
        if cache_key in vector_search_cache:
            docs, timestamp = vector_search_cache[cache_key]
            if time.time() - timestamp < VECTOR_CACHE_TTL:
                cached_result = docs
    
    if cached_result:
        print(f"🔍 Vector search cache hit for: {query[:30]}...")
        return cached_result
    
    # Extract domain for filtering if source_level_url provided
    domain = None
    if source_level_url:
        try:
            url_parts = source_level_url.split('/')
            if len(url_parts) > 2:
                domain = url_parts[2]  # e.g., extract 'example.com' from 'https://example.com/path'
        except:
            pass
    
    # Use thread executor for non-blocking search
    print(f"Performing enhanced vector search for: {query[:50]}...")
    with concurrent.futures.ThreadPoolExecutor() as executor:
        try:
            # IMPROVED: Add context to the search query using hybrid approach
            search_query = query
            if domain:
                # Add domain context but also preserve original query importance
                search_query = f"{query} context:{domain}"
            
            # IMPROVED: Do an initial broader search first, then filter
            # In VM mode, use a smaller multiplier to reduce search size
            future = executor.submit(
                vector_store.similarity_search_with_relevance_scores,
                search_query,
                k=min(k * (2 if VM_MODE else 3), 20 if VM_MODE else 30)  # Adjusted for VM mode
            )
            
            # Set a shorter timeout in VM mode
            timeout_seconds = 5 if VM_MODE else 10
            
            # Set a shorter timeout to prevent VM hanging
            doc_tuples = await asyncio.get_event_loop().run_in_executor(
                None, lambda: future.result(timeout=timeout_seconds)
            )
            
            # Post-process the results with improved filtering
            filtered_docs = []
            
            # IMPROVED: Use multiple filter tiers to improve result quality
            high_relevance_docs = []
            medium_relevance_docs = []
            domain_match_docs = []
            
            # Adjust relevance thresholds in VM mode to be more aggressive in filtering
            high_relevance_threshold = 0.75 if VM_MODE else 0.7
            medium_relevance_threshold = 0.6 if VM_MODE else 0.5
            
            for doc_tuple in doc_tuples:
                doc, score = doc_tuple
                
                # Check if document has metadata that includes the domain or source_level_url
                domain_match = False
                if domain and doc.metadata:
                    meta_str = str(doc.metadata).lower()
                    if domain.lower() in meta_str:
                        domain_match = True
                        domain_match_docs.append(doc)
                
                # Bucket documents by relevance
                if score > high_relevance_threshold:  # Highly relevant
                    high_relevance_docs.append(doc)
                elif score > medium_relevance_threshold:  # Moderately relevant
                    medium_relevance_docs.append(doc)
                elif domain_match and score > 0.3:
                    # Lower score but domain matches, so might be relevant
                    if doc not in domain_match_docs:
                        domain_match_docs.append(doc)
            
            # Prioritize results: high relevance first, then domain matches, then medium relevance
            filtered_docs = high_relevance_docs
            
            # Add domain matches that aren't already included
            for doc in domain_match_docs:
                if doc not in filtered_docs:
                    filtered_docs.append(doc)
            
            # Fill remaining spots with medium relevance docs
            for doc in medium_relevance_docs:
                if doc not in filtered_docs:
                    filtered_docs.append(doc)
            
            # If no filtered docs, fall back to all results above a minimum threshold
            if not filtered_docs:
                filtered_docs = [doc for doc, score in doc_tuples if score > (0.4 if VM_MODE else 0.3)]
            
            # If still no results, use all available results
            if not filtered_docs and doc_tuples:
                filtered_docs = [doc for doc, _ in doc_tuples]
            
            # Limit to k documents
            result_docs = filtered_docs[:k]
            
            # Cache result before returning
            with cache_lock:
                vector_search_cache[cache_key] = (result_docs, time.time())
            
            return result_docs
            
        except concurrent.futures.TimeoutError:
            print("Vector search timed out")
            return []  # Return empty results on timeout
        except Exception as search_error:
            print(f"Error in vector search: {search_error}")
            return []  # Return empty results on error
def force_gc():
    """Force garbage collection to free up memory"""
    if VM_MODE:
        try:
            import gc
            memory_before = 0
            try:
                import psutil
                process = psutil.Process()
                memory_before = process.memory_info().rss / 1024 / 1024
            except:
                pass
                
            # Run garbage collection
            collected = gc.collect(generation=2)
            
            # Get memory after
            memory_after = 0
            try:
                import psutil
                process = psutil.Process()
                memory_after = process.memory_info().rss / 1024 / 1024
            except:
                pass
                
            if memory_before and memory_after:
                print(f"GC collected {collected} objects. Memory: {memory_before:.1f}MB → {memory_after:.1f}MB ({memory_before - memory_after:.1f}MB freed)")
            else:
                print(f"GC collected {collected} objects")
        except Exception as e:
            print(f"Error during forced GC: {e}")        

def direct_metadata_search(vectdb_connection, source_level_url, k=10):
    """
    Perform direct SQL metadata search for faster responses without vector search
    
    Args:
        vectdb_connection: Connection to the vector database
        source_level_url: URL to search for in metadata
        k: Maximum number of results to return
        
    Returns:
        List of Document objects
    """
    cursor = None
    try:
        cursor = vectdb_connection.cursor()
        
        # First identify the table columns
        try:
            cursor.execute(f"SELECT * FROM {VECTDB_TABLE_NAME} WHERE ROWNUM <= 1")
            columns = [desc[0].upper() for desc in cursor.description]
            
            # Find content and metadata columns
            content_col = next((col for col in columns if col in ["CONTENT", "TEXT", "PAGE_CONTENT", "DOCUMENT_CONTENT"]), columns[1])
            metadata_col = next((col for col in columns if col in ["METADATA", "META", "DOCUMENT_METADATA"]), columns[2])
            
            # Construct and execute the query
            query = f"""
                SELECT {content_col}, {metadata_col} FROM {VECTDB_TABLE_NAME}
                WHERE {metadata_col} LIKE :pattern
                FETCH FIRST {k} ROWS ONLY
            """
            cursor.execute(query, pattern=f'%{source_level_url}%')
            results = cursor.fetchall()
            
            # Process the results
            docs = []
            for content, metadata in results:
                # Handle LOB objects
                if hasattr(content, 'read'):
                    content = content.read()
                
                if hasattr(metadata, 'read'):
                    metadata_str = metadata.read()
                    try:
                        metadata_dict = json.loads(metadata_str)
                    except:
                        metadata_dict = {"source": source_level_url}
                else:
                    try:
                        if isinstance(metadata, str):
                            metadata_dict = json.loads(metadata)
                        else:
                            metadata_dict = {"source": source_level_url}
                    except:
                        metadata_dict = {"source": source_level_url}
                
                docs.append(Document(page_content=content, metadata=metadata_dict))
            
            return docs
            
        except Exception as e:
            print(f"Error in direct metadata search: {e}")
            return []
            
    except Exception as e:
        print(f"Database error in direct metadata search: {e}")
        return []
    finally:
        if cursor:
            cursor.close()

# Add this function after the direct_metadata_search function
def truncate_documents(docs, max_chars=15000):
    """
    Truncate document content to limit total size
    
    Args:
        docs: List of Document objects
        max_chars: Maximum total characters across all documents
        
    Returns:
        List of truncated Document objects
    """
    truncated_docs = []
    total_chars = 0
    
    for doc in docs:
        # Calculate how many characters we can still add
        remaining = max(0, max_chars - total_chars)
        
        if remaining <= 100:  # Not enough space for meaningful content
            break
            
        # If content is too long, truncate it
        content = doc.page_content
        if len(content) > remaining:
            content = content[:remaining] + "..."
        
        # Create new document with truncated content
        truncated_docs.append(Document(
            page_content=content,
            metadata=doc.metadata
        ))
        
        total_chars += len(content)
    
    return truncated_docs

def clear_all_caches():
    """Clear all application caches"""
    with cache_lock:
        response_cache.clear()
        vector_search_cache.clear()
        embedding_cache.clear()
        model_cache.clear()  # Also clear model cache
        print("All caches cleared")
    
    # Force garbage collection after clearing caches
    if VM_MODE:
        try:
            import gc
            gc.collect()
            print("Garbage collection performed after cache clearing")
        except:
            pass
def initialize_json_database(jsondb_connection):
    try:
        with jsondb_connection.cursor() as cursor:
            # Check if table exists in the current user's schema
            cursor.execute("""
                SELECT COUNT(*) 
                FROM USER_TABLES 
                WHERE TABLE_NAME = 'CHAT_SESSIONS'
            """)
            count = cursor.fetchone()[0]
            
            if count == 0:
                print(f"CHAT_SESSIONS table doesn't exist. Creating it now...")
                # Create the table with all necessary columns
                cursor.execute("""
                    CREATE TABLE CHAT_SESSIONS (
                        id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        session_id VARCHAR2(50),
                        user_id VARCHAR2(50),
                        source_url VARCHAR2(2000),
                        json_data CLOB CHECK (json_data IS JSON),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                print(f"CHAT_SESSIONS table created successfully!")
            else:
                print(f"CHAT_SESSIONS table already exists. Verifying columns...")
                
                # List of columns to check and add if missing
                columns_to_check = [
                    ('session_id', 'VARCHAR2(50)'),
                    ('user_id', 'VARCHAR2(50)'),
                    ('source_url', 'VARCHAR2(2000)'),
                    ('json_data', 'CLOB CHECK (json_data IS JSON)'),
                    ('created_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'),
                    ('updated_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
                ]
                
                for column_name, column_definition in columns_to_check:
                    # Check if column exists
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM USER_TAB_COLUMNS 
                        WHERE TABLE_NAME = 'CHAT_SESSIONS' AND COLUMN_NAME = :column_name
                    """, column_name=column_name.upper())
                    
                    column_exists = cursor.fetchone()[0] > 0
                    
                    if not column_exists:
                        try:
                            # Add the missing column
                            cursor.execute(f"""
                                ALTER TABLE CHAT_SESSIONS 
                                ADD {column_name} {column_definition}
                            """)
                            print(f"Added {column_name} column to CHAT_SESSIONS table")
                        except Exception as add_column_error:
                            print(f"Error adding {column_name} column: {add_column_error}")
                
                # Create indexes if they don't exist
                index_definitions = [
                    ('idx_chat_sessions_session_id', 'CREATE INDEX idx_chat_sessions_session_id ON CHAT_SESSIONS(session_id)'),
                    ('idx_chat_sessions_user_id', 'CREATE INDEX idx_chat_sessions_user_id ON CHAT_SESSIONS(user_id)'),
                    ('idx_chat_sessions_source_url', 'CREATE INDEX idx_chat_sessions_source_url ON CHAT_SESSIONS(source_url)'),
                ]
                
                for index_name, index_sql in index_definitions:
                    # Check if index exists
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM USER_INDEXES 
                        WHERE TABLE_NAME = 'CHAT_SESSIONS' AND INDEX_NAME = :index_name
                    """, index_name=index_name.upper())
                    
                    index_exists = cursor.fetchone()[0] > 0
                    
                    if not index_exists:
                        try:
                            cursor.execute(index_sql)
                            print(f"Created index {index_name}")
                        except Exception as index_error:
                            print(f"Error creating index {index_name}: {index_error}")
                
                # Ensure unique constraint on session_id if needed
                try:
                    cursor.execute("""
                        ALTER TABLE CHAT_SESSIONS 
                        ADD CONSTRAINT unq_chat_sessions_session_id UNIQUE (session_id)
                    """)
                    print("Added unique constraint on session_id")
                except Exception as constraint_error:
                    # This will raise an error if the constraint already exists
                    # We can safely ignore this specific error
                    if "ORA-00001" not in str(constraint_error):
                        print(f"Error adding unique constraint: {constraint_error}")
            
            # Populate existing rows with a session_id if not already present
            try:
                cursor.execute("""
                    UPDATE CHAT_SESSIONS 
                    SET session_id = TO_CHAR(created_at, 'YYYYMMDDHH24MISS') || '_' || id
                    WHERE session_id IS NULL
                """)
                print("Updated existing rows with session_id")
            except Exception as update_error:
                print(f"Error updating existing rows: {update_error}")
            
            # Commit all changes
            jsondb_connection.commit()
            
            print("CHAT_SESSIONS table verification and migration complete.")
            return True
    except Exception as e:
        print(f"Error initializing JSON database: {e}")
        traceback.print_exc()
        
        # Rollback in case of any errors
        try:
            jsondb_connection.rollback()
        except:
            pass
        
        return False

def process_scrapped_text_to_vector_store(jsondb_connection, user_id=None, source_level_url=None):
    """
    Process scrapped text data from the SCRAPPED_TEXT table to the vector store,
    vectorizing ALL documents associated with the given parent source_level_url
    
    Args:
        jsondb_connection: Oracle connection to the JSON database
        user_id: Optional filter by user_id
        source_level_url: Parent URL to filter all child documents by
    
    Returns:
        Dict with status and message
    """
    try:
        # Initialize vector database connection
        with get_vectdb_connection() as vectdb_connection:
            # Initialize embeddings model using singleton pattern
            embeddings = get_embeddings()
            
            # Initialize vector store
            vector_store = oraclevs.OracleVS(
                client=vectdb_connection,
                embedding_function=embeddings,
                table_name=VECTDB_TABLE_NAME,
                distance_strategy=DistanceStrategy.COSINE,
            )
            
            cursor = jsondb_connection.cursor()
            
            # Build query based on what we know about the schema from the screenshot
            query = """
                SELECT SCRAPPED_CONTENT, CONTENT_LINK, TOP_LEVEL_SOURCE, TITLE
                FROM SCRAPPED_TEXT
                WHERE 1=1
            """
            
            params = {}
            
            # If source_level_url is provided, filter by TOP_LEVEL_SOURCE
            if source_level_url:
                query += " AND TOP_LEVEL_SOURCE = :source_level_url"
                params["source_level_url"] = source_level_url
            
            if user_id:
                query += " AND USER_ID = :user_id"
                params["user_id"] = user_id
                
            # In VM mode, limit the query
            if VM_MODE:
                query += " AND ROWNUM <= 50"  # Limit to 50 documents in VM mode
                
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            if not rows:
                return {
                    "status": "info", 
                    "message": f"No scrapped text data found for source_level_url: {source_level_url}"
                }
            
            # Create documents from scrapped text
            documents = []
            for row in rows:
                content_lob, link, top_level_source, title = row
                
                # Convert Oracle LOB object to string
                if hasattr(content_lob, 'read'):
                    content = content_lob.read()
                else:
                    content = str(content_lob)
                
                # Create metadata as a JSON object (matching your vector store schema)
                metadata = {
                    "url": link,
                    "source": source_level_url if source_level_url else top_level_source,
                    "title": title
                }
                
                documents.append(Document(page_content=content, metadata=metadata))
            
            # Split documents into chunks
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=CHUNK_SIZE,
                chunk_overlap=CHUNK_OVERLAP,
                length_function=len,
                is_separator_regex=False,
            )
            
            chunks = text_splitter.split_documents(documents)
            
            # Process in smaller batches
            BATCH_SIZE = 2 if VM_MODE else 5  # Smaller batch size in VM mode
            total_batches = (len(chunks) + BATCH_SIZE - 1) // BATCH_SIZE
            
            successful_chunks = 0
            failed_chunks = 0
            
            for i in range(0, len(chunks), BATCH_SIZE):
                batch = chunks[i:i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                
                try:
                    vector_store.add_documents(batch)
                    print(f"Batch {batch_num}/{total_batches} processed successfully")
                    successful_chunks += len(batch)
                except Exception as e:
                    print(f"Error processing batch {batch_num}: {e}")
                    traceback.print_exc()
                    failed_chunks += len(batch)
                
                # Less aggressive sleep to avoid rate limits but improve performance
                time.sleep(2 if VM_MODE else 1)
            
            # Clean up
            cursor.close()
            
            # Force garbage collection in VM mode
            if VM_MODE:
                import gc
                gc.collect()
            
            return {
                "status": "success", 
                "message": f"Processed {successful_chunks} chunks from {len(documents)} documents for source_level_url: {source_level_url}",
                "document_count": len(documents),
                "successful_chunks": successful_chunks,
                "failed_chunks": failed_chunks,
                "source_level_url": source_level_url
            }
    
    except Exception as e:
        print(f"Error processing scrapped text: {e}")
        traceback.print_exc()
        return {"status": "error", "message": str(e), "source_level_url": source_level_url}
# # Session Management Functions
# async def create_new_session(db_conn, title="New Conversation", user_id=None):
#     cursor = None
#     try:
#         cursor = db_conn.cursor()
#         session_id = str(int(time.time()))
#         session = ChatSession(session_id=session_id, title=title, user_id=user_id)
        
#         insert_query = """
#         INSERT INTO CHAT_SESSIONS (JSON_DATA)
#         VALUES (:1)
#         """
#         cursor.execute(insert_query, (json.dumps(session.to_dict()),))
#         db_conn.commit()
#         print(f"Session {session_id} created successfully.")
#         return session_id, session.title
#     except Exception as e:
#         print("Error creating session:", e)
#         traceback.print_exc()
#         if db_conn:
#             db_conn.rollback()
#         return None, None
#     finally:
#         if cursor:
#             cursor.close()

async def get_session(db_conn, session_id):
    """
    Get a specific chat session by its ID with improved error handling
    
    Args:
        db_conn: Oracle database connection
        session_id: The ID of the session to retrieve
        
    Returns:
        dict: The session data or None if not found
    """
    cursor = None
    try:
        cursor = db_conn.cursor()
        print(f"Retrieving session with ID: {session_id}")
        
        # Try the standard query first
        try:
            select_query = """
            SELECT JSON_DATA
            FROM CHAT_SESSIONS
            WHERE JSON_VALUE(JSON_DATA, '$.session_id') = :session_id
            """
            cursor.execute(select_query, session_id=session_id)  # Use named parameter
            result = cursor.fetchone()
            
            if result:
                print(f"Found session {session_id} using JSON_VALUE")
                data = result[0]
                
                # Handle different data types
                if hasattr(data, 'read'):
                    data = data.read()
                    
                if isinstance(data, str):
                    return json.loads(data)
                elif isinstance(data, bytes):
                    return json.loads(data.decode('utf-8'))
                else:
                    return data
            else:
                print(f"Session {session_id} not found with JSON_VALUE query")
                
                # Try a more direct approach with LIKE query as fallback
                print("Trying fallback query with LIKE...")
                fallback_query = """
                SELECT JSON_DATA
                FROM CHAT_SESSIONS
                WHERE JSON_DATA LIKE '%"session_id":"' || :session_id || '"%'
                   OR JSON_DATA LIKE '%"session_id": "' || :session_id || '"%'
                """
                cursor.execute(fallback_query, session_id=session_id)  # Use named parameter
                fallback_result = cursor.fetchone()
                
                if fallback_result:
                    print(f"Found session {session_id} using LIKE query")
                    data = fallback_result[0]
                    
                    # Handle different data types
                    if hasattr(data, 'read'):
                        data = data.read()
                        
                    if isinstance(data, str):
                        return json.loads(data)
                    elif isinstance(data, bytes):
                        return json.loads(data.decode('utf-8'))
                    else:
                        return data
                    
                # Last resort: scan all rows
                print("Trying last resort - scanning all sessions...")
                cursor.execute("SELECT JSON_DATA FROM CHAT_SESSIONS")
                all_rows = cursor.fetchall()
                print(f"Scanning {len(all_rows)} rows to find session {session_id}")
                
                for row in all_rows:
                    try:
                        data = row[0]
                        if hasattr(data, 'read'):
                            data = data.read()
                            
                        if isinstance(data, str):
                            parsed = json.loads(data)
                        elif isinstance(data, bytes):
                            parsed = json.loads(data.decode('utf-8'))
                        else:
                            parsed = data
                            
                        if parsed.get('session_id') == session_id:
                            print(f"Found session {session_id} by scanning all rows")
                            return parsed
                    except Exception as row_error:
                        print(f"Error processing row during scan: {str(row_error)}")
                        continue
                
                print(f"Session {session_id} not found after trying all methods")
                return None
                
        except Exception as query_error:
            print(f"Error in main query: {str(query_error)}")
            traceback.print_exc()
            
            # Try a simplified query if JSON_VALUE fails
            try:
                cursor.execute("SELECT JSON_DATA FROM CHAT_SESSIONS")
                all_rows = cursor.fetchall()
                
                for row in all_rows:
                    try:
                        data = row[0]
                        if hasattr(data, 'read'):
                            data = data.read()
                            
                        if isinstance(data, str):
                            parsed = json.loads(data)
                        elif isinstance(data, bytes):
                            parsed = json.loads(data.decode('utf-8'))
                        else:
                            parsed = data
                            
                        if parsed.get('session_id') == session_id:
                            return parsed
                    except:
                        continue
                        
                return None
            except Exception as fallback_error:
                print(f"Fallback query also failed: {str(fallback_error)}")
                return None
    except Exception as e:
        print(f"Unexpected error in get_session: {str(e)}")
        traceback.print_exc()
        return None
    finally:
        if cursor:
            cursor.close()

# async def update_session(db_conn, session_id, user_message, assistant_response, user_id=None):
#     """
#     Update a chat session with new messages with improved error handling and verification
    
#     Args:
#         db_conn: Oracle database connection
#         session_id: The ID of the session to update
#         user_message: The user's message to add
#         assistant_response: The assistant's response to add
#         user_id: Optional user ID to associate with the session
        
#     Returns:
#         bool: True if successful, False otherwise
#     """
#     cursor = None
#     try:
#         print(f"Updating session {session_id} with new messages, user_id: {user_id}")
        
#         # Get the current session
#         session = await get_session(db_conn, session_id)
#         if not session:
#             print(f"Session {session_id} not found, creating a new session")
#             # Create a new session since the specified one doesn't exist
#             new_session_id = str(int(time.time()))
#             new_session = ChatSession(session_id=new_session_id, title="New Conversation", user_id=user_id)
#             session = new_session.to_dict()
#             session_id = new_session_id
            
#             # Insert the new session
#             cursor = db_conn.cursor()
#             insert_query = """
#             INSERT INTO CHAT_SESSIONS (JSON_DATA)
#             VALUES (:1)
#             """
#             cursor.execute(insert_query, (json.dumps(session),))
#             db_conn.commit()
#             print(f"Created new session {session_id} as fallback with user_id: {user_id}")
#         elif user_id and not session.get("user_id"):
#             # Update the user_id if it wasn't set before
#             session["user_id"] = user_id
        
#         # Create message objects
#         message_user = {
#             "role": "user",
#             "content": user_message,
#             "timestamp": datetime.utcnow().isoformat()
#         }

#         message_assistant = {
#             "role": "assistant",
#             "content": assistant_response,
#             "timestamp": datetime.utcnow().isoformat()
#         }
        
#         # Append new messages to existing messages
#         current_messages = session.get("messages", [])
#         updated_messages = current_messages + [message_user, message_assistant]
        
#         # Update timestamp
#         last_updated = datetime.utcnow().isoformat()
        
#         # Try first with JSON_MERGEPATCH
#         cursor = db_conn.cursor()
#         try:
#             update_query = """
#             UPDATE CHAT_SESSIONS
#             SET JSON_DATA = JSON_MERGEPATCH(JSON_DATA, :1)
#             WHERE JSON_VALUE(JSON_DATA, '$.session_id') = :2
#             """
            
#             # Include user_id in the patch if it was provided
#             patch_data = {
#                 "messages": updated_messages,
#                 "last_updated": last_updated
#             }
            
#             if user_id and not session.get("user_id"):
#                 patch_data["user_id"] = user_id
                
#             patch_json = json.dumps(patch_data)
            
#             cursor.execute(update_query, [patch_json, session_id])
#             rows_updated = cursor.rowcount
            
#             if rows_updated > 0:
#                 db_conn.commit()
#                 print(f"Updated session {session_id} using JSON_MERGEPATCH, {rows_updated} rows affected, user_id: {session.get('user_id') or user_id}")
                
#                 # Verify the update
#                 updated_session = await get_session(db_conn, session_id)
#                 if updated_session and len(updated_session.get('messages', [])) == len(updated_messages):
#                     print(f"Verified update success: session now has {len(updated_messages)} messages")
#                     return True
#                 else:
#                     print("WARNING: Session update verification failed")
#             else:
#                 print(f"No rows updated with JSON_MERGEPATCH, trying alternative approach")
                
#                 # Try alternative approach by replacing the entire document
#                 try:
#                     # Update the session object and replace it entirely
#                     session['messages'] = updated_messages
#                     session['last_updated'] = last_updated
                    
#                     # Add user_id if provided and not already set
#                     if user_id and not session.get("user_id"):
#                         session["user_id"] = user_id
                    
#                     # Find the row by ID using a LIKE clause as fallback
#                     select_id_query = """
#                     SELECT ID FROM CHAT_SESSIONS
#                     WHERE JSON_DATA LIKE '%"session_id":"' || :1 || '"%'
#                        OR JSON_DATA LIKE '%"session_id": "' || :1 || '"%'
#                     """
#                     cursor.execute(select_id_query, [session_id])
#                     id_result = cursor.fetchone()
                    
#                     if id_result:
#                         row_id = id_result[0]
                        
#                         # Update by ID (more reliable)
#                         update_by_id_query = """
#                         UPDATE CHAT_SESSIONS
#                         SET JSON_DATA = :1
#                         WHERE ID = :2
#                         """
#                         cursor.execute(update_by_id_query, [json.dumps(session), row_id])
#                         rows_updated = cursor.rowcount
                        
#                         if rows_updated > 0:
#                             db_conn.commit()
#                             print(f"Updated session {session_id} by replacing document, {rows_updated} rows affected")
#                             return True
#                         else:
#                             print(f"Failed to update session {session_id} by ID")
#                     else:
#                         print(f"Couldn't find row ID for session {session_id}, attempting insert as new session")
                        
#                         # Last resort: Insert as new session with same session_id
#                         insert_query = """
#                         INSERT INTO CHAT_SESSIONS (JSON_DATA)
#                         VALUES (:1)
#                         """
#                         cursor.execute(insert_query, [json.dumps(session)])
#                         db_conn.commit()
#                         print(f"Inserted session {session_id} as new document")
#                         return True
#                 except Exception as alt_error:
#                     print(f"Alternative update approach failed: {str(alt_error)}")
#                     traceback.print_exc()
                    
#                     if db_conn:
#                         db_conn.rollback()
#                     return False
#         except Exception as update_error:
#             print(f"Error updating session: {str(update_error)}")
#             traceback.print_exc()
            
#             if db_conn:
#                 db_conn.rollback()
#             return False
#     except Exception as e:
#         print(f"Unexpected error in update_session: {str(e)}")
#         traceback.print_exc()
        
#         if db_conn:
#             db_conn.rollback()
#         return False
#     finally:
#         if cursor:
#             cursor.close()

async def update_session_title(db_conn, session_id, new_title):
    """
    Update the title of a chat session with improved error handling
    
    Args:
        db_conn: Oracle database connection
        session_id: Session ID to update
        new_title: New title for the session
        
    Returns:
        bool: Success status
    """
    cursor = None
    try:
        cursor = db_conn.cursor()
        
        # First get the current session to ensure it exists
        query = "SELECT ID, JSON_DATA FROM CHAT_SESSIONS WHERE JSON_VALUE(JSON_DATA, '$.session_id') = :1"
        cursor.execute(query, [session_id])
        result = cursor.fetchone()
        
        if not result:
            print(f"Session {session_id} not found for title update")
            return False
            
        row_id, data = result
        if hasattr(data, 'read'):
            data = data.read()
            
        session_data = json.loads(data)
        
        # Update the title
        session_data["title"] = new_title
        session_data["last_updated"] = datetime.utcnow().isoformat()
        
        # Check if source_url is missing and can be extracted from title
        if not session_data.get("source_url") and new_title.startswith("Conversation about "):
            source_url = new_title[19:]
            if "http" in source_url:
                session_data["source_url"] = source_url
                print(f"Extracted source_url from title: {source_url}")
        
        # Simple update by row ID
        update_query = "UPDATE CHAT_SESSIONS SET JSON_DATA = :1 WHERE ID = :2"
        cursor.execute(update_query, [json.dumps(session_data), row_id])
        db_conn.commit()
        
        return True
        
    except Exception as e:
        print(f"Error updating session title: {e}")
        traceback.print_exc()
        
        if db_conn:
            db_conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()

async def get_all_conversations(db_conn, user_id=None, source_level_url=None):
    """
    Get all chat conversations with strict user ID and source URL filtering
    
    Args:
        db_conn: Oracle database connection
        user_id: User ID to filter by (required)
        source_level_url: Source URL to filter by (exact match)
        
    Returns:
        list: List of conversation summary objects
    """
    cursor = None
    try:
        cursor = db_conn.cursor()
        
        # User ID is required - return empty list if not provided
        if not user_id:
            print("Missing required user_id parameter")
            return []
        
        # Source URL is required for this implementation
        if not source_level_url:
            print("Missing required source_level_url parameter")
            return []
            
        print(f"Searching for conversations with user_id: {user_id}, source_url: {source_level_url}")
        
        # Most comprehensive search query
        base_query = """
            SELECT ID, JSON_DATA 
            FROM CHAT_SESSIONS 
            WHERE 
                USER_ID = :user_id AND 
                (
                    SOURCE_URL = :source_url OR
                    JSON_VALUE(JSON_DATA, '$.source_url') = :source_url OR
                    JSON_VALUE(JSON_DATA, '$.source') = :source_url OR
                    INSTR(JSON_DATA, :source_url_like) > 0
                )
            ORDER BY UPDATED_AT DESC
        """
        
        query_params = {
            'user_id': user_id, 
            'source_url': source_level_url,
            'source_url_like': f'"{source_level_url}"'
        }
        
        # Additional full JSON search
        print(f"Executing query: {base_query}")
        print(f"Query parameters: {query_params}")
        
        cursor.execute(base_query, query_params)
        rows = cursor.fetchall()
        
        print(f"Found {len(rows)} conversation rows for user {user_id} and source {source_level_url}")
        
        # If no rows found, try a more aggressive search
        if len(rows) == 0:
            print("No rows found. Attempting more aggressive search...")
            fallback_query = """
                SELECT ID, JSON_DATA 
                FROM CHAT_SESSIONS 
                WHERE 
                    USER_ID = :user_id AND 
                    (
                        SOURCE_URL LIKE :source_url_like OR
                        INSTR(JSON_DATA, :source_url_simple) > 0
                    )
                ORDER BY UPDATED_AT DESC
            """
            fallback_params = {
                'user_id': user_id,
                'source_url_like': f'%{source_level_url}%',
                'source_url_simple': source_level_url
            }
            
            print(f"Executing fallback query: {fallback_query}")
            print(f"Fallback parameters: {fallback_params}")
            
            cursor.execute(fallback_query, fallback_params)
            rows = cursor.fetchall()
            print(f"Fallback search found {len(rows)} rows")
        
        conversations = []
        for row_id, data in rows:
            try:
                if not data:
                    continue
                    
                # Handle Oracle LOB objects
                if hasattr(data, 'read'):
                    data = data.read()
                    
                # Parse JSON data
                if isinstance(data, str):
                    session = json.loads(data)
                elif isinstance(data, bytes):
                    session = json.loads(data.decode('utf-8'))
                else:
                    session = data
                
                # Print full session data for debugging
                print(f"\nFull Session Data for row {row_id}:")
                print(json.dumps(session, indent=2))
                
                # Extract session info
                session_id = session.get("session_id", f"unknown-{row_id}")
                title = session.get("title", "Untitled Conversation")
                session_user_id = session.get("user_id", user_id)
                
                # Get messages
                messages = session.get("messages", [])
                last_message = ""
                if messages and len(messages) > 0:
                    # Get the last message, preferring assistant's message
                    for msg in reversed(messages):
                        if isinstance(msg, dict) and 'content' in msg:
                            last_message = msg.get('content', '')[:50] + "..."  # Truncate for summary
                            break
                
                # Get the last_updated time
                last_updated = session.get("last_updated", session.get("created_at", "Unknown"))
                
                # Determine source URL with multiple fallback methods
                possible_source_urls = [
                    session.get("source_url"),
                    session.get("source"),
                    session.get("source_level_url"),
                    source_level_url
                ]
                session_source_url = next((url for url in possible_source_urls if url), source_level_url)
                
                # Create summary for this conversation
                conversation = {
                    "session_id": session_id,
                    "title": title,
                    "last_message": last_message,
                    "last_updated": last_updated,
                    "message_count": len(messages),
                    "user_id": session_user_id,
                    "source_url": session_source_url
                }
                
                print(f"Processed Conversation: {conversation}")
                conversations.append(conversation)
                
            except Exception as row_error:
                print(f"Error processing row {row_id}: {str(row_error)}")
                traceback.print_exc()
                # Continue to next row
        
        print(f"Returning {len(conversations)} conversations after filtering")
        return conversations
        
    except Exception as e:
        print(f"Comprehensive error in get_all_conversations: {e}")
        traceback.print_exc()
        return []
    finally:
        if cursor:
            cursor.close()


def migrate_missing_source_urls(db_conn):
    """
    Migrate existing sessions that have null source_url fields
    by extracting URLs from their title fields
    """
    cursor = None
    try:
        cursor = db_conn.cursor()
        
        # Find sessions with missing source_url
        cursor.execute("""
            SELECT ID, JSON_DATA 
            FROM CHAT_SESSIONS 
            WHERE JSON_VALUE(JSON_DATA, '$.source_url') IS NULL
        """)
        
        rows = cursor.fetchall()
        print(f"Found {len(rows)} sessions with missing source_url")
        
        updated_count = 0
        
        for row_id, data in rows:
            try:
                # Parse the JSON data
                if hasattr(data, 'read'):
                    data = data.read()
                    
                session_data = json.loads(data)
                
                # Extract URL from title if possible
                title = session_data.get('title', '')
                source_url = None
                
                if title.startswith('Conversation about '):
                    source_url = title[19:]  # Extract URL from "Conversation about URL"
                
                if source_url:
                    # Update the session with the extracted source_url
                    session_data['source_url'] = source_url
                    
                    # Update the database
                    cursor.execute("""
                        UPDATE CHAT_SESSIONS 
                        SET JSON_DATA = :1 
                        WHERE ID = :2
                    """, [json.dumps(session_data), row_id])
                    
                    updated_count += 1
                else:
                    # If no URL in title, use a default
                    session_data['source_url'] = "Unknown source"
                    cursor.execute("""
                        UPDATE CHAT_SESSIONS 
                        SET JSON_DATA = :1 
                        WHERE ID = :2
                    """, [json.dumps(session_data), row_id])
                    updated_count += 1
            except Exception as session_error:
                print(f"Error updating session {row_id}: {str(session_error)}")
                continue
        
        # Commit all changes
        db_conn.commit()
        print(f"Updated {updated_count} sessions with source_url from title")
        
        return updated_count
    except Exception as e:
        print(f"Migration error: {str(e)}")
        traceback.print_exc()
        
        if db_conn:
            db_conn.rollback()
        return 0
    finally:
        if cursor:
            cursor.close()


# Chatbot Functions
def verify_api_key(api_key):
    """Verify if the provided API key is valid"""
    if not VALID_API_KEYS or api_key in VALID_API_KEYS:
        return True
    return False

async def process_chat_request(db_conn, message, session_id=None, api_key=None, source_level_url=None, user_id=None, use_prefetch=False, stream=False):
    """
    Process a chat request with optional streaming support
    
    Args:
        db_conn: Database connection
        message: User message
        session_id: Optional session ID
        api_key: API key for authorization
        source_level_url: URL to restrict the knowledge retrieval context (REQUIRED)
        user_id: User ID of the authenticated user
        use_prefetch: Whether to check for and use prefetched context (ignored - direct access used)
        stream: Whether to stream the response
        
    Returns:
        ChatResponse object or an async generator if streaming
    """
    # Check if message is a greeting or simple conversation starter
    greeting_patterns = [
        r'^hi\s*$', r'^hello\s*$', r'^hey\s*$', r'^greetings\s*$', 
        r'^how are you', r'^how\'s it going', r'^how r u', r'^what\'s up',
        r'^good morning', r'^good afternoon', r'^good evening',
        r'^thanks', r'^thank you'
    ]
    
    is_greeting = any(re.search(pattern, message.lower()) for pattern in greeting_patterns)
    
    if is_greeting:
        greeting_response = "Hello! I'm your AI assistant for answering questions about the content you've provided. What would you like to know about this topic?"
        
        # If this is a greeting and we're streaming, handle it directly
        if stream:
            async def greeting_generator():
                yield greeting_response
            
            # Still update the session if provided
            if session_id:
                await save_message(db_conn, session_id, message, greeting_response, user_id, source_level_url)
                
            return greeting_generator()
        else:
            # For non-streaming, create a proper response object
            session_title = f"Conversation about {source_level_url}" if source_level_url else "New Conversation"
            response = ChatResponse(
                answer=greeting_response,
                sources=[],
                session_id=session_id or str(int(time.time())),
                title=session_title
            )
            
            # Save the greeting exchange if we have a session
            if session_id:
                await save_message(db_conn, session_id, message, greeting_response, user_id, source_level_url)
                
            return response
    
    # Enforce source_level_url requirement for non-greeting questions
    if not source_level_url:
        if stream:
            async def error_generator():
                yield "I need a specific source URL to answer your question. Please provide a valid source_level_url parameter."
            return error_generator()
        else:
            return ChatResponse(
                answer="I need a specific source URL to answer your question. Please provide a valid source_level_url parameter.",
                sources=[],
                session_id=session_id or str(int(time.time())),
                title="Error: Missing Source URL"
            )

    # Create enhanced cache key
    cache_key = get_enhanced_cache_key(message, source_level_url, user_id)
    
    # Get force_refresh from request arguments if available
    force_refresh = False
    
    # Check cache first - fast path for cached responses
    if not force_refresh and not stream:  # Skip cache for streaming
        cached_response = get_cached_response(cache_key)
        if cached_response:
            print(f"🔥 Cache hit for query: {message[:30]}...")
            
            # Still update the session to record this interaction
            if session_id:
                await save_message(db_conn, session_id, message, cached_response.answer, user_id, source_level_url)
                
            return cached_response

    try:
        # Verify API key if required
        if VALID_API_KEYS and not verify_api_key(api_key):
            raise Exception("Invalid API Key")
        
        # Step 1: Get or create a session
        if session_id:
            # Try to get existing session only if session_id is provided
            print(f"Attempting to retrieve existing session with ID: {session_id}")
            session = await get_session(db_conn, session_id)
            if not session:
                # If session not found with provided ID, create a new one
                print(f"Session {session_id} not found, creating new session")
                timestamp_id = str(int(time.time()))
                session_id = timestamp_id  # Use new timestamp-based ID
                session_title = f"Conversation about {source_level_url}"
                
                # Create a new session explicitly
                new_session = {
                    "session_id": session_id,
                    "title": session_title,
                    "user_id": user_id,
                    "source_url": source_level_url,
                    "messages": [],
                    "created_at": datetime.utcnow().isoformat(),
                    "last_updated": datetime.utcnow().isoformat()
                }
                
                # Insert the new session
                cursor = db_conn.cursor()
                insert_query = """
                    INSERT INTO CHAT_SESSIONS 
                    (SESSION_ID, USER_ID, SOURCE_URL, JSON_DATA, CREATED_AT, UPDATED_AT)
                    VALUES (:session_id, :user_id, :source_url, :json_data, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                
                try:
                    cursor.execute(
                        insert_query, 
                        session_id=session_id,
                        user_id=user_id,
                        source_url=source_level_url,
                        json_data=json.dumps(new_session)
                    )
                    db_conn.commit()
                    print(f"Created new session with ID: {session_id}")
                except Exception as insert_error:
                    print(f"Error creating session: {insert_error}")
                    traceback.print_exc()
                finally:
                    cursor.close()
                
                session = new_session
        else:
            # No session ID provided, ALWAYS create a new one
            print("No session_id provided, creating new session")
            timestamp_id = str(int(time.time()))
            session_id = timestamp_id
            session_title = f"Conversation about {source_level_url}"
            
            # Create a new session explicitly
            new_session = {
                "session_id": session_id,
                "title": session_title,
                "user_id": user_id,
                "source_url": source_level_url,
                "messages": [],
                "created_at": datetime.utcnow().isoformat(),
                "last_updated": datetime.utcnow().isoformat()
            }
            
            # Insert the new session
            cursor = db_conn.cursor()
            insert_query = """
                INSERT INTO CHAT_SESSIONS 
                (SESSION_ID, USER_ID, SOURCE_URL, JSON_DATA, CREATED_AT, UPDATED_AT)
                VALUES (:session_id, :user_id, :source_url, :json_data, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            
            try:
                cursor.execute(
                    insert_query, 
                    session_id=session_id,
                    user_id=user_id,
                    source_url=source_level_url,
                    json_data=json.dumps(new_session)
                )
                db_conn.commit()
                print(f"Created new session with ID: {session_id}")
            except Exception as insert_error:
                print(f"Error creating session: {insert_error}")
                traceback.print_exc()
            finally:
                cursor.close()
            
            session = new_session
        
        title = session.get("title", f"Conversation about {source_level_url}")
        
        # Get conversation history for context
        messages = session.get("messages", [])
        formatted_history = ""
        recent_messages = messages[-6:] if len(messages) > 6 else messages
        for msg in recent_messages:
            role = msg.get('role', 'unknown')
            content = msg.get('content', '')
            if role.lower() == 'user':
                formatted_history += f"Human: {content}\n\n"
            else:
                formatted_history += f"Assistant: {content}\n\n"
        
        # DIRECT VECTOR SEARCH - Skip prefetch entirely
        with get_vectdb_connection() as vectdb_connection:
            # First try direct metadata search for speed (new optimization)
            print(f"Attempting direct metadata search for {source_level_url}")
            retrieved_docs = direct_metadata_search(vectdb_connection, source_level_url, k=15)
            
            # If direct search yields insufficient results, fall back to vector search
            if len(retrieved_docs) < 3:
                print(f"Direct search found only {len(retrieved_docs)} docs, falling back to vector search")
                # Initialize the vector store
                vector_store = oraclevs.OracleVS(
                    client=vectdb_connection,
                    embedding_function=get_embeddings(),
                    table_name=VECTDB_TABLE_NAME,
                    distance_strategy=DistanceStrategy.COSINE,
                )
                
                # Direct vector search - retrieve relevant documents
                print(f"Performing vector search for query: {message[:50]}...")
                
                # Use a more targeted search by including the source URL in the query
                search_query = f"{message} site:{source_level_url}"
                
                # Execute the search (with a reasonable timeout)
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        vector_store.similarity_search_with_relevance_scores,
                        search_query,
                        k=10  # Retrieve top 10 documents
                    )
                    
                    try:
                        # Set a 7-second timeout for vector search (reduced from 10)
                        search_results = await asyncio.get_event_loop().run_in_executor(
                            None, lambda: future.result(timeout=7)
                        )
                        
                        # Extract documents (and filter by source URL if needed)
                        for doc_tuple in search_results:
                            doc, score = doc_tuple
                            
                            # Filter documents by relevance score
                            if score > 0.3:  # Only keep if somewhat relevant
                                retrieved_docs.append(doc)
                                
                    except concurrent.futures.TimeoutError:
                        print("Vector search timed out")
                        if len(retrieved_docs) == 0:
                            error_message = "I'm having trouble accessing the knowledge base right now. Please try a simpler query or try again later."
                            
                            if stream:
                                async def timeout_generator():
                                    yield error_message
                                return timeout_generator()
                            else:
                                return ChatResponse(
                                    answer=error_message,
                                    sources=[],
                                    session_id=session_id,
                                    title=title
                                )
                    except Exception as search_error:
                        print(f"Error in vector search: {search_error}")
                        # Continue with whatever documents we already have
            
            # Extract sources from documents
            sources = []
            seen_urls = set()
            
            for doc in retrieved_docs:
                # Extract source URL
                url = source_level_url  # Default
                title = None
                
                if isinstance(doc.metadata, dict):
                    url = doc.metadata.get("url", doc.metadata.get("source", source_level_url))
                    title = doc.metadata.get("title")
                
                # Add source if unique
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    source_entry = {"url": url}
                    if title:
                        source_entry["title"] = title
                    sources.append(source_entry)
            
            print(f"Retrieved {len(retrieved_docs)} relevant documents")
            
            # Handle the case where no documents were found
            if not retrieved_docs:
                error_message = f"I couldn't find any relevant information about '{message}' in the provided source."
                
                if stream:
                    async def error_generator():
                        yield error_message
                    return error_generator()
                else:
                    return ChatResponse(
                        answer=error_message,
                        sources=[],
                        session_id=session_id,
                        title=title
                    )
            
            # Apply the new truncation function to limit content size
            retrieved_docs = truncate_documents(retrieved_docs, max_chars=15000)
            
            # Create context from retrieved documents
            context_text = ""
            for i, doc in enumerate(retrieved_docs):
                context_text += f"\n--- Document {i+1} ---\n{doc.page_content}\n"
        
        # Use a prompt template that emphasizes ONLY using the provided context
        prompt_template = """
        You are a helpful assistant that answers questions based on the provided documents.

        Follow these rules strictly:
        1. Base your answer ONLY on information from the provided documents.
        2. If the answer isn't found in the documents, say "I don't have information about that in the provided documents."
        3. Do not make up or infer information that isn't explicitly stated in the documents.
        4. Answer in a clear, concise, and helpful manner and elaborately
        5. If the documents contain technical information, explain its technical details.
        6. If a polite greeting is given, respond in a friendly way before addressing the question.
        7. If the documents contain multiple perspectives or options, present them fairly.
        8. If the documents mention uncertainties, acknowledge them in your response.
        9. Organize your response clearly with appropriate structure if the answer is complex.
        10. If the documents contain information minutely answer by explaining.

        Chat history (for context only, do not use as the primary source of information):
        {chat_history}

        Documents (use these as your primary source of information):
        {context}RetryClaude does not have the ability to run the code it generates yet.Claude can make mistakes. Please double-check responses.
        
        Human: {question}
        Assistant:
        """
        
        # Handle streaming vs. non-streaming responses
        if stream:
            # For streaming, use the model directly with streaming enabled
            model = genai.GenerativeModel(
                model_name="gemini-2.0-flash",
                generation_config={"temperature": 0.2}
            )
            
            async def response_generator():
                # Create the prompt with all necessary context
                formatted_prompt = prompt_template.format(
                    context=context_text,
                    question=message,
                    chat_history=formatted_history
                )
                
                # Start the streaming generation
                stream_response = model.generate_content(
                    formatted_prompt,
                    stream=True
                )
                
                full_response = ""
                for chunk in stream_response:
                    if hasattr(chunk, 'text'):
                        chunk_text = chunk.text
                        # Only yield non-empty chunks
                        if chunk_text:
                            yield chunk_text
                            full_response += chunk_text
                
                # Add source attribution at the end
                source_note = f"\n\nInformation provided is from documents related to: {source_level_url}"
                yield source_note
                full_response += source_note
                
                # Save the complete message after streaming is done
                await save_message(db_conn, session_id, message, full_response, user_id, source_level_url)
            
            return response_generator()
        else:
            # Non-streaming approach
            # Initialize model with appropriate temperature
            model = get_gemini_model("gemini-2.0-flash", 0.2)  # Lower temperature for more factual responses
            
            prompt = PromptTemplate(
                template=prompt_template,
                input_variables=["context", "question", "chat_history"]
            )
            
            # Use load_qa_chain
            chain = load_qa_chain(model, chain_type="stuff", prompt=prompt)
            
            # Get response
            chain_response = chain(
                {
                    "input_documents": [Document(page_content=context_text)],
                    "question": message,
                    "chat_history": formatted_history
                },
                return_only_outputs=True
            )
            
            answer_text = chain_response["output_text"]
            
            # Add explicit source attribution
            source_note = f"\n\nInformation provided is from documents related to: {source_level_url}"
            answer_text += source_note
                
            # Save the conversation update
            await save_message(db_conn, session_id, message, answer_text, user_id, source_level_url)
            
            # Return structured response
            response = ChatResponse(
                answer=answer_text,
                sources=[s.get("url") for s in sources if s.get("url")],
                session_id=session_id,
                title=title
            )
            
            # Cache the response
            cache_response(cache_key, response, CACHE_TTL_MEDIUM)
            return response
    
    except Exception as e:
        print(f"Error processing chat request: {e}")
        traceback.print_exc()
        error_message = f"An error occurred while processing your request: {str(e)}"
        
        # Try to update session with error message if possible
        try:
            if db_conn and session_id:
                await save_message(db_conn, session_id, message, error_message, user_id, source_level_url)
        except:
            pass
            
        if stream:
            async def error_generator():
                yield error_message
            return error_generator()
        else:
            return ChatResponse(
                answer=error_message,
                sources=[],
                session_id=session_id if session_id else "error",
                title="Error Response"
            )
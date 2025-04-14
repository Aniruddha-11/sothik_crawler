from flask import Blueprint, request, jsonify
import os
import json
from datetime import datetime
import traceback
import asyncio
from functools import wraps
from connection import connect_to_vectdb, get_jsondb_connection, get_vectdb_connection
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_community.vectorstores import oraclevs
from langchain.docstore.document import Document
from langchain_community.vectorstores.utils import DistanceStrategy
import threading
import time
import concurrent.futures
import hashlib
from connection import connect_to_vectdb, get_vectdb_connection
from shared_utils import get_cache_key, prefetched_contexts, CACHE_TTL_MEDIUM

# Create a blueprint for our prefetch API
prefetch_api = Blueprint('prefetch_api', __name__)

# Dictionary to store pre-fetched context data
# Key: source_level_url, Value: {data: context_data, timestamp: last_refresh_time}
prefetched_contexts = {}

# Configuration
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
VECTDB_TABLE_NAME = os.getenv("VECTDB_TABLE_NAME", "vector_files_with_10000_chunk_new")
PREFETCH_EXPIRY_SECONDS = 3600  # 1 hour cache expiration

# Global tracking for in-progress prefetches
prefetch_in_progress = set()
prefetch_lock = threading.RLock()
last_prefetch_time = {}  # Track when URL was last prefetched
MIN_PREFETCH_INTERVAL = 1800  # Don't prefetch same URL more than once per 30 minutes

# Global embeddings instance for reuse
_global_embeddings = None
_embeddings_lock = threading.Lock()

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

# API key verification middleware
def api_key_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.json.get('api_key') if request.is_json else request.args.get('api_key')
        valid_api_keys = os.getenv("VALID_API_KEYS", "").split(",")
        
        if not valid_api_keys or api_key in valid_api_keys:
            return f(*args, **kwargs)
            
        return jsonify({
            'status': 'error',
            'message': 'Invalid or missing API key',
            'timestamp': datetime.now().isoformat()
        }), 401
            
    return decorated

def get_cache_key(source_level_url):
    """Generate a cache key for prefetched context"""
    key_data = f"prefetch:{source_level_url}"
    return hashlib.md5(key_data.encode()).hexdigest()

def should_prefetch(source_level_url):
    """Determine if a URL should be prefetched based on recency and status"""
    with prefetch_lock:
        # Already prefetching this URL
        if source_level_url in prefetch_in_progress:
            print(f"Skipping prefetch for {source_level_url} - already in progress")
            return False
            
        # Check if it was recently prefetched
        now = time.time()
        if source_level_url in last_prefetch_time:
            last_time = last_prefetch_time[source_level_url]
            if now - last_time < MIN_PREFETCH_INTERVAL:
                print(f"Skipping prefetch for {source_level_url} - recently fetched {int(now-last_time)}s ago")
                return False
            
        # Check if we already have fresh cached data
        cache_key = get_cache_key(source_level_url)
        if cache_key in prefetched_contexts:
            cache_age = now - prefetched_contexts[cache_key].get("timestamp", 0)
            # If cache is still fresh enough, don't prefetch again
            if cache_age < (PREFETCH_EXPIRY_SECONDS * 0.75):
                print(f"Skipping prefetch - existing cache is still fresh (age: {int(cache_age)}s)")
                return False
                
        # Track that we're starting a prefetch
        prefetch_in_progress.add(source_level_url)
        last_prefetch_time[source_level_url] = now
        return True

def fetch_documents_for_url(source_level_url):
    """
    Fetch ALL documents that have the specified source_level_url in their metadata
    """
    start_time = time.time()
    try:
        print(f"Starting comprehensive document prefetch for {source_level_url}")
        
        # Use connection pooling with context manager
        with get_vectdb_connection() as vectdb_connection:
            try:
                # Check table structure to determine column names
                cursor = vectdb_connection.cursor()
                
                # Get column information
                try:
                    cursor.execute(f"SELECT * FROM {VECTDB_TABLE_NAME} WHERE ROWNUM <= 1")
                    columns = [desc[0].upper() for desc in cursor.description]
                    print(f"Available columns in vector table: {columns}")
                    
                    # Find the content and metadata columns
                    content_col = None
                    metadata_col = None
                    id_col = None
                    
                    # Look for standard column names
                    for col in columns:
                        if col in ["CONTENT", "TEXT", "PAGE_CONTENT", "DOCUMENT_CONTENT"]:
                            content_col = col
                        elif col in ["METADATA", "META", "DOCUMENT_METADATA"]:
                            metadata_col = col
                        elif col in ["ROW_ID", "ID", "DOCUMENT_ID"]:
                            id_col = col
                    
                    # If we couldn't identify columns by name, use position-based approach
                    if not content_col and len(columns) >= 2:
                        content_col = columns[1]  # Second column often contains content
                    
                    if not metadata_col and len(columns) >= 3:
                        metadata_col = columns[2]  # Third column often has metadata
                    
                    if not id_col and len(columns) >= 1:
                        id_col = columns[0]  # First column is usually the ID
                    
                    print(f"Identified columns - ID: {id_col}, Content: {content_col}, Metadata: {metadata_col}")
                    
                    if not content_col or not metadata_col:
                        raise Exception(f"Could not identify content and metadata columns in {VECTDB_TABLE_NAME}")
                        
                except Exception as e:
                    print(f"Error examining table structure: {e}")
                    raise
                
                # IMPROVED APPROACH: Try multiple patterns to match source in different formats
                patterns = [
                    f'%"source":"{source_level_url}"%',      # Exact source match
                    f'%"source":"%{source_level_url}%',      # Partial source match
                    f'%"url":"{source_level_url}"%',         # Exact URL match
                    f'%"url":"%{source_level_url}%',         # Partial URL match
                    f'%"TOP_LEVEL_SOURCE":"%{source_level_url}%',  # Check for TOP_LEVEL_SOURCE
                    f'%"top_level_source":"%{source_level_url}%',  # Check lowercase variant
                    f'%{source_level_url}%'                  # Any mention of the URL
                ]
                
                query = f"""
                    SELECT {id_col}, {content_col}, {metadata_col}
                    FROM {VECTDB_TABLE_NAME}
                    WHERE {metadata_col} LIKE :pattern
                """
                
                # Union all results from different patterns
                all_results = []
                for pattern in patterns:
                    try:
                        cursor.execute(query, pattern=pattern)
                        results = cursor.fetchall()
                        print(f"Found {len(results)} documents with pattern: {pattern}")
                        all_results.extend(results)
                    except Exception as query_error:
                        print(f"Error with pattern {pattern}: {query_error}")
                
                # Remove duplicates by ID
                seen_ids = set()
                direct_results = []
                for result in all_results:
                    result_id = result[0]
                    if result_id not in seen_ids:
                        seen_ids.add(result_id)
                        direct_results.append(result)
                
                print(f"Found {len(direct_results)} unique metadata matches for {source_level_url}")
                
                # If no direct matches, try semantic search as backup
                vector_docs = []
                if len(direct_results) == 0:
                    # Initialize vector store for semantic search
                    try:
                        vector_store = oraclevs.OracleVS(
                            client=vectdb_connection,
                            embedding_function=get_embeddings(),
                            table_name=VECTDB_TABLE_NAME,
                            distance_strategy=DistanceStrategy.COSINE,
                        )
                        
                        # Get documents through vector search
                        vector_docs = vector_store.similarity_search(
                            source_level_url, 
                            k=50  # Get more results for better coverage
                        )
                        print(f"Found {len(vector_docs)} docs through vector search for {source_level_url}")
                        
                        # Use ALL vector results since we couldn't find direct matches
                        # Only filter if we have more than 30 documents to avoid empty results
                        if len(vector_docs) > 30:
                            # Do minimal filtering to ensure some relevance
                            filtered_docs = []
                            for doc in vector_docs:
                                # Check if any metadata field contains our source URL
                                metadata_str = str(doc.metadata).lower()
                                if source_level_url.lower() in metadata_str:
                                    filtered_docs.append(doc)
                            
                            # If we found at least some matches, use those
                            if len(filtered_docs) > 0:
                                vector_docs = filtered_docs
                                print(f"Filtered to {len(vector_docs)} docs with source URL in metadata")
                    except Exception as vs_error:
                        print(f"Error in vector search: {vs_error}")
                        vector_docs = []
                
                # Process and combine the results
                documents = []
                seen_content_hashes = set()
                
                # Process direct database results
                for row in direct_results:
                    try:
                        # Extract ID, content and metadata from the row
                        row_id = row[0]
                        content_raw = row[1]
                        metadata_raw = row[2]
                        
                        # Handle Oracle LOB objects for content
                        if hasattr(content_raw, 'read'):
                            content = content_raw.read()
                        else:
                            content = str(content_raw)
                            
                        # Skip empty content
                        if not content:
                            continue
                            
                        # Create a hash to identify duplicate content
                        content_hash = hashlib.md5(str(content).encode()).hexdigest()
                        if content_hash in seen_content_hashes:
                            continue
                            
                        seen_content_hashes.add(content_hash)
                        
                        # Parse metadata - properly handle Oracle LOB objects
                        metadata = {"source": source_level_url}  # Default metadata
                        if metadata_raw:
                            try:
                                # Handle Oracle LOB objects for metadata
                                if hasattr(metadata_raw, 'read'):
                                    metadata_str = metadata_raw.read()
                                else:
                                    metadata_str = str(metadata_raw)
                                
                                # Try to parse as JSON
                                if metadata_str and isinstance(metadata_str, str):
                                    try:
                                        parsed_metadata = json.loads(metadata_str)
                                        if isinstance(parsed_metadata, dict):
                                            metadata = parsed_metadata
                                    except json.JSONDecodeError:
                                        # Not valid JSON, keep default metadata
                                        pass
                            except Exception as meta_error:
                                print(f"Error parsing metadata: {str(meta_error)}")
                                
                        # Create Document object - accept ANY document from the query results
                        # since we've already filtered with SQL
                        documents.append(Document(
                            page_content=str(content),
                            metadata=metadata
                        ))
                    except Exception as doc_error:
                        print(f"Error processing DB document: {doc_error}")
                
                # Add documents from vector search results if we have no direct matches
                if len(documents) == 0 and vector_docs:
                    print("No direct matches found, using vector search results")
                    for doc in vector_docs:
                        try:
                            # Skip duplicates
                            content_hash = hashlib.md5(str(doc.page_content).encode()).hexdigest()
                            if content_hash in seen_content_hashes:
                                continue
                                
                            seen_content_hashes.add(content_hash)
                            documents.append(doc)
                        except Exception as doc_error:
                            print(f"Error processing vector document: {doc_error}")
                
                print(f"Final document count: {len(documents)}")
                
                if not documents:
                    print(f"No documents found for {source_level_url}")
                    return None
                
                # Process documents into context and sources
                contexts = []
                sources = []
                seen_urls = set()
                
                for doc in documents:
                    # Add page content
                    contexts.append(doc.page_content)
                    
                    # Extract source info
                    try:
                        metadata = doc.metadata
                        url = source_level_url  # Default to the requested URL
                        title = None
                        
                        if isinstance(metadata, dict):
                            url = metadata.get("url", metadata.get("source", source_level_url))
                            title = metadata.get("title")
                        
                        # Add source if unique
                        if url and url not in seen_urls:
                            seen_urls.add(url)
                            source_entry = {"url": url}
                            if title:
                                source_entry["title"] = title
                            sources.append(source_entry)
                    except Exception as source_error:
                        print(f"Error extracting source info: {source_error}")
                
                # Create a structured context with document separation
                context_text = ""
                for i, ctx in enumerate(contexts):
                    # Add separator between documents
                    context_text += f"\n--- Document {i+1} ---\n{ctx}\n"
                
                result = {
                    "context": context_text,
                    "sources": sources,
                    "document_count": len(documents),
                    "source_urls": list(seen_urls) or [source_level_url],
                    "processed_docs": len(documents),
                    "timestamp": datetime.now().isoformat()
                }
                
                print(f"Successfully processed {len(documents)} documents for {source_level_url} in {time.time() - start_time:.2f}s")
                return result
                
            except Exception as e:
                print(f"Error in document fetch: {e}")
                traceback.print_exc()
                return None
                
    except Exception as e:
        print(f"Error fetching documents for {source_level_url}: {e}")
        traceback.print_exc()
        return None
    finally:
        # Always remove from in-progress tracking, even on error
        with prefetch_lock:
            if source_level_url in prefetch_in_progress:
                prefetch_in_progress.remove(source_level_url)
        
def background_prefetch(source_level_url):
    """
    Background task to prefetch and update context with improved error handling
    and retry logic
    """
    try:
        if not should_prefetch(source_level_url):
            print(f"Skipping prefetch for {source_level_url}")
            return
            
        print(f"Starting optimized prefetch for {source_level_url}")
        
        # Perform the fetch
        result = fetch_documents_for_url(source_level_url)
        
        if result:
            cache_key = get_cache_key(source_level_url)
            prefetched_contexts[cache_key] = {
                "data": result,
                "timestamp": time.time(),
                "source_url": source_level_url
            }
            print(f"✅ Successfully prefetched context for {source_level_url} with {result.get('document_count', 0)} documents")
        else:
            # If first attempt failed, wait and retry once
            print(f"Initial prefetch failed for {source_level_url}, retrying in 2 seconds...")
            time.sleep(2)
            
            # Second try with different approach - just use vector store directly
            try:
                with get_vectdb_connection() as vectdb_connection:
                    vector_store = oraclevs.OracleVS(
                        client=vectdb_connection,
                        embedding_function=get_embeddings(),
                        table_name=VECTDB_TABLE_NAME,
                        distance_strategy=DistanceStrategy.COSINE,
                    )
                    
                    # Try direct vector search with source URL as query
                    docs = vector_store.similarity_search(
                        source_level_url, 
                        k=30  # Get more results
                    )
                    
                    if docs:
                        # Process documents into context and sources
                        contexts = []
                        sources = []
                        seen_urls = set()
                        
                        for doc in docs:
                            contexts.append(doc.page_content)
                            
                            # Extract source URL
                            url = "Unknown source"
                            title = None
                            
                            if isinstance(doc.metadata, dict):
                                url = doc.metadata.get("url", "Unknown source")
                                title = doc.metadata.get("title")
                            
                            # Add source if unique
                            if url and url not in seen_urls:
                                seen_urls.add(url)
                                source_entry = {"url": url}
                                if title:
                                    source_entry["title"] = title
                                sources.append(source_entry)
                        
                        # Create a structured context
                        context_text = ""
                        for i, ctx in enumerate(contexts):
                            context_text += f"\n--- Document {i+1} ---\n{ctx}\n"
                            
                        result = {
                            "context": context_text,
                            "sources": sources,
                            "document_count": len(docs),
                            "source_urls": list(seen_urls),
                            "processed_docs": len(docs),
                            "timestamp": datetime.now().isoformat()
                        }
                        
                        # Update the cache with the backup method results
                        cache_key = get_cache_key(source_level_url)
                        prefetched_contexts[cache_key] = {
                            "data": result,
                            "timestamp": time.time(),
                            "source_url": source_level_url
                        }
                        print(f"✅ Updated prefetched context for {source_level_url} using backup method with {len(docs)} documents")
                    else:
                        print(f"❌ Backup prefetch method also failed for {source_level_url}")
            except Exception as backup_error:
                print(f"Error in backup prefetch method: {backup_error}")
                traceback.print_exc()
    except Exception as e:
        print(f"Error in background prefetch for {source_level_url}: {e}")
        traceback.print_exc()
    finally:
        # Always clean up tracking, even on error
        with prefetch_lock:
            if source_level_url in prefetch_in_progress:
                prefetch_in_progress.remove(source_level_url)

@prefetch_api.route('/prefetch', methods=['POST'])
@api_key_required
def prefetch_context():
    """
    Manually trigger prefetching for a specific source_level_url
    with more detailed error handling and status tracking
    
    Request body:
    - source_level_url: URL to prefetch documents for
    - api_key: API key for authentication
    """
    try:
        start_time = time.time()
        data = request.json
        source_level_url = data.get('source_level_url')
        force_refresh = data.get('force_refresh', True)
        
        if not source_level_url:
            return jsonify({
                'status': 'error',
                'message': 'source_level_url is required',
                'timestamp': datetime.now().isoformat()
            }), 400
        
        # Check if we already have cached data
        cache_key = get_cache_key(source_level_url)
        current_time = time.time()
        
        if cache_key in prefetched_contexts and not force_refresh:
            cache_entry = prefetched_contexts[cache_key]
            cache_age = current_time - cache_entry.get("timestamp", 0)
            
            # If cache is fresh enough, return status
            if cache_age < PREFETCH_EXPIRY_SECONDS:
                return jsonify({
                    'status': 'success',
                    'message': f'Context for {source_level_url} already prefetched and up to date',
                    'document_count': cache_entry["data"]["document_count"],
                    'source_count': len(cache_entry["data"].get("sources", [])),
                    'age_seconds': int(cache_age),
                    'prefetched': True,
                    'process_time': f"{time.time() - start_time:.2f}s",
                    'timestamp': datetime.now().isoformat()
                })
                
            # If stale but exists, mention it's being refreshed
            message = f'Refreshing stale prefetched context for {source_level_url} (age: {int(cache_age)}s)'
        else:
            message = f'Starting prefetch for {source_level_url}'
        
        # Check if a prefetch is already in progress
        with prefetch_lock:
            already_prefetching = source_level_url in prefetch_in_progress
            
        if already_prefetching and not force_refresh:
            return jsonify({
                'status': 'info',
                'message': f'Prefetch for {source_level_url} already in progress',
                'prefetch_status': 'processing',
                'process_time': f"{time.time() - start_time:.2f}s",
                'timestamp': datetime.now().isoformat()
            })
        
        # Start immediate prefetch for synchronous response
        if force_refresh or data.get('wait', True):
            # Perform prefetch now and wait for result
            try:
                if should_prefetch(source_level_url) or force_refresh:
                    result = fetch_documents_for_url(source_level_url)
                    if result:
                        prefetched_contexts[cache_key] = {
                            "data": result,
                            "timestamp": time.time(),
                            "source_url": source_level_url
                        }
                        
                        return jsonify({
                            'status': 'success',
                            'message': f'Successfully prefetched context for {source_level_url}',
                            'document_count': result["document_count"],
                            'source_count': len(result.get("sources", [])),
                            'prefetched': True,
                            'process_time': f"{time.time() - start_time:.2f}s",
                            'timestamp': datetime.now().isoformat()
                        })
                    else:
                        return jsonify({
                            'status': 'error',
                            'message': f'Failed to prefetch context for {source_level_url}',
                            'process_time': f"{time.time() - start_time:.2f}s",
                            'timestamp': datetime.now().isoformat()
                        }), 500
                else:
                    return jsonify({
                        'status': 'info',
                        'message': f'Skipped prefetch for {source_level_url}',
                        'prefetch_status': 'skipped',
                        'reason': 'Recently prefetched or already in progress',
                        'process_time': f"{time.time() - start_time:.2f}s",
                        'timestamp': datetime.now().isoformat()
                    })
            except Exception as direct_error:
                print(f"Error in direct prefetch: {direct_error}")
                traceback.print_exc()
                
                # Fall back to async prefetch on error
                threading.Thread(
                    target=background_prefetch,
                    args=(source_level_url,)
                ).start()
        else:
            # Start background prefetch
            threading.Thread(
                target=background_prefetch,
                args=(source_level_url,)
            ).start()
        
        return jsonify({
            'status': 'success',
            'message': message,
            'prefetch_status': 'processing',
            'process_time': f"{time.time() - start_time:.2f}s",
            'timestamp': datetime.now().isoformat()
        })
    
    except Exception as e:
        print(f"Error in prefetch endpoint: {e}")
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@prefetch_api.route('/status', methods=['GET'])
@api_key_required
def get_prefetch_status():
    """
    Get the status of prefetched context for a source_level_url with enhanced diagnostics
    
    Query parameters:
    - source_level_url: URL to check status for
    - api_key: API key for authentication
    """
    try:
        start_time = time.time()
        source_level_url = request.args.get('source_level_url')
        
        if not source_level_url:
            # Return status for all prefetched contexts
            status_data = {}
            for key, cache_entry in prefetched_contexts.items():
                cache_age = time.time() - cache_entry.get("timestamp", 0)
                source_url = cache_entry.get("source_url", "unknown")
                
                # Get more detailed stats
                data = cache_entry.get("data", {})
                document_count = data.get("document_count", 0)
                source_count = len(data.get("sources", []))
                context_size = len(data.get("context", "")) if data else 0
                
                status_data[source_url] = {
                    'prefetched': True,
                    'document_count': document_count,
                    'source_count': source_count,
                    'context_size_chars': context_size,
                    'age_seconds': int(cache_age),
                    'fresh': cache_age < PREFETCH_EXPIRY_SECONDS,
                    'key': key
                }
                
            return jsonify({
                'status': 'success',
                'prefetched_urls': status_data,
                'cache_entries': len(prefetched_contexts),
                'in_progress': list(prefetch_in_progress),
                'process_time': f"{time.time() - start_time:.2f}s",
                'timestamp': datetime.now().isoformat()
            })
        
        # Check status for specific URL
        cache_key = get_cache_key(source_level_url)
        if cache_key in prefetched_contexts:
            cache_entry = prefetched_contexts[cache_key]
            cache_age = time.time() - cache_entry.get("timestamp", 0)
            
            # Get more detailed stats
            data = cache_entry.get("data", {})
            document_count = data.get("document_count", 0)
            source_count = len(data.get("sources", []))
            context_size = len(data.get("context", "")) if data else 0
            
            # Check if prefetch is currently running
            with prefetch_lock:
                in_progress = source_level_url in prefetch_in_progress
            
            return jsonify({
                'status': 'success',
                'url': source_level_url,
                'prefetched': True,
                'document_count': document_count,
                'source_count': source_count,
                'context_size_chars': context_size,
                'age_seconds': int(cache_age),
                'fresh': cache_age < PREFETCH_EXPIRY_SECONDS,
                'in_progress': in_progress,
                'key': cache_key,
                'process_time': f"{time.time() - start_time:.2f}s",
                'timestamp': datetime.now().isoformat()
            })
        else:
            # Check if prefetch is in progress
            with prefetch_lock:
                in_progress = source_level_url in prefetch_in_progress
                
            # Try to find any related prefetched context
            related_urls = []
            for key, entry in prefetched_contexts.items():
                entry_url = entry.get("source_url", "")
                if source_level_url in entry_url or entry_url in source_level_url:
                    related_urls.append({
                        'url': entry_url,
                        'key': key,
                        'age_seconds': int(time.time() - entry.get("timestamp", 0)),
                        'document_count': entry.get("data", {}).get("document_count", 0)
                    })
            
            return jsonify({
                'status': 'success',
                'url': source_level_url,
                'prefetched': False,
                'in_progress': in_progress,
                'related_prefetched_urls': related_urls,
                'message': f'No prefetched context found for {source_level_url}' +
                           (', but prefetch is in progress' if in_progress else ''),
                'process_time': f"{time.time() - start_time:.2f}s",
                'timestamp': datetime.now().isoformat()
            })
    
    except Exception as e:
        print(f"Error in status endpoint: {e}")
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# Clean expired cache entries periodically
def clean_prefetch_cache():
    """Remove expired entries from the prefetch cache"""
    try:
        current_time = time.time()
        keys_to_remove = []
        
        for key, cache_entry in prefetched_contexts.items():
            cache_age = current_time - cache_entry.get("timestamp", 0)
            if cache_age > PREFETCH_EXPIRY_SECONDS:
                keys_to_remove.append(key)
                
        for key in keys_to_remove:
            del prefetched_contexts[key]
            
        if keys_to_remove:
            print(f"Cleaned {len(keys_to_remove)} expired prefetch cache entries")
    except Exception as e:
        print(f"Error cleaning prefetch cache: {e}")

# Function to check if there's prefetched content for a URL
def has_prefetched_context(source_level_url):
    """
    Check if there's fresh prefetched context available for the given URL
    with better error handling and fallback
    
    Args:
        source_level_url: The URL to check
        
    Returns:
        (bool, dict): Tuple of (has_context, context_data_or_none)
    """
    if not source_level_url:
        return False, None
    
    # Try exact URL match first
    cache_key = get_cache_key(source_level_url)
    if cache_key in prefetched_contexts:
        cache_entry = prefetched_contexts[cache_key]
        cache_age = time.time() - cache_entry.get("timestamp", 0)
        
        # Check if cache is fresh
        if cache_age <= PREFETCH_EXPIRY_SECONDS:
            print(f"Found fresh prefetched context for {source_level_url}")
            return True, cache_entry["data"]
        else:
            print(f"Found stale prefetched context for {source_level_url}, age: {int(cache_age)}s")
            # Start refresh for next time if stale, but don't block current request
            if should_prefetch(source_level_url):
                threading.Thread(
                    target=background_prefetch,
                    args=(source_level_url,)
                ).start()
            # Still return the stale data
            return True, cache_entry["data"]
    
    # Try partial URL matching as fallback
    for key, entry in prefetched_contexts.items():
        entry_url = entry.get("source_url", "")
        
        # Check if this cache entry is related to our target URL
        if (source_level_url in entry_url or 
            entry_url in source_level_url or
            (entry.get("data", {}).get("source_urls", []) and 
             source_level_url in entry.get("data", {}).get("source_urls", []))):
            
            print(f"Found related prefetched context: {entry_url}")
            cache_age = time.time() - entry.get("timestamp", 0)
            
            # Start refresh for exact URL for next time, but don't block
            if should_prefetch(source_level_url):
                threading.Thread(
                    target=background_prefetch,
                    args=(source_level_url,)
                ).start()
            
            # Return the related data
            return True, entry["data"]
    
    # No suitable prefetched context found
    print(f"No prefetched context found for {source_level_url}")
    
    # Start a background prefetch for next time, if not already in progress
    if should_prefetch(source_level_url):
        threading.Thread(
            target=background_prefetch,
            args=(source_level_url,)
        ).start()
        print(f"Started background prefetch for future use: {source_level_url}")
    
    return False, None

# Initialize background thread to refresh prefetched contexts
def start_refresh_daemon():
    """Start a daemon thread to periodically refresh prefetched contexts and clean cache"""
    def refresh_worker():
        while True:
            try:
                # First clean expired cache entries
                clean_prefetch_cache()
                
                # Check all prefetched contexts
                for key, cache_entry in list(prefetched_contexts.items()):
                    cache_age = time.time() - cache_entry.get("timestamp", 0)
                    source_url = cache_entry.get("source_url")
                    
                    # Refresh if older than 3/4 of expiry time
                    if cache_age > (PREFETCH_EXPIRY_SECONDS * 0.75) and source_url:
                        if should_prefetch(source_url):
                            print(f"Auto-refreshing context for {source_url}")
                            background_prefetch(source_url)
                        
                # Sleep for a while
                time.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                print(f"Error in refresh worker: {e}")
                time.sleep(60)  # Sleep for a minute if there's an error
    
    # Start daemon thread
    thread = threading.Thread(target=refresh_worker)
    thread.daemon = True
    thread.start()
    print("Started prefetch refresh daemon")

# Start the refresh daemon when module loads
start_refresh_daemon()
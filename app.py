from flask import Flask, request, jsonify, make_response
from file_api import file_api
from user_api import user_api, login_user, register_user
from chatbot_api import chatbot_api  # Import the chatbot API blueprint
import os
import logging
import jwt
from datetime import datetime
import traceback
import time
from flask_cors import CORS  # Import Flask-CORS instead of custom middleware
from prefetch_api import prefetch_api
from connection import get_jsondb_connection, initialize_jsondb_connection_pool, initialize_vectdb_connection_pool, connect_to_jsondb, check_connection_pools
from oracle_chatbot import clear_all_caches

# Check if we're in VM mode
VM_MODE = os.getenv('VM_MODE', 'true').lower() in ('true', '1', 'yes')
ENABLE_VECTORIZATION = os.getenv('ENABLE_VECTORIZATION', 'false' if VM_MODE else 'true').lower() in ('true', '1', 'yes')
ENABLE_PREFETCH = os.getenv('ENABLE_PREFETCH', 'false').lower() in ('true', '1', 'yes')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

logger.info(f"Starting application - VM Mode: {VM_MODE}")
logger.info(f"Vectorization enabled: {ENABLE_VECTORIZATION}")
logger.info(f"Prefetch enabled: {ENABLE_PREFETCH}")

# Add resource monitoring
def check_resources():
    """Monitor resource usage"""
    try:
        import psutil
        
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent()
        
        logger.info(f"Memory usage: {memory.percent}% ({memory.used / 1024 / 1024:.1f} MB)")
        logger.info(f"CPU usage: {cpu_percent}%")
        
        # Force garbage collection if memory usage is high
        if memory.percent > 80:
            logger.warning("High memory usage detected! Cleaning caches...")
            clear_all_caches()
            
            import gc
            gc.collect()
            
            # Check memory again after cleaning
            memory_after = psutil.virtual_memory()
            logger.info(f"Memory after cleanup: {memory_after.percent}% ({memory_after.used / 1024 / 1024:.1f} MB)")
    except ImportError:
        logger.warning("psutil not installed, skipping resource monitoring")
    except Exception as e:
        logger.error(f"Error checking resources: {e}")

# Initialize a monitoring thread
def start_resource_monitor():
    def monitor_worker():
        while True:
            try:
                check_resources()
                time.sleep(300 if VM_MODE else 60)  # Check less frequently in VM mode
            except Exception as e:
                logger.error(f"Error in resource monitor: {e}")
                time.sleep(120)  # Wait longer on error
    
    from threading import Thread
    monitor_thread = Thread(target=monitor_worker, daemon=True)
    monitor_thread.start()
    logger.info("Resource monitoring started")

# Start resource monitoring if we're in VM mode
if VM_MODE:
    try:
        start_resource_monitor()
    except Exception as e:
        logger.error(f"Could not start resource monitor: {e}")

# Get environment variables
SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-fallback')

# Create the Flask app
app = Flask(__name__)

# Configure CORS with Flask-CORS
allowed_origins = ["*"]

# Add any CORS_ORIGINS from environment variables
cors_origins = os.getenv('CORS_ORIGINS', '')
if cors_origins:
    additional_origins = [origin.strip() for origin in cors_origins.split(',') if origin.strip()]
    allowed_origins.extend(additional_origins)

# Apply CORS to the app with configuration
CORS(app, resources={r"/*": {"origins": allowed_origins, "supports_credentials": True}})

# Initialize database connection pools - now with more conservative settings for VM mode
initialize_jsondb_connection_pool(use_wallet=True)
initialize_vectdb_connection_pool(use_wallet=True)

# Check connection pools during startup
pool_status = check_connection_pools()
logger.info(f"Connection pool status at startup: {pool_status}")

# Initialize the chatbot JSON database - only if vectorization is enabled
if ENABLE_VECTORIZATION:
    try:
        from oracle_chatbot import initialize_json_database
        chatbot_jsondb_connection = connect_to_jsondb()
        if chatbot_jsondb_connection:
            initialize_json_database(chatbot_jsondb_connection)
            print("Chatbot database initialized successfully")
            chatbot_jsondb_connection.close()
        else:
            print("WARNING: Failed to initialize chatbot database connection")
    except Exception as init_error:
        logger.error(f"Error initializing chatbot database: {init_error}")
        traceback.print_exc()

# Initialize tables
from file_api import initialize_tables
initialize_tables()

# Initialize user tables
from user_api import initialize_user_tables
initialize_user_tables()

# Start cache cleaning daemon for optimized performance - only if not in VM mode
if not VM_MODE:
    from oracle_chatbot import start_cache_cleaning_daemon
    start_cache_cleaning_daemon()

# Clear cache on startup
clear_all_caches()

# Standardized error response function
def standardize_error_response(error, code=None, status_code=500, process_time=None):
    """
    Create a standardized error response
    
    Args:
        error: The error message or exception
        code: Error code for frontend handling
        status_code: HTTP status code
        process_time: Processing time for the request
        
    Returns:
        JSON response with error details and status code
    """
    if isinstance(error, Exception):
        error_message = str(error)
        traceback_str = traceback.format_exc()
    else:
        error_message = error
        traceback_str = None
    
    response = {
        'status': 'error',
        'message': error_message,
        'timestamp': datetime.now().isoformat()
    }
    
    if code:
        response['code'] = code
        
    if process_time:
        response['process_time'] = process_time
    elif hasattr(request, 'start_time'):
        # If process_time is not provided but request has start_time, calculate it
        response['process_time'] = f"{time.time() - request.start_time:.2f}s"
        
    if traceback_str and status_code >= 500 and not VM_MODE:  # Only include traceback in non-VM mode
        # Include traceback in response for server errors
        logger.error(f"Server error: {error_message}\n{traceback_str}")
        response['error_details'] = traceback_str
    
    resp = jsonify(response), status_code
    return resp

# Helper function to verify JWT tokens
def verify_token():
    """Verify JWT token and get user_id"""
    token = request.headers.get('Authorization')
    
    if not token or not token.startswith("Bearer "):
        print("Invalid token format")
        return None

    token = token.split(" ")[1]
    try:
        # Use the exact same SECRET_KEY as used in token generation
        decoded = jwt.decode(token, os.getenv('SECRET_KEY'), algorithms=['HS256'])
        return decoded['user_id']
    except jwt.ExpiredSignatureError:
        print("Token has expired")
        return None
    except jwt.InvalidTokenError as e:
        print(f"Invalid Token Error: {e}")
        return None

# Handle all OPTIONS requests globally
@app.route('/', defaults={'path': ''}, methods=['OPTIONS'])
@app.route('/<path:path>', methods=['OPTIONS'])
def options_route(path):
    response = make_response()
    response.headers['Content-Type'] = 'text/plain'
    response.headers['Content-Length'] = '0'
    return response

# Root routes
@app.route('/')
def home():
    return "Backend is working!", 200

@app.route('/health')
def health_check():
    # Check connection pools health
    pool_status = check_connection_pools()
    
    # In VM mode, also do a memory check
    memory_status = {}
    if VM_MODE:
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_status = {
                "memory_usage_mb": memory_info.rss / 1024 / 1024,
                "memory_percent": process.memory_percent()
            }
        except ImportError:
            memory_status = {"message": "psutil not installed"}
        except Exception as e:
            memory_status = {"error": str(e)}
    
    return {
        "status": "healthy",
        "version": "1.0.0",
        "pools": pool_status,
        "vm_mode": VM_MODE,
        "memory": memory_status,
        "timestamp": datetime.now().isoformat()
    }, 200

# Auth Routes
@app.route('/api/users/auth/login', methods=['POST', 'OPTIONS'])
def auth_login_direct():
    if request.method == 'OPTIONS':
        return options_route('')
    return login_user()

@app.route('/api/users/auth/register', methods=['POST', 'OPTIONS'])
def auth_register_direct():
    if request.method == 'OPTIONS':
        return options_route('')
    return register_user()

@app.route('/api/users/auth/get-user-details', methods=['GET', 'OPTIONS'])
def auth_get_user_details_direct():
    if request.method == 'OPTIONS':
        return options_route('')
    
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    # Import the function from user_api
    from user_api import get_user_details
    return get_user_details(user_id)

# Import chatbot-related functions
from oracle_chatbot import (
    process_chat_request, 
    get_all_conversations, 
    get_session, 
    update_session_title,
    clear_all_caches
)

# File Routes
from file_api import (
    recursive_crawl, 
    process_all_links, 
    get_all_documents,
    get_discovered_links, 
    scrapped_sub_links,
    get_source_url_status,
    get_progress_bar,
    realtime_scrapped_links,
    realtime_pending_links,
    realtime_total_words_scrapped,
    get_total_words,
    check_vectorization_ready,
    get_vectorization_status
)

@app.route('/api/chatbot/sessions-by-url', methods=['GET', 'OPTIONS'])
def chatbot_sessions_by_url_direct():
    if request.method == 'OPTIONS':
        return options_route('')
    
    # Import the method from chatbot_api
    from chatbot_api import get_sessions_by_url
    return get_sessions_by_url()

@app.route('/api/chatbot/edit-message', methods=['POST', 'OPTIONS'])
def chatbot_edit_message_direct():
    if request.method == 'OPTIONS':
        return options_route('')
    
    # Import the method from chatbot_api
    from chatbot_api import edit_and_reprocess_message
    return edit_and_reprocess_message()

# Route registrations for file operations
@app.route('/api/files/recursive-crawl', methods=['POST', 'OPTIONS'])
def recursive_crawl_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    result = recursive_crawl(user_id)
    # Add performance timing info if it's a JSON response
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
        result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
    return result

@app.route('/api/files/process-all-links', methods=['POST', 'OPTIONS'])
def process_all_links_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    result = process_all_links(user_id)
    # Add performance timing info if it's a JSON response
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
        result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
    return result

@app.route('/api/files/all-documents', methods=['GET', 'OPTIONS'])
def all_documents_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    result = get_all_documents(user_id)
    # Add performance timing info if it's a JSON response
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
        result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
    return result

@app.route('/api/files/get-discovered-links', methods=['GET', 'OPTIONS'])
def discovered_links_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    # Get the source_url parameter
    source_url = request.args.get('source_url')
    
    result = get_discovered_links(user_id)
    # Add performance timing info if it's a JSON response
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
        result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
    return result

@app.route('/api/files/scrapped-sub-links', methods=['POST', 'OPTIONS'])
def scrapped_sub_links_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    result = scrapped_sub_links()
    # Add performance timing info if it's a JSON response
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
        result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
    return result

@app.route('/api/files/source-url-status', methods=['GET', 'OPTIONS'])
def source_url_status_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    # Get the source_url parameter
    source_url = request.args.get('source_url')
    
    if not source_url:
        return standardize_error_response('source_url parameter is required.', 'MISSING_PARAM', 400)
    
    result = get_source_url_status(user_id)
    # Add performance timing info if it's a JSON response
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
        result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
    return result

@app.route('/api/files/progress-bar', methods=['GET', 'OPTIONS'])
def progress_bar_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    # Verify user token
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    # Get source URL from query parameters
    source_url = request.args.get('source_url')
    
    if not source_url:
        return standardize_error_response('source_url parameter is required.', 'MISSING_PARAM', 400)
    
    result = get_progress_bar(user_id)
    # Add performance timing info if it's a JSON response
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
        result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
    return result

@app.route('/api/files/get-scrapped-links', methods=['GET', 'OPTIONS'])
def get_scrapped_links_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    result = realtime_scrapped_links(user_id)
    # Add performance timing info if it's a JSON response
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
        result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
    return result

@app.route('/api/files/get-pending-links', methods=['GET', 'OPTIONS'])
def get_pending_links_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    result = realtime_pending_links(user_id)
    # Add performance timing info if it's a JSON response
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
        result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
    return result

@app.route('/api/files/get-total-words-scrapped', methods=['GET', 'OPTIONS'])
def get_total_words_scrapped_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    result = realtime_total_words_scrapped(user_id)
    # Add performance timing info if it's a JSON response
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
        result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
    return result

@app.route('/api/files/get-total-words', methods=['GET', 'OPTIONS'])
def get_total_words_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    result = get_total_words(user_id)
    # Add performance timing info if it's a JSON response
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
        result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
    return result

@app.route('/api/files/vectorization-ready', methods=['GET', 'OPTIONS'])
def vectorization_ready_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    # Return "disabled" response if vectorization is not enabled
    if not ENABLE_VECTORIZATION:
        return jsonify({
            'status': 'disabled',
            'message': 'Vectorization is disabled in this environment',
            'timestamp': datetime.now().isoformat(),
            'process_time': f"{time.time() - start_time:.2f}s"
        })
    
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    result = check_vectorization_ready(user_id)
    # Add performance timing info if it's a JSON response
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
        result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
    return result

@app.route('/api/files/vectorization-status', methods=['GET', 'OPTIONS'])
def vectorization_status_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    # Return "disabled" response if vectorization is not enabled
    if not ENABLE_VECTORIZATION:
        return jsonify({
            'status': 'disabled',
            'message': 'Vectorization is disabled in this environment',
            'timestamp': datetime.now().isoformat(),
            'process_time': f"{time.time() - start_time:.2f}s"
        })
    
    user_id = verify_token()
    if not user_id:
        return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
    
    result = get_vectorization_status(user_id)
    # Add performance timing info if it's a JSON response
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict):
        result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
    return result

# Chatbot Routes
@app.route('/api/chatbot/chat', methods=['POST', 'OPTIONS'])
def chatbot_chat_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    try:
        data = request.json
        message = data.get('message')
        session_id = data.get('session_id')
        api_key = data.get('api_key')
        source_level_url = data.get('source_level_url')
        use_prefetch = False  # FORCE TO FALSE to bypass prefetch, use direct access
        stream = data.get('stream', False)  # Get stream parameter
        
        # Validate required parameters
        if not message:
            return jsonify({
                'status': 'error',
                'message': 'Message is required',
                'process_time': f"{time.time() - start_time:.2f}s",
                'timestamp': datetime.now().isoformat()
            }), 400
        
        # Require source_level_url
        if not source_level_url:
            return jsonify({
                'status': 'error',
                'message': 'source_level_url is required. The chat endpoint only answers questions about specific source documents.',
                'process_time': f"{time.time() - start_time:.2f}s",
                'timestamp': datetime.now().isoformat()
            }), 400
        
        # In VM mode, provide a simple response to simple greetings
        if VM_MODE and len(message.strip()) < 20:
            import re
            greeting_patterns = [
                r'^hi\s*$', r'^hello\s*$', r'^hey\s*$', r'^greetings\s*$', 
                r'^how are you', r'^how\'s it going', r'^how r u', r'^what\'s up',
                r'^good morning', r'^good afternoon', r'^good evening',
                r'^thanks', r'^thank you'
            ]
            is_greeting = any(re.search(pattern, message.lower()) for pattern in greeting_patterns)
            
            if is_greeting:
                greeting_response = "Hello! I'm your AI assistant for answering questions about the content you've provided. What would you like to know about this topic?"
                return jsonify({
                    'status': 'success',
                    'answer': greeting_response,
                    'sources': [],
                    'session_id': session_id or str(int(time.time())),
                    'title': f"Conversation about {source_level_url}",
                    'process_time': f"{time.time() - start_time:.2f}s",
                    'timestamp': datetime.now().isoformat(),
                    'source_level_url': source_level_url
                })
        
        # Extract user_id from the Authorization header
        user_id = verify_token()
        
        # Create a new database connection from the pool for this request
        with get_jsondb_connection() as jsondb_connection:
            # Process the chat request with the source_level_url and user_id
            import asyncio
            
            if stream:
                # Handle streaming response
                async def generate():
                    try:
                        # Process the request with streaming enabled and PREFETCH DISABLED
                        generator = await asyncio.wait_for(
                            process_chat_request(
                                jsondb_connection, 
                                message, 
                                session_id, 
                                api_key, 
                                source_level_url, 
                                user_id,
                                use_prefetch=False,  # Explicitly disable prefetch
                                stream=True
                            ),
                            timeout=30.0  # 30 second timeout
                        )
                        
                        # Format the initial response
                        yield 'data: {"status": "start", "session_id": "' + (session_id or str(int(time.time()))) + '"}\n\n'
                        
                        # Stream the response chunks
                        async for chunk in generator:
                            if chunk:
                                # Escape special characters and format as SSE
                                json_chunk = json.dumps({
                                    "status": "chunk", 
                                    "content": chunk
                                })
                                yield f'data: {json_chunk}\n\n'
                        
                        # End the stream with source info
                        sources_json = json.dumps([])  # No source details in stream mode
                        
                        end_json = json.dumps({
                            "status": "end",
                            "sources": [],
                            "process_time": f"{time.time() - start_time:.2f}s",
                            "source_level_url": source_level_url
                        })
                        yield f'data: {end_json}\n\n'
                        
                    except asyncio.TimeoutError:
                        error_json = json.dumps({
                            "status": "error",
                            "message": "Request timed out. Try a simpler query or try again later.",
                            "process_time": f"{time.time() - start_time:.2f}s"
                        })
                        yield f'data: {error_json}\n\n'
                    except Exception as e:
                        print(f"Error in streaming: {e}")
                        traceback.print_exc()
                        error_json = json.dumps({
                            "status": "error",
                            "message": str(e),
                            "process_time": f"{time.time() - start_time:.2f}s"
                        })
                        yield f'data: {error_json}\n\n'
                
                # Return streaming response
                from flask import Response
                return Response(
                    generate(),
                    mimetype='text/event-stream',
                    headers={
                        'Cache-Control': 'no-cache',
                        'X-Accel-Buffering': 'no',
                        'Connection': 'keep-alive'
                    }
                )
            else:
                # Handle normal response (set prefetch to False)
                # Set a timeout for chat request processing
                response = asyncio.run(asyncio.wait_for(
                    process_chat_request(
                        jsondb_connection, 
                        message, 
                        session_id, 
                        api_key, 
                        source_level_url, 
                        user_id,
                        use_prefetch=False,  # Explicitly disable prefetch
                        stream=False
                    ),
                    timeout=30.0  # 30 second timeout
                ))
                
                # Calculate process time
                process_time = time.time() - start_time
                
                # Prepare the response
                result = {
                    'status': 'success',
                    'answer': response.answer,
                    'sources': response.sources,
                    'session_id': response.session_id,
                    'title': response.title,
                    'process_time': f"{process_time:.2f}s",
                    'timestamp': datetime.now().isoformat(),
                    'source_level_url': source_level_url
                }
                
                return jsonify(result)
                
    except asyncio.TimeoutError:
        return jsonify({
            'status': 'error',
            'message': 'Request timed out. Try a simpler query or try again later.',
            'process_time': f"{time.time() - start_time:.2f}s",
            'timestamp': datetime.now().isoformat()
        }), 504
    except Exception as e:
        print(f"Error in chat endpoint: {e}")
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e),
            'error_details': traceback.format_exc() if not VM_MODE else None,  # Only include traceback in non-VM mode
            'process_time': f"{time.time() - start_time:.2f}s",
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/chatbot/conversations', methods=['GET', 'OPTIONS'])
def chatbot_conversations_direct():
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    try:
        # Extract user_id from query parameters 
        user_id = request.args.get('user_id')
        
        # If not in query params, try to extract from Authorization header
        if not user_id:
            user_id = verify_token()
            if not user_id:
                return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
        
        # Use connection pool with context manager
        with get_jsondb_connection() as jsondb_connection:
            # Get all conversations with improved function, passing user_id
            import asyncio
        # Set a timeout for conversations request
            conversations = asyncio.run(asyncio.wait_for(
                get_all_conversations(jsondb_connection, user_id),
                timeout=15.0  # 15 second timeout
            ))
            
            # Calculate process time
            process_time = time.time() - start_time
            
            return jsonify({
                'status': 'success',
                'conversations': conversations,
                'count': len(conversations),
                'process_time': f"{process_time:.2f}s",
                'timestamp': datetime.now().isoformat()
            })
                
    except asyncio.TimeoutError:
        return jsonify({
            'status': 'error',
            'message': 'Request timed out. Try again later.',
            'process_time': f"{time.time() - start_time:.2f}s",
            'timestamp': datetime.now().isoformat()
        }), 504
    except Exception as e:
        print(f"Error in get_conversations endpoint: {e}")
        traceback.print_exc()
        return standardize_error_response(str(e), 'CONVERSATIONS_ERROR', 500)

@app.route('/api/chatbot/conversation/<session_id>', methods=['GET', 'OPTIONS'])
def chatbot_conversation_direct(session_id):
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    try:
        # Verify the token
        user_id = verify_token()
        if not user_id:
            return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
        
        # Use connection pool
        with get_jsondb_connection() as jsondb_connection:
            # Get the conversation with improved function
            import asyncio
            
            # Set a timeout for session request
            session = asyncio.run(asyncio.wait_for(
                get_session(jsondb_connection, session_id),
                timeout=10.0  # 10 second timeout
            ))
            
            if not session:
                return standardize_error_response('Conversation not found', 'NOT_FOUND', 404)
            
            # Calculate process time
            process_time = time.time() - start_time
            
            return jsonify({
                'status': 'success',
                'session': session,
                'process_time': f"{process_time:.2f}s",
                'timestamp': datetime.now().isoformat()
            })
                
    except asyncio.TimeoutError:
        return jsonify({
            'status': 'error',
            'message': 'Request timed out. Try again later.',
            'process_time': f"{time.time() - start_time:.2f}s",
            'timestamp': datetime.now().isoformat()
        }), 504
    except Exception as e:
        print(f"Error in get_conversation endpoint: {e}")
        traceback.print_exc()
        return standardize_error_response(str(e), 'CONVERSATION_ERROR', 500)

@app.route('/api/chatbot/conversation/<session_id>/title', methods=['PUT', 'OPTIONS'])
def chatbot_update_title_direct(session_id):
    start_time = time.time()
    if request.method == 'OPTIONS':
        return options_route('')
    
    try:
        # Verify the token
        user_id = verify_token()
        if not user_id:
            return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
        
        data = request.json
        new_title = data.get('title')
        
        if not new_title:
            return standardize_error_response('Title is required', 'MISSING_PARAM', 400)
        
        # Use connection pool
        with get_jsondb_connection() as jsondb_connection:
            # Update the title - use asyncio.run for async function
            import asyncio
            
            # Set a timeout for title update
            result = asyncio.run(asyncio.wait_for(
                update_session_title(jsondb_connection, session_id, new_title),
                timeout=10.0  # 10 second timeout
            ))
            
            if not result:
                return standardize_error_response('Failed to update title', 'UPDATE_ERROR', 500)
            
            # Calculate process time
            process_time = time.time() - start_time
            
            return jsonify({
                'status': 'success',
                'message': 'Title updated successfully',
                'session_id': session_id,
                'title': new_title,
                'process_time': f"{process_time:.2f}s",
                'timestamp': datetime.now().isoformat()
            })
                
    except asyncio.TimeoutError:
        return jsonify({
            'status': 'error',
            'message': 'Request timed out. Try again later.',
            'process_time': f"{time.time() - start_time:.2f}s",
            'timestamp': datetime.now().isoformat()
        }), 504
    except Exception as e:
        print(f"Error in update_conversation_title endpoint: {e}")
        traceback.print_exc()
        return standardize_error_response(str(e), 'UPDATE_TITLE_ERROR', 500)

# Disable vectorization endpoint if not enabled
if not ENABLE_VECTORIZATION:
    @app.route('/api/chatbot/vectorize', methods=['POST', 'OPTIONS'])
    def vectorize_disabled():
        if request.method == 'OPTIONS':
            return options_route('')
        return jsonify({
            'status': 'info',
            'message': 'Vectorization is disabled in this environment',
            'timestamp': datetime.now().isoformat()
        })

# Disable prefetch endpoints if not enabled
if not ENABLE_PREFETCH:
    @app.route('/api/prefetch/prefetch', methods=['POST', 'OPTIONS'])
    def prefetch_disabled():
        if request.method == 'OPTIONS':
            return options_route('')
        return jsonify({
            'status': 'info',
            'message': 'Prefetch is disabled in this environment',
            'timestamp': datetime.now().isoformat()
        })
        
    @app.route('/api/prefetch/status', methods=['GET', 'OPTIONS'])
    def prefetch_status_disabled():
        if request.method == 'OPTIONS':
            return options_route('')
        return jsonify({
            'status': 'info',
            'message': 'Prefetch is disabled in this environment',
            'timestamp': datetime.now().isoformat()
        })
else:
    # Add these routes for the prefetch API - only if prefetch is enabled
    @app.route('/api/prefetch/prefetch', methods=['POST', 'OPTIONS'])
    def prefetch_direct():
        start_time = time.time()
        if request.method == 'OPTIONS':
            return options_route('')
        
        user_id = verify_token()
        if not user_id:
            return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
        
        # Forward to the prefetch API
        from prefetch_api import prefetch_context
        result = prefetch_context()
        
        # Add performance timing if not already present
        if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict) and 'process_time' not in result[0]:
            result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
        return result

    @app.route('/api/prefetch/status', methods=['GET', 'OPTIONS'])
    def prefetch_status_direct():
        start_time = time.time()
        if request.method == 'OPTIONS':
            return options_route('')
        
        user_id = verify_token()
        if not user_id:
            return standardize_error_response('Unauthorized access. Valid token required.', 'AUTH_REQUIRED', 401)
        
        # Forward to the prefetch API
        from prefetch_api import get_prefetch_status
        result = get_prefetch_status()
        
        # Add performance timing if not already present
        if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], dict) and 'process_time' not in result[0]:
            result[0]['process_time'] = f"{time.time() - start_time:.2f}s"
        return result

# Register blueprints - but only the ones we need based on environment
app.register_blueprint(file_api, url_prefix='/api/files')
app.register_blueprint(user_api, url_prefix='/api/users')
app.register_blueprint(chatbot_api, url_prefix='/api/chatbot')

# Only register prefetch_api if enabled
if ENABLE_PREFETCH:
    app.register_blueprint(prefetch_api, url_prefix='/api/prefetch')

# Error handlers
@app.errorhandler(404)
def not_found_error(error):
    return standardize_error_response('Resource not found', 'NOT_FOUND', 404)

@app.errorhandler(500)
def internal_error(error):
    return standardize_error_response(error, 'SERVER_ERROR', 500)

# Performance monitoring middleware
@app.before_request
def before_request():
    request.start_time = time.time()

@app.after_request
def after_request(response):
    # Add processing time header if not already present
    if hasattr(request, 'start_time'):
        process_time = time.time() - request.start_time
        response.headers['X-Process-Time'] = f"{process_time:.4f}s"
    return response

# Perform an initial cache clearance and GC
def perform_initial_cleanup():
    """Clear caches and force garbage collection on startup"""
    try:
        clear_all_caches()
        import gc
        gc.collect()
        logger.info("Initial cleanup completed")
    except Exception as e:
        logger.error(f"Error during initial cleanup: {e}")

# Run this at startup
perform_initial_cleanup()

if __name__ == "__main__":
    try:
        port = int(os.getenv("PORT", 5000))
        debug_mode = os.getenv("FLASK_ENV", "production") == "development"
        
        # In VM mode, always disable debug
        if VM_MODE:
            debug_mode = False
            
        logger.info(f"Starting server on port {port}, debug mode: {debug_mode}")
        app.run(host="0.0.0.0", port=port, debug=debug_mode)
    except Exception as e:
        logger.critical(f"Failed to start server: {e}")
        traceback.print_exc()
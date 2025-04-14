from flask import Blueprint, request, jsonify
import os
import json
from datetime import datetime
import traceback
import asyncio
from functools import wraps
from connection import connect_to_jsondb, get_jsondb_connection
from oracle_chatbot import (
    initialize_json_database, 
    process_scrapped_text_to_vector_store,
    process_chat_request,
    get_all_conversations,
    get_session,
    update_session_title,
    verify_api_key
)
import time

# Create the blueprint
chatbot_api = Blueprint('chatbot_api', __name__)

# Middleware for API key verification
def api_key_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.json.get('api_key') if request.is_json else request.args.get('api_key')
        
        if not api_key or not verify_api_key(api_key):
            return jsonify({
                'status': 'error',
                'message': 'Invalid or missing API key',
                'timestamp': datetime.now().isoformat()
            }), 401
            
        return f(*args, **kwargs)
    return decorated

# Initialize the Oracle JSON database on module load
with get_jsondb_connection() as jsondb_connection:
    initialize_json_database(jsondb_connection)
    print("Chatbot API initialized successfully")

@chatbot_api.route('/chat', methods=['POST'])
@api_key_required
def chat():
    """
    Process a chat message and get a response with streaming support
    
    Required Request body:
    - message: The user message
    - source_level_url: The URL context for the conversation
    - session_id: (Optional) The session ID to continue an existing conversation
    - api_key: API key for authentication
    - stream: (Optional) Whether to stream the response
    """
    try:
        start_time = time.time()
        data = request.json
        
        # Validate required parameters
        message = data.get('message')
        source_level_url = data.get('source_level_url')
        
        if not message:
            return jsonify({
                'status': 'error',
                'message': 'Message is required',
                'timestamp': datetime.now().isoformat()
            }), 400
        
        if not source_level_url:
            return jsonify({
                'status': 'error',
                'message': 'source_level_url is required',
                'timestamp': datetime.now().isoformat()
            }), 400
        
        session_id = data.get('session_id')
        api_key = data.get('api_key')
        # Always set use_prefetch to False to bypass prefetch system
        use_prefetch = False  
        stream = data.get('stream', False)  # Get stream parameter
        
        # Extract user_id from the Authorization header
        user_id = None
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            try:
                from app import SECRET_KEY
                import jwt
                decoded = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
                user_id = decoded.get('user_id')
            except Exception as jwt_error:
                print(f"Could not extract user_id from token: {str(jwt_error)}")
        
        # Create a new database connection using the pool
        with get_jsondb_connection() as db_conn:
            if stream:
                # Handle streaming response
                async def generate():
                    try:
                        # Process the request with streaming enabled and prefetch disabled
                        generator = await asyncio.wait_for(
                            process_chat_request(
                                db_conn, 
                                message, 
                                session_id, 
                                api_key, 
                                source_level_url, 
                                user_id,
                                use_prefetch=False,  # Explicitly set to False
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
                        
                        # End the stream
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
                # Handle normal response with prefetch disabled
                try:
                    # Set a timeout to prevent long-running requests
                    response = asyncio.run(asyncio.wait_for(
                        process_chat_request(
                            db_conn, 
                            message, 
                            session_id, 
                            api_key, 
                            source_level_url,
                            user_id,
                            use_prefetch=False,  # Explicitly set to False
                            stream=False
                        ),
                        timeout=30.0  # 30 seconds timeout
                    ))
                    
                    # Calculate processing time
                    process_time = time.time() - start_time
                    
                    # Prepare the response
                    result = {
                        'status': 'success',
                        'answer': response.answer,
                        'sources': response.sources,
                        'session_id': response.session_id,
                        'title': response.title,
                        'timestamp': datetime.now().isoformat(),
                        'source_level_url': source_level_url,
                        'process_time': f"{process_time:.2f}s"
                    }
                    
                    return jsonify(result)
                    
                except asyncio.TimeoutError:
                    return jsonify({
                        'status': 'error',
                        'message': 'Request timed out. Try a simpler query or try again later.',
                        'timestamp': datetime.now().isoformat(),
                        'process_time': f"{time.time() - start_time:.2f}s"
                    }), 504
    
    except Exception as e:
        print(f"Error in chat endpoint: {e}")
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat(),
            'process_time': f"{time.time() - start_time:.2f}s"
        }), 500

@chatbot_api.route('/vectorize', methods=['POST'])
def vectorize_scrapped_text():
    """
    Process scrapped text data into the vector store with optimized performance
    
    Request body:
    - user_id: (Optional) Filter by user_id
    - source_level_url: URL to filter documents by and associate with vectorized data
    - api_key: API key for authentication
    """
    try:
        start_time = time.time()
        data = request.json
        user_id = data.get('user_id')
        source_level_url = data.get('source_level_url')
        
        if not source_level_url:
            return jsonify({
                'status': 'error',
                'message': 'source_level_url is required in the request body',
                'timestamp': datetime.now().isoformat()
            }), 400
        
        # Create a new database connection using the pool
        with get_jsondb_connection() as db_conn:
            # Process the scrapped text data filtered by source_level_url
            result = process_scrapped_text_to_vector_store(db_conn, user_id, source_level_url)
            
            # Add timing information
            process_time = time.time() - start_time
            result['timestamp'] = datetime.now().isoformat()
            result['source_level_url'] = source_level_url
            result['process_time'] = f"{process_time:.2f}s"
            
            return jsonify(result)
    
    except Exception as e:
        print(f"Error in vectorize endpoint: {e}")
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat(),
            'process_time': f"{time.time() - start_time:.2f}s"
        }), 500

@chatbot_api.route('/conversations', methods=['GET'])
def get_conversations_route():
    """
    Get chat conversations for a specific source URL with enhanced debugging
    """
    try:
        start_time = time.time()
        
        # Extract source_level_url for filtering (REQUIRED)
        source_level_url = request.args.get('source_level_url')
        user_id = request.args.get('user_id')
        
        # Enhanced logging
        print("=" * 50)
        print(f"Conversation Request Details:")
        print(f"Source URL: {source_level_url}")
        print(f"User ID: {user_id}")
        print(f"Full Request Args: {request.args}")
        print("=" * 50)
        
        if not source_level_url:
            return jsonify({
                'status': 'error',
                'message': 'source_level_url is required',
                'timestamp': datetime.now().isoformat(),
                'process_time': f"{time.time() - start_time:.2f}s"
            }), 400
        
        # If not in query params, extract from Authorization header
        if not user_id:
            auth_header = request.headers.get('Authorization')
            if auth_header and auth_header.startswith("Bearer "):
                token = auth_header.split(" ")[1]
                try:
                    from app import SECRET_KEY
                    import jwt
                    decoded = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
                    user_id = decoded.get('user_id')
                    print(f"Extracted user_id {user_id} from JWT token")
                except Exception as jwt_error:
                    print(f"Could not extract user_id from token: {str(jwt_error)}")
                    return jsonify({
                        'status': 'error',
                        'message': 'Unable to authenticate user from token',
                        'timestamp': datetime.now().isoformat(),
                        'process_time': f"{time.time() - start_time:.2f}s"
                    }), 401
            else:
                return jsonify({
                    'status': 'error',
                    'message': 'Authentication required',
                    'timestamp': datetime.now().isoformat(),
                    'process_time': f"{time.time() - start_time:.2f}s"
                }), 401
        
        if not user_id:
            return jsonify({
                'status': 'error',
                'message': 'User ID is required',
                'timestamp': datetime.now().isoformat(),
                'process_time': f"{time.time() - start_time:.2f}s"
            }), 400
            
        print(f"Getting conversations for user_id: {user_id}, source_url: {source_level_url}")
        
        # Use connection pool with context manager
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Manually query using the correct column names in the database
                query = """
                    SELECT ID, JSON_DATA 
                    FROM CHAT_SESSIONS 
                    WHERE USER_ID = :user_id AND SOURCE_URL = :source_url
                    ORDER BY UPDATED_AT DESC
                """
                
                cursor.execute(query, user_id=user_id, source_url=source_level_url)
                rows = cursor.fetchall()
                
                print(f"Found {len(rows)} conversations for user {user_id} with source_url {source_level_url}")
                
                conversations = []
                for row_id, data in rows:
                    try:
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
                        
                        # Extract session info
                        session_id = session.get("session_id", f"unknown-{row_id}")
                        title = session.get("title", "Untitled Conversation")
                        
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
                        
                        # Create summary for this conversation
                        conversation = {
                            "session_id": session_id,
                            "title": title,
                            "last_message": last_message,
                            "last_updated": last_updated,
                            "message_count": len(messages),
                            "user_id": user_id,
                            "source_url": source_level_url
                        }
                        
                        conversations.append(conversation)
                        
                    except Exception as row_error:
                        print(f"Error processing row {row_id}: {str(row_error)}")
                        # Continue to next row
            
            # Calculate process time
            process_time = time.time() - start_time
            
            return jsonify({
                'status': 'success',
                'conversations': conversations,
                'count': len(conversations),
                'user_id': user_id,
                'source_level_url': source_level_url,
                'process_time': f"{process_time:.2f}s",
                'timestamp': datetime.now().isoformat()
            })
                
    except Exception as e:
        print(f"Error in get_conversations endpoint: {e}")
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e),
            'source_level_url': source_level_url,
            'timestamp': datetime.now().isoformat(),
            'process_time': f"{time.time() - start_time:.2f}s"
        }), 500


    
@chatbot_api.route('/conversation/<session_id>', methods=['GET'])
@api_key_required
def get_conversation(session_id):
    """
    Get a specific chat conversation with improved filtering by source URL and user ID
    
    Path parameters:
    - session_id: The session ID
    
    Query parameters:
    - api_key: API key for authentication
    - source_url: Optional source URL to verify authorization
    """
    try:
        start_time = time.time()
        print(f"Getting conversation for session ID: {session_id}")
        
        # Verify the token to get user_id
        user_id = None
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            try:
                from app import SECRET_KEY
                import jwt
                decoded = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
                user_id = decoded.get('user_id')
                print(f"Authenticated request from user_id: {user_id}")
            except Exception as jwt_error:
                print(f"Could not extract user_id from token: {str(jwt_error)}")
        
        # Get optional source_url from query params for additional filtering
        source_url = request.args.get('source_url')
        
        # Use connection pool with context manager
        with get_jsondb_connection() as db_conn:
            cursor = db_conn.cursor()
            
            # Build query based on available parameters
            query_params = {'session_id': session_id}
            
            # Start with base query by session ID
            base_query = """
                SELECT JSON_DATA, USER_ID, SOURCE_URL, ID
                FROM CHAT_SESSIONS 
                WHERE JSON_VALUE(JSON_DATA, '$.session_id') = :session_id
            """
            
            # Add user_id filter if provided
            if user_id:
                base_query += " AND (JSON_VALUE(JSON_DATA, '$.user_id') = :user_id OR USER_ID = :user_id)"
                query_params['user_id'] = user_id
            
            # Add source_url filter if provided
            if source_url:
                base_query += " AND (JSON_VALUE(JSON_DATA, '$.source_url') = :source_url OR SOURCE_URL = :source_url)"
                query_params['source_url'] = source_url
                
            # Execute the query with all filters
            cursor.execute(base_query, query_params)
            result = cursor.fetchone()
            
            if not result:
                print(f"Session {session_id} not found with primary query")
                
                # Try fallback with LIKE query if primary search fails
                fallback_query = """
                    SELECT JSON_DATA, USER_ID, SOURCE_URL, ID
                    FROM CHAT_SESSIONS
                    WHERE JSON_DATA LIKE '%"session_id":"' || :session_id || '"%'
                       OR JSON_DATA LIKE '%"session_id": "' || :session_id || '"%'
                """
                
                # Add user_id filter if provided
                if user_id:
                    fallback_query += " AND (USER_ID = :user_id OR JSON_DATA LIKE '%\"user_id\":' || :user_id || '%')"
                
                # Add source_url filter if provided
                if source_url:
                    fallback_query += " AND (SOURCE_URL = :source_url OR JSON_DATA LIKE '%\"source_url\":\"' || :source_url || '\"%')"
                
                cursor.execute(fallback_query, query_params)
                result = cursor.fetchone()
                
            if not result:
                return jsonify({
                    'status': 'error',
                    'message': 'Conversation not found',
                    'timestamp': datetime.now().isoformat(),
                    'process_time': f"{time.time() - start_time:.2f}s"
                }), 404
            
            # Session found - extract data
            session_data, db_user_id, db_source_url, row_id = result
            
            # Verify user has access to this session
            if user_id and db_user_id and str(user_id) != str(db_user_id):
                print(f"Access denied: Request user {user_id} doesn't match session user {db_user_id}")
                return jsonify({
                    'status': 'error',
                    'message': 'Access denied to this conversation',
                    'timestamp': datetime.now().isoformat(),
                    'process_time': f"{time.time() - start_time:.2f}s"
                }), 403
            
            # Parse JSON data
            if hasattr(session_data, 'read'):
                session_data = session_data.read()
                
            if isinstance(session_data, str):
                session = json.loads(session_data)
            elif isinstance(session_data, bytes):
                session = json.loads(session_data.decode('utf-8'))
            else:
                session = session_data
            
            # For debugging, count messages
            message_count = len(session.get('messages', []))
            print(f"Retrieved session with {message_count} messages")
            
            # Check if source_url is missing but can be extracted from title
            if not session.get("source_url") and not db_source_url:
                title = session.get('title', '')
                if title.startswith('Conversation about '):
                    source_url = title[19:]
                    session['source_url'] = source_url
                    
                    # Update the database with the extracted source_url
                    try:
                        patch = json.dumps({"source_url": source_url})
                        cursor.execute("""
                            UPDATE CHAT_SESSIONS 
                            SET JSON_DATA = JSON_MERGEPATCH(JSON_DATA, :patch),
                                SOURCE_URL = :source_url
                            WHERE ID = :id
                        """, patch=patch, source_url=source_url, id=row_id)
                        db_conn.commit()
                        print(f"Updated source_url for session {session_id} with {source_url}")
                    except Exception as update_error:
                        print(f"Error updating source_url: {update_error}")
            
            # Add source_url for consistency with our new model
            source_url = session.get('source_url', db_source_url) or 'Unknown source'
            
            # Calculate process time
            process_time = time.time() - start_time
            
            return jsonify({
                'status': 'success',
                'session': session,
                'message_count': message_count,
                'source_url': source_url,
                'process_time': f"{process_time:.2f}s",
                'timestamp': datetime.now().isoformat()
            })
            
    except Exception as e:
        print(f"Error in get_conversation endpoint: {e}")
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat(),
            'process_time': f"{time.time() - start_time:.2f}s"
        }), 500


@chatbot_api.route('/conversation/<session_id>/title', methods=['PUT'])
def update_conversation_title(session_id):
    """
    Update the title of a chat conversation with improved database handling
    
    Path parameters:
    - session_id: The session ID
    
    Request body:
    - title: The new title
    """
    try:
        start_time = time.time()
        data = request.json
        new_title = data.get('title')
        
        if not new_title:
            return jsonify({
                'status': 'error',
                'message': 'Title is required',
                'timestamp': datetime.now().isoformat()
            }), 400
        
        # Get user_id from token if available
        user_id = None
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            try:
                from app import SECRET_KEY
                import jwt
                decoded = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
                user_id = decoded.get('user_id')
            except Exception as jwt_error:
                print(f"Token verification error: {jwt_error}")
                # Continue without user_id
        
        # Create a new database connection using the pool
        with get_jsondb_connection() as connection:
            # Run the async function to update the title
            import asyncio
            result = asyncio.run(update_session_title(connection, session_id, new_title))
            
            if result:
                return jsonify({
                    'status': 'success',
                    'message': 'Title updated successfully',
                    'session_id': session_id,
                    'title': new_title,
                    'process_time': f"{time.time() - start_time:.2f}s",
                    'timestamp': datetime.now().isoformat()
                })
            else:
                return jsonify({
                    'status': 'error',
                    'message': 'Failed to update title',
                    'timestamp': datetime.now().isoformat(),
                    'process_time': f"{time.time() - start_time:.2f}s"
                }), 500
    
    except Exception as e:
        print(f"Error in update_conversation_title endpoint: {e}")
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat(),
            'process_time': f"{time.time() - start_time:.2f}s"
        }), 500


@chatbot_api.route('/sessions-by-url', methods=['GET'])
def get_sessions_by_url():
    """
    Get chat conversations filtered by source URL and user ID with robust error handling
    
    Query parameters:
    - source_url: URL to filter conversations by
    - user_id: User ID to filter conversations by
    """
    try:
        start_time = time.time()
        
        # Get query parameters
        source_url = request.args.get('source_url')
        user_id = request.args.get('user_id')
        
        print(f"Getting sessions by URL - Source URL: {source_url}, User ID: {user_id}")
        
        # Validate parameters
        if not source_url:
            return jsonify({
                'status': 'error',
                'message': 'source_url parameter is required',
                'timestamp': datetime.now().isoformat(),
                'process_time': f"{time.time() - start_time:.2f}s"
            }), 400
        
        # Build the SQL query
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Print table structure for debugging
                try:
                    cursor.execute("SELECT COLUMN_NAME, DATA_TYPE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = 'CHAT_SESSIONS'")
                    columns = cursor.fetchall()
                    print("Table structure:")
                    for col in columns:
                        print(f"Column: {col[0]}, Type: {col[1]}")
                except Exception as schema_error:
                    print(f"Error getting schema: {schema_error}")
                
                # First try an exact match
                base_query = """
                    SELECT ID, JSON_DATA, SESSION_ID, SOURCE_URL, USER_ID 
                    FROM CHAT_SESSIONS 
                    WHERE SOURCE_URL = :source_url
                """
                
                params = {'source_url': source_url}
                
                # Add user filter if provided
                if user_id:
                    base_query += " AND USER_ID = :user_id"
                    params['user_id'] = user_id
                    
                base_query += " ORDER BY UPDATED_AT DESC"
                
                # Execute the query
                print(f"Executing query: {base_query}")
                print(f"With parameters: {params}")
                
                cursor.execute(base_query, params)
                exact_rows = cursor.fetchall()
                print(f"Found {len(exact_rows)} rows with exact match")
                
                # If no results, try a more flexible approach
                if not exact_rows:
                    print("No exact matches, trying flexible matching")
                    flexible_query = """
                        SELECT ID, JSON_DATA, SESSION_ID, SOURCE_URL, USER_ID 
                        FROM CHAT_SESSIONS 
                        WHERE 
                            (SOURCE_URL = :source_url OR 
                             SOURCE_URL LIKE :source_url_like OR
                             JSON_DATA LIKE :json_pattern)
                    """
                    
                    flexible_params = {
                        'source_url': source_url,
                        'source_url_like': f"%{source_url}%",
                        'json_pattern': f"%{source_url}%"
                    }
                    
                    # Add user filter if provided
                    if user_id:
                        flexible_query += " AND USER_ID = :user_id"
                        flexible_params['user_id'] = user_id
                        
                    flexible_query += " ORDER BY UPDATED_AT DESC"
                    
                    # Execute the flexible query
                    print(f"Executing flexible query: {flexible_query}")
                    print(f"With parameters: {flexible_params}")
                    
                    cursor.execute(flexible_query, flexible_params)
                    flexible_rows = cursor.fetchall()
                    print(f"Found {len(flexible_rows)} rows with flexible match")
                    
                    rows = flexible_rows
                else:
                    rows = exact_rows
                
                # If still no rows, return all sessions for the user if user_id was provided
                if not rows and user_id:
                    print(f"No matches found, retrieving all sessions for user {user_id}")
                    user_query = """
                        SELECT ID, JSON_DATA, SESSION_ID, SOURCE_URL, USER_ID 
                        FROM CHAT_SESSIONS 
                        WHERE USER_ID = :user_id
                        ORDER BY UPDATED_AT DESC
                    """
                    
                    cursor.execute(user_query, {'user_id': user_id})
                    user_rows = cursor.fetchall()
                    print(f"Found {len(user_rows)} rows for user {user_id}")
                    
                    rows = user_rows
                
                # Process the rows into conversation objects
                conversations = []
                for row in rows:
                    try:
                        row_id, json_data, session_id, row_source_url, row_user_id = row
                        
                        # Handle Oracle LOB objects
                        if hasattr(json_data, 'read'):
                            json_data = json_data.read()
                        
                        # Parse JSON data
                        session_data = None
                        if isinstance(json_data, str):
                            try:
                                session_data = json.loads(json_data)
                            except json.JSONDecodeError:
                                print(f"Error parsing JSON for row {row_id}")
                        elif isinstance(json_data, bytes):
                            try:
                                session_data = json.loads(json_data.decode('utf-8'))
                            except:
                                print(f"Error decoding bytes to string for row {row_id}")
                        else:
                            session_data = json_data
                        
                        if not session_data:
                            print(f"Skipping row {row_id} - could not parse JSON data")
                            continue
                        
                        # Extract conversation details
                        session_title = session_data.get('title', 'Untitled Conversation')
                        messages = session_data.get('messages', [])
                        
                        # Get the last message for preview
                        last_message = ""
                        if messages:
                            for msg in reversed(messages):
                                if msg.get('role') == 'assistant' and 'content' in msg:
                                    last_message = msg.get('content', '')[:100] + "..."
                                    break
                                    
                            if not last_message and 'content' in messages[-1]:
                                last_message = messages[-1].get('content', '')[:100] + "..."
                        
                        # Create the conversation object
                        conversation = {
                            'session_id': session_data.get('session_id', session_id),
                            'title': session_title,
                            'last_message': last_message,
                            'last_updated': session_data.get('last_updated', session_data.get('created_at')),
                            'message_count': len(messages),
                            'user_id': row_user_id,
                            'source_url': row_source_url or source_url
                        }
                        
                        conversations.append(conversation)
                        
                    except Exception as row_error:
                        print(f"Error processing row {row[0] if row else 'unknown'}: {row_error}")
                        traceback.print_exc()
                        # Continue to next row
                
                # Calculate process time
                process_time = time.time() - start_time
                
                return jsonify({
                    'status': 'success',
                    'conversations': conversations,
                    'count': len(conversations),
                    'source_url': source_url,
                    'user_id': user_id,
                    'process_time': f"{process_time:.2f}s",
                    'timestamp': datetime.now().isoformat()
                })
    
    except Exception as e:
        print(f"Error in get_sessions_by_url: {e}")
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat(),
            'process_time': f"{time.time() - start_time:.2f}s"
        }), 500
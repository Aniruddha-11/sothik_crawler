from flask import Blueprint, request, jsonify
from functools import wraps
import jwt
from datetime import datetime, timedelta
import traceback
from connection import get_jsondb_connection, connect_to_jsondb, execute_query
from dotenv import load_dotenv
import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import json
import validators
from threading import Lock, Thread, Event
from contextlib import contextmanager
from threading import Thread, Event
import asyncio
import concurrent.futures
import hashlib


# Load environment variables from .env file
load_dotenv()

file_api = Blueprint('file_api', __name__)

# Global variables to track processing state
processing_events = {}
crawling_events = {}
active_user_jobs = {}  # Track which users have active jobs
vectorization_events = {}  # Track vectorization stop events
vectorization_threads = {}  # Track vectorization threads

# Consolidated vectorization tracking system
vectorization_manager = {
    'events': {},  # Maps user_id:source_url to Event objects
    'threads': {},  # Maps user_id:source_url to Thread objects
    'status': {}   # Maps user_id:source_url to status information
}


# Oracle Database connection configuration
SECRET_KEY = os.environ.get('SECRET_KEY')

# Table names for Oracle
CONTENT_LINKS_TABLE = 'CONTENT_LINKS'
LINKS_TO_SCRAP_TABLE = 'LINKS_TO_SCRAP'
SCRAPPED_TEXT_TABLE = 'SCRAPPED_TEXT'
PROCESSING_QUEUE_TABLE = 'PROCESSING_QUEUE'
SOURCE_URLS_TABLE = 'SOURCE_URLS'
PROGRESS_HISTORY_TABLE = 'PROGRESS_HISTORY'
VECTORIZED_DOCUMENTS_TABLE = 'VECTORIZED_DOCUMENTS'
# Global variables to track processing state
processing_events = {}
crawling_events = {}
active_user_jobs = {}  # Track which users have active jobs
# Add near other global variables in file_api.py
vectorization_events = {}
vectorization_threads = {}

def update_links_limit_to_page_limit():
    """Reverse migration: Update LINKS_LIMIT columns back to PAGE_LIMIT in existing tables"""
    try:
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Check if PROCESSING_QUEUE has LINKS_LIMIT
                cursor.execute("""
                    SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                    WHERE TABLE_NAME = 'PROCESSING_QUEUE' AND COLUMN_NAME = 'LINKS_LIMIT'
                """)
                
                has_links_limit = cursor.fetchone()[0] > 0
                
                cursor.execute("""
                    SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                    WHERE TABLE_NAME = 'PROCESSING_QUEUE' AND COLUMN_NAME = 'PAGE_LIMIT'
                """)
                
                has_page_limit = cursor.fetchone()[0] > 0
                
                if has_links_limit:
                    if not has_page_limit:
                        # Add PAGE_LIMIT column if it doesn't exist
                        cursor.execute("""
                            ALTER TABLE PROCESSING_QUEUE 
                            ADD PAGE_LIMIT NUMBER DEFAULT 10
                        """)
                    
                    # Copy values from LINKS_LIMIT to PAGE_LIMIT
                    cursor.execute("""
                        UPDATE PROCESSING_QUEUE
                        SET PAGE_LIMIT = LINKS_LIMIT
                    """)
                    
                    # If we want to drop the LINKS_LIMIT column, uncomment this line:
                    # cursor.execute("ALTER TABLE PROCESSING_QUEUE DROP COLUMN LINKS_LIMIT")
                    
                    print("Migrated PROCESSING_QUEUE.LINKS_LIMIT back to PAGE_LIMIT")
                
                # Repeat for SOURCE_URLS
                cursor.execute("""
                    SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                    WHERE TABLE_NAME = 'SOURCE_URLS' AND COLUMN_NAME = 'LINKS_LIMIT'
                """)
                
                has_links_limit = cursor.fetchone()[0] > 0
                
                cursor.execute("""
                    SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                    WHERE TABLE_NAME = 'SOURCE_URLS' AND COLUMN_NAME = 'PAGE_LIMIT'
                """)
                
                has_page_limit = cursor.fetchone()[0] > 0
                
                if has_links_limit:
                    if not has_page_limit:
                        # Add PAGE_LIMIT column if it doesn't exist
                        cursor.execute("""
                            ALTER TABLE SOURCE_URLS 
                            ADD PAGE_LIMIT NUMBER DEFAULT 10
                        """)
                    
                    # Copy values from LINKS_LIMIT to PAGE_LIMIT
                    cursor.execute("""
                        UPDATE SOURCE_URLS
                        SET PAGE_LIMIT = LINKS_LIMIT
                    """)
                    
                    # If we want to drop the LINKS_LIMIT column, uncomment this line:
                    # cursor.execute("ALTER TABLE SOURCE_URLS DROP COLUMN LINKS_LIMIT")
                    
                    print("Migrated SOURCE_URLS.LINKS_LIMIT back to PAGE_LIMIT")
                    
                connection.commit()
                return True
    except Exception as e:
        print(f"Error during links_limit to page_limit migration: {str(e)}")
        traceback.print_exc()
        return False

def initialize_tables():
    """Initialize all necessary tables if they don't exist with optimized indexes and AUTO_VECTORIZE column"""
    try:
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Map of table names to their creation SQL (using PAGE_LIMIT instead of LINKS_LIMIT)
                tables = {
                    CONTENT_LINKS_TABLE: """
                        CREATE TABLE {0} (
                            ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            SOURCE_URL VARCHAR2(2000) NOT NULL,
                            UNIQUE_LINKS CLOB CHECK (UNIQUE_LINKS IS JSON),
                            CRAWLED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            DEPTH NUMBER DEFAULT 0,
                            TOP_LEVEL_SOURCE VARCHAR2(2000),
                            USER_ID VARCHAR2(50)
                        )
                    """,
                    
                    LINKS_TO_SCRAP_TABLE: """
                        CREATE TABLE {0} (
                            ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            LINK VARCHAR2(2000) NOT NULL,
                            ADDED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            IS_CRAWLED NUMBER(1) DEFAULT 0,
                            IS_PROCESSED VARCHAR2(20) DEFAULT 'false',
                            SOURCE_URL VARCHAR2(2000),
                            TOP_LEVEL_SOURCE VARCHAR2(2000),
                            DEPTH NUMBER DEFAULT 0,
                            PROCESSED_AT TIMESTAMP,
                            HAS_TEXT_IN_URL NUMBER(1) DEFAULT 0,
                            USER_ID VARCHAR2(50),
                            CRAWLING_STARTED TIMESTAMP,
                            CRAWLED_AT TIMESTAMP,
                            LINKS_FOUND NUMBER,
                            LINKS_ADDED NUMBER,
                            ERROR CLOB,
                            TRACEBACK CLOB,
                            SKIPPED NUMBER(1) DEFAULT 0,
                            SKIP_REASON VARCHAR2(100),
                            CONSTRAINT UQ_LINK_USER UNIQUE (LINK, USER_ID)
                        )
                    """,
                    
                    SCRAPPED_TEXT_TABLE: """
                        CREATE TABLE {0} (
                            ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            SCRAPPED_CONTENT CLOB,
                            CONTENT_LINK VARCHAR2(2000) NOT NULL,
                            SCRAPE_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            LINK_ID NUMBER,
                            SOURCE_URL VARCHAR2(2000),
                            TOP_LEVEL_SOURCE VARCHAR2(2000),
                            DEPTH NUMBER DEFAULT 0,
                            TITLE VARCHAR2(1000),
                            USER_ID VARCHAR2(50),
                            WORD_COUNT NUMBER DEFAULT 0,
                            CONSTRAINT UQ_CONTENT_LINK_USER UNIQUE (CONTENT_LINK, USER_ID)
                        )
                    """,
                    
                    PROCESSING_QUEUE_TABLE: """
                        CREATE TABLE {0} (
                            ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            USER_ID VARCHAR2(50) NOT NULL,
                            SOURCE_URL VARCHAR2(2000) NOT NULL,
                            ADDED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            PROCESSED NUMBER(1) DEFAULT 0,
                            PROCESSING_STARTED TIMESTAMP,
                            PROCESSING_COMPLETED TIMESTAMP,
                            PAGE_LIMIT NUMBER DEFAULT 10,
                            AUTO_VECTORIZE NUMBER(1) DEFAULT 1,
                            CONSTRAINT UQ_USER_SOURCE UNIQUE (USER_ID, SOURCE_URL)
                        )
                    """,
                    
                    SOURCE_URLS_TABLE: """
                        CREATE TABLE {0} (
                            ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            SOURCE_URL VARCHAR2(2000) NOT NULL,
                            USER_ID VARCHAR2(50),
                            TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            PAGE_LIMIT NUMBER DEFAULT 10,
                            AUTO_VECTORIZE NUMBER(1) DEFAULT 1
                        )
                    """,
                    PROGRESS_HISTORY_TABLE: """
                        CREATE TABLE {0} (
                            ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            SOURCE_URL VARCHAR2(2000) NOT NULL,
                            TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            CRAWL_PROGRESS NUMBER(5,2),
                            SCRAPE_PROGRESS NUMBER(5,2),
                            CRAWLED_COUNT NUMBER,
                            SCRAPED_COUNT NUMBER,
                            TOTAL_LINKS NUMBER,
                            OPERATION_STATUS VARCHAR2(50)
                        )
                      """,
                    VECTORIZED_DOCUMENTS_TABLE: """
                            CREATE TABLE {0} (
                                ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                DOCUMENT_ID NUMBER NOT NULL,
                                SOURCE_URL VARCHAR2(2000) NOT NULL,
                                USER_ID VARCHAR2(50),
                                VECTORIZED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                CHUNK_COUNT NUMBER DEFAULT 0,
                                CONSTRAINT UQ_DOC_SOURCE_USER UNIQUE (DOCUMENT_ID, SOURCE_URL, USER_ID)
                            )
                        """,

                }
                
                # Check and create each table if it doesn't exist
                for table_name, create_sql in tables.items():
                    cursor.execute(f"""
                        SELECT COUNT(*) 
                        FROM USER_TABLES
                        WHERE TABLE_NAME = '{table_name}'
                    """)
                    
                    if cursor.fetchone()[0] == 0:
                        cursor.execute(create_sql.format(table_name))
                        print(f"Created table {table_name}")
                        
                        # Create necessary indexes with improved structure
                        if table_name == LINKS_TO_SCRAP_TABLE:
                            # Create optimized index on common search fields
                            cursor.execute(f"""
                                CREATE INDEX IDX_{table_name}_TOP_LEVEL 
                                ON {table_name} (TOP_LEVEL_SOURCE, USER_ID, IS_CRAWLED)
                            """)
                            cursor.execute(f"""
                                CREATE INDEX IDX_{table_name}_PROCESSING 
                                ON {table_name} (TOP_LEVEL_SOURCE, USER_ID, IS_PROCESSED)
                            """)
                            cursor.execute(f"""
                                CREATE INDEX IDX_{table_name}_LINK_USER
                                ON {table_name} (LINK, USER_ID)
                            """)
                            # New optimized indexes
                            cursor.execute(f"""
                                CREATE INDEX IDX_{table_name}_CRAWL_QUEUE 
                                ON {table_name} (IS_CRAWLED, TOP_LEVEL_SOURCE, USER_ID, DEPTH, ADDED_AT)
                            """)
                            cursor.execute(f"""
                                CREATE INDEX IDX_{table_name}_PROCESS_QUEUE 
                                ON {table_name} (IS_PROCESSED, TOP_LEVEL_SOURCE, USER_ID)
                            """)
                        elif table_name == PROCESSING_QUEUE_TABLE:
                            # Create optimized index for queue processing
                            cursor.execute(f"""
                                CREATE INDEX IDX_{table_name}_QUEUE 
                                ON {table_name} (USER_ID, PROCESSED, PROCESSING_STARTED, ADDED_AT)
                            """)
                        elif table_name == SCRAPPED_TEXT_TABLE:
                            # Add index for content lookups
                            cursor.execute(f"""
                                CREATE INDEX IDX_{table_name}_SOURCE 
                                ON {table_name} (TOP_LEVEL_SOURCE, USER_ID)
                            """)
                            # Add index for word count
                            cursor.execute(f"""
                                CREATE INDEX IDX_{table_name}_WORD_COUNT 
                                ON {table_name} (WORD_COUNT)
                            """)
                    else:
                        # If SCRAPPED_TEXT table already exists, check for WORD_COUNT column
                        if table_name == SCRAPPED_TEXT_TABLE:
                            cursor.execute("""
                                SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                                WHERE TABLE_NAME = 'SCRAPPED_TEXT' AND COLUMN_NAME = 'WORD_COUNT'
                            """)
                            
                            column_exists = cursor.fetchone()[0] > 0
                            
                            if not column_exists:
                                # Add the WORD_COUNT column to an existing table
                                cursor.execute("""
                                    ALTER TABLE SCRAPPED_TEXT 
                                    ADD WORD_COUNT NUMBER DEFAULT 0
                                """)
                                
                                # Create index for the new column
                                cursor.execute(f"""
                                    CREATE INDEX IDX_{table_name}_WORD_COUNT 
                                    ON {table_name} (WORD_COUNT)
                                """)
                                
                                print(f"Added WORD_COUNT column to existing SCRAPPED_TEXT table")
                        
                        # Check for SKIPPED and SKIP_REASON columns in LINKS_TO_SCRAP table
                        if table_name == LINKS_TO_SCRAP_TABLE:
                            cursor.execute("""
                                SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                                WHERE TABLE_NAME = 'LINKS_TO_SCRAP' AND COLUMN_NAME = 'SKIPPED'
                            """)
                            
                            column_exists = cursor.fetchone()[0] > 0
                            
                            if not column_exists:
                                # Add the SKIPPED column to an existing table
                                cursor.execute("""
                                    ALTER TABLE LINKS_TO_SCRAP 
                                    ADD SKIPPED NUMBER(1) DEFAULT 0
                                """)
                                print(f"Added SKIPPED column to existing LINKS_TO_SCRAP table")
                                
                            cursor.execute("""
                                SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                                WHERE TABLE_NAME = 'LINKS_TO_SCRAP' AND COLUMN_NAME = 'SKIP_REASON'
                            """)
                            
                            column_exists = cursor.fetchone()[0] > 0
                            
                            if not column_exists:
                                # Add the SKIP_REASON column to an existing table
                                cursor.execute("""
                                    ALTER TABLE LINKS_TO_SCRAP 
                                    ADD SKIP_REASON VARCHAR2(100)
                                """)
                                print(f"Added SKIP_REASON column to existing LINKS_TO_SCRAP table")
                                
                                # Create index for the SKIPPED column
                                cursor.execute(f"""
                                    CREATE INDEX IDX_{table_name}_SKIPPED 
                                    ON {table_name} (SKIPPED, TOP_LEVEL_SOURCE, USER_ID)
                                """)
                        
                        # Check for AUTO_VECTORIZE column in PROCESSING_QUEUE_TABLE
                        if table_name == PROCESSING_QUEUE_TABLE:
                            cursor.execute("""
                                SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                                WHERE TABLE_NAME = 'PROCESSING_QUEUE' AND COLUMN_NAME = 'AUTO_VECTORIZE'
                            """)
                            
                            column_exists = cursor.fetchone()[0] > 0
                            
                            if not column_exists:
                                # Add the AUTO_VECTORIZE column to an existing table
                                cursor.execute("""
                                    ALTER TABLE PROCESSING_QUEUE 
                                    ADD AUTO_VECTORIZE NUMBER(1) DEFAULT 1
                                """)
                                
                                print(f"Added AUTO_VECTORIZE column to existing PROCESSING_QUEUE table")
                        
                        # Check for AUTO_VECTORIZE column in SOURCE_URLS_TABLE
                        if table_name == SOURCE_URLS_TABLE:
                            cursor.execute("""
                                SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                                WHERE TABLE_NAME = 'SOURCE_URLS' AND COLUMN_NAME = 'AUTO_VECTORIZE'
                            """)
                            
                            column_exists = cursor.fetchone()[0] > 0
                            
                            if not column_exists:
                                # Add the AUTO_VECTORIZE column to an existing table
                                cursor.execute("""
                                    ALTER TABLE SOURCE_URLS 
                                    ADD AUTO_VECTORIZE NUMBER(1) DEFAULT 1
                                """)
                                
                                print(f"Added AUTO_VECTORIZE column to existing SOURCE_URLS table")
                                
                # Run migration to update LINKS_LIMIT back to PAGE_LIMIT if needed
                update_links_limit_to_page_limit()
                
                connection.commit()
                print("Database tables initialized successfully")
    except Exception as e:
        print(f"Error initializing tables: {e}")
        traceback.print_exc()

def stop_vectorization(user_id, source_url):
    """
    Properly stop any running vectorization for the specified source
    
    Args:
        user_id: User ID
        source_url: Source URL to stop vectorization for
        
    Returns:
        bool: True if vectorization was stopped, False if none was running
    """
    key = f"{user_id}:{source_url}"
    
    # Check if a vectorization process exists for this key
    if key in vectorization_manager['events']:
        # Signal thread to stop
        vectorization_manager['events'][key].set()
        print(f"Set stop signal for vectorization of {source_url}")
        
        # Wait a short time for thread to notice the signal
        time.sleep(0.1)
        
        # Remove from tracking
        if key in vectorization_manager['events']:
            del vectorization_manager['events'][key]
        
        if key in vectorization_manager['threads']:
            thread = vectorization_manager['threads'][key]
            # Check if thread is still alive
            if thread.is_alive():
                print(f"Vectorization thread for {source_url} is still running after stop signal")
            else:
                print(f"Vectorization thread for {source_url} has terminated")
            
            del vectorization_manager['threads'][key]
            
        if key in vectorization_manager['status']:
            vectorization_manager['status'][key] = 'stopped'
            
        return True
        
    # Also check legacy tracking if it exists
    if hasattr(start_crawling_and_processing, 'vectorization_events'):
        if key in start_crawling_and_processing.vectorization_events:
            start_crawling_and_processing.vectorization_events[key].set()
            del start_crawling_and_processing.vectorization_events[key]
            print(f"Cleaned up legacy vectorization event for {key}")
            
    if hasattr(start_crawling_and_processing, 'vectorization_threads'):
        if key in start_crawling_and_processing.vectorization_threads:
            del start_crawling_and_processing.vectorization_threads[key]
            print(f"Cleaned up legacy vectorization thread reference for {key}")
            
    return False

def update_database_structure():
    """Add the AUTO_VECTORIZE column to tables if it doesn't exist"""
    try:
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Check and update the PROCESSING_QUEUE_TABLE
                cursor.execute("""
                    SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                    WHERE TABLE_NAME = :table_name AND COLUMN_NAME = 'AUTO_VECTORIZE'
                """, table_name=PROCESSING_QUEUE_TABLE)
                
                if cursor.fetchone()[0] == 0:
                    # Add the AUTO_VECTORIZE column
                    cursor.execute(f"""
                        ALTER TABLE {PROCESSING_QUEUE_TABLE} 
                        ADD AUTO_VECTORIZE NUMBER(1) DEFAULT 1
                    """)
                    print(f"Added AUTO_VECTORIZE column to {PROCESSING_QUEUE_TABLE}")
                
                # Check and update the SOURCE_URLS_TABLE
                cursor.execute("""
                    SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                    WHERE TABLE_NAME = :table_name AND COLUMN_NAME = 'AUTO_VECTORIZE'
                """, table_name=SOURCE_URLS_TABLE)
                
                if cursor.fetchone()[0] == 0:
                    # Add the AUTO_VECTORIZE column
                    cursor.execute(f"""
                        ALTER TABLE {SOURCE_URLS_TABLE} 
                        ADD AUTO_VECTORIZE NUMBER(1) DEFAULT 1
                    """)
                    print(f"Added AUTO_VECTORIZE column to {SOURCE_URLS_TABLE}")
                
                connection.commit()
                return True
    except Exception as e:
        print(f"Error updating database structure: {str(e)}")
        traceback.print_exc()
        return False

# Initialize tables on module load
initialize_tables()

def verify_token():
    """Verify JWT token and get user_id"""
    token = request.headers.get('Authorization')
    
    if not token or not token.startswith("Bearer "):
        return None

    token = token.split(" ")[1]
    try:
        decoded = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        return decoded['user_id']
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

# Authentication decorator
def token_required(f):
    """Decorator for verifying JWT tokens with improved error handling and performance"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        token = request.headers.get('Authorization')
        
        if not token or not token.startswith("Bearer "):
            print("No valid token found in decorator")
            return jsonify({
                'status': 'error',
                'message': 'Unauthorized access. Valid token required.',
                'timestamp': datetime.now().isoformat(),
                'process_time': f"{time.time() - start_time:.2f}s"
            }), 401

        token = token.split(" ")[1]
        
        try:
            decoded = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            user_id = decoded['user_id']
            print(f"Token verified for user_id: {user_id}")
            
            # Cache the decoded token for this request to avoid repeated decoding
            request.user_id = user_id
            
            # Performance optimization: capture start time for the wrapped function
            func_start_time = time.time()
            
            # If the function expects only one argument (user_id)
            if f.__code__.co_argcount == 1:
                result = f(user_id)
            else:
                # If the function can handle multiple arguments
                result = f(user_id, *args, **kwargs)
                
            # Add process_time to JSON responses if not already present
            if isinstance(result, tuple) and len(result) == 2:
                response_data, status_code = result
                if isinstance(response_data, dict) and 'process_time' not in response_data:
                    response_data['process_time'] = f"{time.time() - func_start_time:.2f}s"
                    result = jsonify(response_data), status_code
            
            return result
            
        except jwt.ExpiredSignatureError:
            print("Token has expired")
            return jsonify({
                'status': 'error',
                'message': 'Token has expired',
                'timestamp': datetime.now().isoformat(),
                'process_time': f"{time.time() - start_time:.2f}s"
            }), 401
        except jwt.InvalidTokenError:
            print("Invalid token")
            return jsonify({
                'status': 'error',
                'message': 'Invalid token',
                'timestamp': datetime.now().isoformat(),
                'process_time': f"{time.time() - start_time:.2f}s"
            }), 401
        except Exception as e:
            print(f"Token verification error: {str(e)}")
            traceback.print_exc()
            return jsonify({
                'status': 'error',
                'message': 'Authentication failed',
                'error_details': str(e),
                'timestamp': datetime.now().isoformat(),
                'process_time': f"{time.time() - start_time:.2f}s"
            }), 401
            
    return wrapper

def is_valid_url(url):
    """Enhanced URL validation function"""
    try:
        return validators.url(url)
    except:
        # Some URLs might cause validators to raise exceptions
        return False

def is_problematic_url(url):
    """
    Check if a URL is problematic and should be skipped during crawling
    
    Args:
        url: URL to check
        
    Returns:
        bool: True if URL should be skipped, False otherwise
    """
    # Check for Wikipedia redlinks
    if "wikipedia.org" in url and ("redlink=1" in url or 
                                 ("/w/index.php?title=" in url and "action=edit" in url)):
        return True
    
    # Check for common problematic URL patterns
    problematic_patterns = [
        # File downloads
        r'\.(zip|exe|dmg|tar|gz|pkg|msi|rar|jar|iso)$',
        # Media files that weren't caught by the content check
        r'\.(ico|tiff|bmp|webp|svg|eps|raw|heic|avif)$',
        # Resource files
        r'\.(map|woff|woff2|ttf|eot|json|xml|csv|xls|xlsx|txt)$',
        # Special actions
        r'/(delete|remove|purge|logout|signout|admin|login|auth)',
        # Common tracker or non-content parameters
        r'\?(callback=|nocache=|updated=|v=)',
        # Other APIs
        r'/api/|/rest/|/graphql'
    ]
    
    for pattern in problematic_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    
    # Check for URL length issues
    if len(url) > 500:  # Extremely long URLs can cause issues
        return True
    
    # Check for suspicious repeated patterns (can indicate loops)
    segment_counts = {}
    parts = url.split('/')
    for part in parts:
        if part in segment_counts:
            segment_counts[part] += 1
            if segment_counts[part] > 3:  # More than 3 occurrences of the same segment
                return True
        else:
            segment_counts[part] = 1
    
    return False

def is_valid_content_url(url):
    """Check if URL is likely to contain text content"""
    # First check if it's a problematic URL that should be skipped
    if is_problematic_url(url):
        return False
        
    # Skip common non-text content URLs and query params that indicate non-content
    if re.search(r'\.(jpg|jpeg|png|gif|svg|webp|mp4|mp3|pdf|zip|exe|js|css|xml)$', url, re.IGNORECASE):
        return False

    # Skip common non-content paths
    if re.search(r'/(login|logout|signin|signout|register|cart|checkout|api)/?$', url, re.IGNORECASE):
        return False

    # Skip social media URLs
    if is_social_media_url(url):
        return False
        
    # Additional checks for common query parameters that indicate non-content
    if re.search(r'\?(utm_|ref=|source=|campaign=|medium=)', url, re.IGNORECASE):
        # Only strip these parameters rather than rejecting the URL completely
        try:
            base_url = url.split('?')[0]
            return True
        except:
            pass
            
    # Add check for fragment identifiers (anchors) which often point to the same content
    if '#' in url:
        # We could either strip the fragment or just accept the URL
        return True

    return True

def is_social_media_url(url):
    """Check if the URL is a social media URL"""
    social_media_domains = [
        'facebook.com', 'twitter.com', 'instagram.com', 'linkedin.com', 
        'youtube.com', 'tiktok.com', 'pinterest.com', 'reddit.com', 
        'snapchat.com', 'whatsapp.com', 'telegram.org', 'wechat.com', 
        'tumblr.com', 'flickr.com', 'vk.com', 'weibo.com'
    ]
    
    # Check if the URL contains any social media domain
    for domain in social_media_domains:
        if domain in url:
            return True
    return False

def contains_text_in_url(url):
    """Check if URL contains text content indicators"""
    # Look for words that suggest text content in the URL
    text_indicators = [
        'article', 'blog', 'post', 'news', 'story', 'content', 
        'text', 'page', 'read', 'view', 'doc', 'document', 
        'info', 'about', 'faq', 'help', 'guide', 'tutorial',
        'wiki', 'knowledge', 'learn', 'support'
    ]
    
    # Convert URL to lowercase for case-insensitive matching
    url_lower = url.lower()
    
    # Check if any text indicator appears in the URL
    for indicator in text_indicators:
        if indicator in url_lower:
            return True
            
    return False
def user_has_active_job(user_id):
    """
    Check if a user has an active job with improved logging and validation
    
    Args:
        user_id: The user ID to check
        
    Returns:
        bool: True if the user has any active jobs, False otherwise
    """
    if not user_id:
        print("Invalid user_id provided to user_has_active_job")
        return False
        
    has_active = user_id in active_user_jobs and len(active_user_jobs[user_id]) > 0
    
    if has_active:
        active_count = len(active_user_jobs[user_id])
        active_jobs = ", ".join(list(active_user_jobs[user_id])[:3])  # Show first few jobs
        print(f"User {user_id} has {active_count} active job(s): {active_jobs}...")
    else:
        print(f"User {user_id} has no active jobs")
        
    return has_active

def add_active_job(user_id, source_url):
    """
    Add a job to the active jobs list for a user with validation
    
    Args:
        user_id: The user ID
        source_url: The source URL for the job
        
    Returns:
        bool: True if added successfully, False otherwise
    """
    if not user_id or not source_url:
        print("Invalid parameters provided to add_active_job")
        return False
        
    if user_id not in active_user_jobs:
        active_user_jobs[user_id] = set()
    
    # Check if already in active jobs
    if source_url in active_user_jobs[user_id]:
        print(f"Job already active: {source_url} for user {user_id}")
        return False
        
    active_user_jobs[user_id].add(source_url)
    print(f"Added active job: {source_url} for user {user_id}")
    
    # Log total active jobs for this user
    job_count = len(active_user_jobs[user_id])
    print(f"User {user_id} now has {job_count} active job(s)")
    
    return True

def remove_active_job(user_id, source_url):
    """
    Remove a job from the active jobs list for a user
    
    Args:
        user_id: The user ID
        source_url: The source URL for the job
        
    Returns:
        bool: True if removed successfully, False otherwise
    """
    if not user_id or not source_url:
        print("Invalid parameters provided to remove_active_job")
        return False
        
    if user_id not in active_user_jobs:
        print(f"User {user_id} has no active jobs to remove")
        return False
    
    if source_url not in active_user_jobs[user_id]:
        print(f"Job not found in active jobs: {source_url} for user {user_id}")
        return False
    
    active_user_jobs[user_id].remove(source_url)
    print(f"Removed active job: {source_url} for user {user_id}")
    
    # Clean up if no more active jobs
    if len(active_user_jobs[user_id]) == 0:
        del active_user_jobs[user_id]
        print(f"User {user_id} now has no active jobs, removed from tracking")
    else:
        job_count = len(active_user_jobs[user_id])
        print(f"User {user_id} now has {job_count} active job(s)")
    
    return True
def auto_vectorize_data(user_id, source_url):
    """
    Automatically trigger vectorization for a completed URL
    
    Args:
        user_id: The user ID
        source_url: The source URL that has been processed
        
    Returns:
        dict: Results of the vectorization process
    """
    try:
        print(f"Auto-vectorizing data for URL: {source_url}, user: {user_id}")
        
        # Import the vectorize function from oracle_chatbot.py
        from oracle_chatbot import process_scrapped_text_to_vector_store
        
        # Use connection pool with context manager
        with get_jsondb_connection() as jsondb_connection:
            # Call the vectorization function with source_level_url filter
            result = process_scrapped_text_to_vector_store(
                jsondb_connection, 
                user_id=user_id, 
                source_level_url=source_url
            )
            
            print(f"Vectorization completed for {source_url}: {result}")
            return result
    
    except Exception as e:
        print(f"Error during auto-vectorization for {source_url}: {str(e)}")
        traceback.print_exc()
        return {
            'status': 'error',
            'message': str(e),
            'source_url': source_url
        }
def synchronize_active_jobs(user_id=None):
    """
    Synchronize in-memory active jobs tracking with database state
    to ensure consistency in queue processing.
    
    Args:
        user_id: Optional user ID to sync only for a specific user
        
    Returns:
        dict: Statistics about the synchronization
    """
    try:
        print(f"Synchronizing active jobs{' for user ' + str(user_id) if user_id else ''}")
        
        # Stats to return
        stats = {
            'added_to_memory': 0,
            'removed_from_memory': 0,
            'active_jobs_before': sum(len(jobs) for jobs in active_user_jobs.values()) if active_user_jobs else 0,
            'active_jobs_after': 0
        }
        
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Build query based on parameters
                query = """
                    SELECT USER_ID, SOURCE_URL FROM {0}
                    WHERE PROCESSED = 0 AND PROCESSING_STARTED IS NOT NULL
                """.format(PROCESSING_QUEUE_TABLE)
                
                params = {}
                
                if user_id:
                    query += " AND USER_ID = :user_id"
                    params['user_id'] = user_id
                    
                # Get currently active jobs from database
                cursor.execute(query, params)
                active_from_db = {}
                
                for row in cursor.fetchall():
                    db_user_id, db_source_url = row
                    if db_user_id not in active_from_db:
                        active_from_db[db_user_id] = set()
                    active_from_db[db_user_id].add(db_source_url)
                
                # If filtering by user_id, only process that user
                users_to_process = [user_id] if user_id else list(active_from_db.keys())
                
                # Add any missing users from in-memory tracking
                if not user_id:
                    users_to_process.extend([u for u in active_user_jobs.keys() if u not in users_to_process])
                
                # For each user, sync memory with database
                for uid in users_to_process:
                    if uid not in active_from_db:
                        active_from_db[uid] = set()
                    
                    if uid not in active_user_jobs:
                        active_user_jobs[uid] = set()
                    
                    # Items in DB but not in memory should be added to memory
                    for url in active_from_db[uid]:
                        if url not in active_user_jobs[uid]:
                            active_user_jobs[uid].add(url)
                            stats['added_to_memory'] += 1
                            print(f"Added to memory tracking: {url} for user {uid}")
                    
                    # Items in memory but not in DB should be removed from memory
                    urls_to_remove = []
                    for url in active_user_jobs[uid]:
                        if url not in active_from_db[uid]:
                            urls_to_remove.append(url)
                    
                    for url in urls_to_remove:
                        active_user_jobs[uid].remove(url)
                        stats['removed_from_memory'] += 1
                        print(f"Removed from memory tracking: {url} for user {uid}")
                    
                    # Clean up empty sets
                    if not active_user_jobs[uid]:
                        del active_user_jobs[uid]
                        print(f"Removed empty tracking for user {uid}")
        
        # Calculate final stats
        stats['active_jobs_after'] = sum(len(jobs) for jobs in active_user_jobs.values()) if active_user_jobs else 0
        
        print(f"Synchronization complete: {stats}")
        return stats
    
    except Exception as e:
        print(f"Error synchronizing active jobs: {str(e)}")
        traceback.print_exc()
        return {
            'error': str(e),
            'added_to_memory': 0,
            'removed_from_memory': 0,
            'active_jobs_before': 0,
            'active_jobs_after': 0
        }
def recover_stalled_jobs(user_id=None, max_age_minutes=30):
    """
    Check for and recover stalled jobs in the queue
    
    Args:
        user_id: Optional user ID to check only for a specific user
        max_age_minutes: Maximum age in minutes for a job to be considered stalled
        
    Returns:
        dict: Statistics about recovered jobs
    """
    try:
        print(f"Checking for stalled jobs{' for user ' + str(user_id) if user_id else ''}")
        
        # Stats to return
        stats = {
            'stalled_jobs_found': 0,
            'jobs_recovered': 0,
            'jobs_failed': 0
        }
        
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(minutes=max_age_minutes)
        
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Build query to find stalled jobs
                query = """
                    SELECT ID, USER_ID, SOURCE_URL, PAGE_LIMIT,
                           TO_CHAR(PROCESSING_STARTED, 'YYYY-MM-DD HH24:MI:SS') as START_TIME
                    FROM {0}
                    WHERE PROCESSED = 0 
                    AND PROCESSING_STARTED IS NOT NULL
                    AND PROCESSING_STARTED < :cutoff_time
                """.format(PROCESSING_QUEUE_TABLE)
                
                params = {'cutoff_time': cutoff_time}
                
                if user_id:
                    query += " AND USER_ID = :user_id"
                    params['user_id'] = user_id
                
                # Get stalled jobs
                cursor.execute(query, params)
                stalled_jobs = cursor.fetchall()
                
                stats['stalled_jobs_found'] = len(stalled_jobs)
                
                if stalled_jobs:
                    print(f"Found {len(stalled_jobs)} stalled jobs")
                    
                    # Process each stalled job
                    for job in stalled_jobs:
                        job_id, job_user_id, job_url, job_limit, job_start_time = job
                        
                        try:
                            print(f"Recovering stalled job: {job_url} for user {job_user_id}, started at {job_start_time}")
                            
                            # Clean up any active job tracking
                            if job_user_id in active_user_jobs and job_url in active_user_jobs[job_user_id]:
                                active_user_jobs[job_user_id].remove(job_url)
                                if not active_user_jobs[job_user_id]:
                                    del active_user_jobs[job_user_id]
                            
                           # Clean up any events
                            event_key = f"{job_user_id}:{job_url}"
                            if event_key in crawling_events:
                                crawling_events[event_key].set()
                                del crawling_events[event_key]
                            if event_key in processing_events:
                                processing_events[event_key].set()
                                del processing_events[event_key]
                                
                            # Clean up any vectorization events and threads
                            if event_key in vectorization_events:
                                vectorization_events[event_key].set()
                                del vectorization_events[event_key]
                                print(f"Cleaned up stalled vectorization event for {event_key}")
                            
                            if event_key in vectorization_threads:
                                del vectorization_threads[event_key]
                                print(f"Cleaned up stalled vectorization thread reference for {event_key}")
                            
                            # Synchronize active job tracking if we recovered any jobs
                            if stats['jobs_recovered'] > 0:
                                synchronize_active_jobs(user_id)
                            
                            # Reset the PROCESSING_STARTED field in the database
                            cursor.execute("""
                                UPDATE {0} SET PROCESSING_STARTED = NULL
                                WHERE ID = :id
                            """.format(PROCESSING_QUEUE_TABLE), id=job_id)
                            
                            connection.commit()
                            stats['jobs_recovered'] += 1
                            
                            print(f"Successfully reset stalled job: {job_url}")
                        except Exception as job_error:
                            print(f"Error recovering job {job_id} ({job_url}): {str(job_error)}")
                            stats['jobs_failed'] += 1
                else:
                    print("No stalled jobs found")
        
        # Synchronize active job tracking if we recovered any jobs
        if stats['jobs_recovered'] > 0:
            synchronize_active_jobs(user_id)
        
        print(f"Stalled job recovery complete: {stats}")
        return stats
        
    except Exception as e:
        print(f"Error recovering stalled jobs: {str(e)}")
        traceback.print_exc()
        return {
            'error': str(e),
            'stalled_jobs_found': 0,
            'jobs_recovered': 0,
            'jobs_failed': 0
        }    
# Queue Management Functions
def perform_queue_maintenance():
    """
    Perform maintenance on the queue to ensure it's in a consistent state.
    This function should be called at application startup.
    
    Returns:
        dict: Statistics about maintenance operations
    """
    try:
        print("Starting queue maintenance...")
        
        stats = {
            'stalled_jobs': 0,
            'memory_sync': {},
            'next_jobs_started': 0,
            'memory_cleaned': 0,
            'vectorization_cleaned': 0
        }
        
        # 1. Recover stalled jobs
        stalled_results = recover_stalled_jobs(max_age_minutes=15)  # Consider jobs stalled after 15 minutes
        stats['stalled_jobs'] = stalled_results
        
        # 2. Synchronize in-memory job tracking with database
        sync_results = synchronize_active_jobs()
        stats['memory_sync'] = sync_results
        
        # 3. Clean up completed vectorization threads
        stats['vectorization_cleaned'] = clean_vectorization_threads()
        
        # 4. For each user, check if they have no active jobs but queued jobs waiting
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Find users with queued jobs but no active processing
                cursor.execute("""
                    SELECT DISTINCT USER_ID FROM {0}
                    WHERE PROCESSED = 0 AND PROCESSING_STARTED IS NULL
                """.format(PROCESSING_QUEUE_TABLE))
                
                users_with_queued = [row[0] for row in cursor.fetchall()]
                
                for user_id in users_with_queued:
                    # Check if this user has any active jobs
                    if not user_has_active_job(user_id):
                        # No active jobs, but has queued jobs - start the next one
                        next_item = get_next_from_queue(user_id)
                        
                        if next_item:
                            next_url = next_item['source_url']
                            page_limit = next_item['page_limit']
                            print(f"Starting next queued job for inactive user {user_id}: {next_url}")
                            
                            # Start processing
                            result = start_crawling_and_processing(user_id, next_url, page_limit)
                            
                            if result:
                                stats['next_jobs_started'] += 1
                                print(f"Successfully started processing for queue item: {next_url}")
        
        print(f"Queue maintenance completed: {stats}")
        return stats
        
    except Exception as e:
        print(f"Error during queue maintenance: {str(e)}")
        traceback.print_exc()
        return {
            'error': str(e),
            'stalled_jobs': 0,
            'memory_sync': {},
            'next_jobs_started': 0,
            'memory_cleaned': 0,
            'vectorization_cleaned': 0
        }
    
# This function should be called when the application starts
def initialize_queue_system():
    """Initialize the queue system at application startup"""
    try:
        print("Initializing queue system...")
        
        # Reset in-memory tracking
        global active_user_jobs, crawling_events, processing_events
        active_user_jobs = {}
        crawling_events = {}
        processing_events = {}
        
        # Run maintenance to recover from any previous crashes
        maintenance_results = perform_queue_maintenance()
        
        print(f"Queue system initialized: {maintenance_results}")
        
        # Optional: Set up a background thread to periodically check for stalled jobs
        def maintenance_worker():
            while True:
                try:
                    time.sleep(300)  # Run every 5 minutes
                    print("Running scheduled queue maintenance...")
                    perform_queue_maintenance()
                except Exception as e:
                    print(f"Error in maintenance worker: {str(e)}")
        
        maintenance_thread = Thread(target=maintenance_worker, daemon=True)
        maintenance_thread.start()
        
        return True
    except Exception as e:
        print(f"Error initializing queue system: {str(e)}")
        traceback.print_exc()
        return False
    
def add_to_queue(user_id, source_url, additional_data=None):
    """Add a URL to the processing queue for a user with better error handling and logging"""
    try:
        print(f"Adding URL to queue: {source_url} for user {user_id}")
        
        # Validate input
        if not user_id or not source_url:
            print("Invalid input: user_id and source_url are required")
            return False
            
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Check if it already exists in the queue
                cursor.execute("""
                    SELECT ID, PROCESSED, PROCESSING_STARTED FROM {0}
                    WHERE USER_ID = :user_id AND SOURCE_URL = :source_url
                """.format(PROCESSING_QUEUE_TABLE), 
                   user_id=user_id, source_url=source_url)
                
                existing_row = cursor.fetchone()
                
                if existing_row:
                    id, processed, started = existing_row
                    
                    if processed == 0:
                        # Item already in queue and not processed
                        if started:
                            print(f"URL already in progress: {source_url} for user {user_id}")
                        else:
                            print(f"URL already in queue: {source_url} for user {user_id}")
                        return False
                    else:
                        # Item was processed before, re-add it
                        print(f"URL was previously processed, re-adding to queue: {source_url}")
                        
                        # Delete old entry
                        cursor.execute("""
                            DELETE FROM {0}
                            WHERE ID = :id
                        """.format(PROCESSING_QUEUE_TABLE), id=id)
                        
                        # Continue to add new entry
                    
                # Set defaults
                page_limit = 10
                auto_vectorize = True
                
                # Update with any additional data
                if additional_data:
                    if 'page_limit' in additional_data:
                        page_limit = additional_data['page_limit']
                    if 'auto_vectorize' in additional_data:
                        auto_vectorize = additional_data['auto_vectorize']
                
                # Insert queue item with retry logic
                retry_count = 0
                max_retries = 3
                
                while retry_count < max_retries:
                    try:
                        # Check if PAGE_LIMIT or LINKS_LIMIT column exists
                        cursor.execute("""
                            SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                            WHERE TABLE_NAME = 'PROCESSING_QUEUE' AND COLUMN_NAME = 'PAGE_LIMIT'
                        """)
                        
                        has_page_limit = cursor.fetchone()[0] > 0
                        
                        cursor.execute("""
                            SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                            WHERE TABLE_NAME = 'PROCESSING_QUEUE' AND COLUMN_NAME = 'LINKS_LIMIT'
                        """)
                        
                        has_links_limit = cursor.fetchone()[0] > 0
                        
                        # Prepare column names based on what exists in the database
                        if has_page_limit:
                            limit_column = "PAGE_LIMIT"
                        elif has_links_limit:
                            limit_column = "LINKS_LIMIT"
                        else:
                            limit_column = "PAGE_LIMIT"  # Default to PAGE_LIMIT if neither exists
                        
                        # Build the dynamic SQL
                        insert_sql = f"""
                            INSERT INTO {PROCESSING_QUEUE_TABLE} (
                                USER_ID, SOURCE_URL, ADDED_AT, PROCESSED, 
                                {limit_column}, AUTO_VECTORIZE
                            ) VALUES (
                                :user_id, :source_url, CURRENT_TIMESTAMP, 0, 
                                :limit_value, :auto_vectorize
                            )
                        """
                        
                        cursor.execute(insert_sql,
                           user_id=user_id, 
                           source_url=source_url, 
                           limit_value=page_limit,
                           auto_vectorize=1 if auto_vectorize else 0)
                        
                        connection.commit()
                        print(f"Added URL to queue: {source_url} for user {user_id}, page_limit: {page_limit}, auto_vectorize: {auto_vectorize}")
                        return True
                    except Exception as db_error:
                        # Retry on database errors
                        print(f"Database error adding to queue (attempt {retry_count+1}): {str(db_error)}")
                        retry_count += 1
                        time.sleep(0.5)  # Short delay before retry
                
                # If we reach here, all retries failed
                print(f"Failed to add URL to queue after {max_retries} attempts: {source_url}")
                return False
    except Exception as e:
        print(f"Error adding to queue: {str(e)}")
        traceback.print_exc()
        return False
# Add these functions to your file_api.py file, preferably near the other helper functions

def extract_domain(url):
    """Helper function to extract domain from URL"""
    try:
        # Remove protocol and path
        domain = re.sub(r'^https?://', '', url)
        domain = re.sub(r'/.*$', '', domain)
        return domain.lower()
    except:
        return None

def continuous_crawl_job(top_level_source_url, stop_event, user_id=None, link_limit=10):
    """
    Worker function to continuously crawl links from the starting URL
    with improved batch processing, until the specified number of links are crawled.
    
    Args:
        top_level_source_url: The top-level source URL to crawl
        stop_event: Event object to signal when to stop crawling
        user_id: User ID for associating crawled content
        link_limit: Maximum number of links to crawl (0 = unlimited)
        
    Returns:
        dict: Dictionary containing crawl statistics
    """
    start_time = time.time()
    try:
        print(f"Starting crawl job for {top_level_source_url} for user {user_id}")
        
        # Extract the original domain for same-domain crawling
        original_domain = extract_domain(top_level_source_url)
        if not original_domain:
            print(f"Invalid domain extracted from URL: {top_level_source_url}")
            return {
                'error': f"Invalid domain extracted from URL: {top_level_source_url}",
                'process_time': f"{time.time() - start_time:.2f}s"
            }
        
        print(f"Original domain for same-domain crawling: {original_domain}")

        # Validate user_id
        if not user_id:
            print("No user_id provided for crawl job")
            return {
                'error': "No user ID provided",
                'process_time': f"{time.time() - start_time:.2f}s"
            }

        # Stats counters
        links_added = 0
        errors_encountered = 0
        already_crawled_count = 0
        skipped_urls_count = 0
        
        # Add a flag to track if we've already crawled the initial page
        initial_page_crawled = False
        current_depth = 0  # Start with depth 0 (first page only)
        
        # NEW: Add a counter for consecutive empty depths
        consecutive_empty_depths = 0
        max_consecutive_empty_depths = 20  # Stop after checking 20 consecutive depths with no links
        
        # CRITICAL FIX: Add transaction isolation to prevent duplicate link additions
        try:
            with get_jsondb_connection() as connection:
                with connection.cursor() as cursor:
                    # Ensure that the initial URL is counted as part of the link limit
                    # Check if the initial URL exists
                    cursor.execute("""
                        SELECT COUNT(*) FROM {0}
                        WHERE LINK = :link AND USER_ID = :user_id AND TOP_LEVEL_SOURCE = :source_url
                    """.format(LINKS_TO_SCRAP_TABLE), 
                       link=top_level_source_url, user_id=user_id, source_url=top_level_source_url)
                    
                    has_initial_url = cursor.fetchone()[0] > 0
                    
                    if not has_initial_url:
                        # Insert the initial URL if it doesn't exist
                        try:
                            cursor.execute("""
                                INSERT INTO {0} (
                                    LINK, ADDED_AT, IS_CRAWLED, IS_PROCESSED, 
                                    DEPTH, SOURCE_URL, TOP_LEVEL_SOURCE, USER_ID,
                                    HAS_TEXT_IN_URL
                                ) VALUES (
                                    :link, CURRENT_TIMESTAMP, 0, 'false',
                                    0, :source_url, :top_level_source, :user_id,
                                    1
                                )
                            """.format(LINKS_TO_SCRAP_TABLE),
                                link=top_level_source_url,
                                source_url=top_level_source_url,
                                top_level_source=top_level_source_url,
                                user_id=user_id
                            )
                            print(f"Initial URL added: {top_level_source_url}")
                        except oracledb.IntegrityError as ie:
                            # Handle duplicate entry gracefully
                            print(f"Initial URL already exists: {top_level_source_url}, error: {str(ie)}")
                            pass
                    
                    # CRITICAL FIX: Delete any excess links if a smaller limit is specified
                    if link_limit > 0:
                        # Get the current count of links
                        cursor.execute("""
                            SELECT COUNT(*) FROM {0}
                            WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                        """.format(LINKS_TO_SCRAP_TABLE), 
                           source_url=top_level_source_url, user_id=user_id)
                        
                        current_count = cursor.fetchone()[0]
                        
                        # If we already have more links than the limit, we need to delete the excess
                        if current_count > link_limit:
                            print(f"Current link count ({current_count}) exceeds limit ({link_limit}). Removing excess links.")
                            
                            # Keep only the first link_limit links (ordered by ID to ensure stable behavior)
                            # First, get the IDs of links to keep
                            cursor.execute(f"""
                                SELECT ID FROM (
                                    SELECT ID FROM {LINKS_TO_SCRAP_TABLE}
                                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                                    ORDER BY ID ASC
                                ) WHERE ROWNUM <= :limit
                            """, source_url=top_level_source_url, user_id=user_id, limit=link_limit)
                            
                            keep_ids = [row[0] for row in cursor.fetchall()]
                            
                            if keep_ids:
                                # Delete links not in the keep list
                                placeholders = ','.join([str(id) for id in keep_ids])
                                
                                cursor.execute(f"""
                                    DELETE FROM {LINKS_TO_SCRAP_TABLE}
                                    WHERE TOP_LEVEL_SOURCE = :source_url 
                                    AND USER_ID = :user_id
                                    AND ID NOT IN ({placeholders})
                                """, source_url=top_level_source_url, user_id=user_id)
                                
                                deleted_count = cursor.rowcount
                                print(f"Deleted {deleted_count} excess links to enforce limit of {link_limit}")
                            
                            # Recalculate current count after deletion
                            cursor.execute("""
                                SELECT COUNT(*) FROM {0}
                                WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                            """.format(LINKS_TO_SCRAP_TABLE), 
                               source_url=top_level_source_url, user_id=user_id)
                            
                            current_count = cursor.fetchone()[0]
                            print(f"New link count after deletion: {current_count}")
                    
                    # Now continue with normal crawling if we haven't reached the limit
                    if link_limit > 0 and current_count >= link_limit:
                        print(f"Reached link limit ({link_limit}). No additional crawling needed.")
                        return {
                            'message': f"Link limit reached with {current_count} links",
                            'links_crawled': already_crawled_count,
                            'links_added': links_added,
                            'process_time': f"{time.time() - start_time:.2f}s"
                        }
        except Exception as setup_error:
            print(f"Error during initial setup: {str(setup_error)}")
            traceback.print_exc()
            return {
                'error': str(setup_error),
                'traceback': traceback.format_exc(),
                'process_time': f"{time.time() - start_time:.2f}s"
            }
        
        # Check if we have a limit
        if link_limit == 0:
            print("Link limit is set to 0 (unlimited)")
        else:
            print(f"Link limit is set to {link_limit} links")
        
        # Create a thread pool for parallel HTTP requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            while not stop_event.is_set():
                try:
                    # Open a new connection for each iteration to prevent errors from propagating
                    connection = None
                    try:
                        connection = connect_to_jsondb()
                        if not connection:
                            print("Failed to connect to database. Retrying...")
                            time.sleep(1)
                            continue
                        
                        cursor = connection.cursor()
                        
                        # Get the current count of all links
                        cursor.execute("""
                            SELECT COUNT(*) FROM {0}
                            WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                        """.format(LINKS_TO_SCRAP_TABLE), 
                           source_url=top_level_source_url, user_id=user_id)
                        
                        total_links_count = cursor.fetchone()[0]
                        
                        # CRITICAL CHECK: Enforce the strict link limit
                        if link_limit > 0 and total_links_count >= link_limit:
                            print(f"REACHED LINK LIMIT: Total of {total_links_count} links meets or exceeds limit of {link_limit}. Exiting.")
                            break
                        
                        # Check how many URLs have already been crawled for this user and source
                        cursor.execute("""
                            SELECT COUNT(*) FROM {0}
                            WHERE IS_CRAWLED = 1 AND TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                        """.format(LINKS_TO_SCRAP_TABLE), 
                           source_url=top_level_source_url, user_id=user_id)
                        
                        already_crawled_count = cursor.fetchone()[0]
                        
                        # Also check how many URLs were skipped
                        cursor.execute("""
                            SELECT COUNT(*) FROM {0}
                            WHERE SKIPPED = 1 AND TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                        """.format(LINKS_TO_SCRAP_TABLE), 
                           source_url=top_level_source_url, user_id=user_id)
                        
                        skipped_urls_count = cursor.fetchone()[0]
                        print(f"Stats: {already_crawled_count} crawled, {skipped_urls_count} skipped URLs, {total_links_count} total links")
                        
                        # Calculate remaining capacity under our limit
                        remaining_capacity = link_limit - total_links_count if link_limit > 0 else float('inf')
                        if remaining_capacity <= 0:
                            print(f"REACHED LINK LIMIT: {total_links_count} total links meets or exceeds limit of {link_limit}. Breaking.")
                            break
                        
                        # Find uncrawled links at current depth
                        cursor.execute("""
                            SELECT ID, LINK, NVL(DEPTH, 0) AS DEPTH 
                            FROM {0}
                            WHERE (IS_CRAWLED = 0 OR IS_CRAWLED IS NULL)
                            AND TOP_LEVEL_SOURCE = :source_url 
                            AND USER_ID = :user_id
                            AND DEPTH = :depth
                            ORDER BY ADDED_AT ASC
                        """.format(LINKS_TO_SCRAP_TABLE), 
                           source_url=top_level_source_url, user_id=user_id, depth=current_depth)
                        
                        links_at_current_depth = cursor.fetchall()
                        
                        # If no links at current depth, increment depth and check again
                        if not links_at_current_depth:
                            # If this was depth 0 (first page), we need to move to depth 1
                            if current_depth == 0 and already_crawled_count == 0:
                                # We haven't even crawled the initial URL, so add it first
                                # Use a PL/SQL block with exception handling for duplicates
                                try:
                                    cursor.execute("""
                                        BEGIN
                                            -- Try to insert the initial URL
                                            INSERT INTO {0} (
                                                LINK, ADDED_AT, IS_CRAWLED, IS_PROCESSED, 
                                                DEPTH, SOURCE_URL, TOP_LEVEL_SOURCE, USER_ID,
                                                HAS_TEXT_IN_URL
                                            ) VALUES (
                                                :link, CURRENT_TIMESTAMP, 0, 'false',
                                                0, :source_url, :top_level_source, :user_id,
                                                1
                                            );
                                        EXCEPTION
                                            -- Handle unique constraint violation
                                            WHEN DUP_VAL_ON_INDEX THEN
                                                NULL; -- Skip silently if already exists
                                        END;
                                    """.format(LINKS_TO_SCRAP_TABLE),
                                        link=top_level_source_url,
                                        source_url=top_level_source_url,
                                        top_level_source=top_level_source_url,
                                        user_id=user_id
                                    )
                                    connection.commit()
                                except Exception as insert_error:
                                    print(f"Error inserting initial URL: {str(insert_error)}")
                                    # Continue anyway as the URL might already exist
                                    pass
                                continue
                            elif initial_page_crawled:
                                print(f"No more links at depth {current_depth}, moving to depth {current_depth + 1}")
                                current_depth += 1
                                consecutive_empty_depths += 1  # Increment empty depth counter
                                
                                # Check if we've checked too many empty depths
                                if consecutive_empty_depths >= max_consecutive_empty_depths:
                                    print(f"No new links found after checking {max_consecutive_empty_depths} consecutive depths. Exiting crawl job.")
                                    break
                                
                                continue
                            else:
                                print(f"No more links to crawl at any depth. Exiting crawl job.")
                                break
                        
                        # Reset consecutive empty depths counter since we found links
                        consecutive_empty_depths = 0
                        
                        # Limit how many links we process from this depth based on our remaining capacity
                        links_to_process = links_at_current_depth[:min(10, remaining_capacity)]
                        
                        if not links_to_process:
                            print(f"No more capacity for links at depth {current_depth}. Exiting crawl job.")
                            break
                            
                        # Mark all of these links as being crawled
                        link_ids = [row[0] for row in links_to_process]
                        placeholders = ','.join([f':id{i}' for i in range(len(link_ids))])
                        id_dict = {f'id{i}': id_val for i, id_val in enumerate(link_ids)}
                        
                        cursor.execute(f"""
                            UPDATE {LINKS_TO_SCRAP_TABLE} 
                            SET CRAWLING_STARTED = CURRENT_TIMESTAMP
                            WHERE ID IN ({placeholders})
                        """, **id_dict)
                        
                        connection.commit()
                        
                        batch_links_added = 0
                        batch_errors = 0
                        batch_skipped = 0
                        
                        # Get a set of all existing links for faster checking
                        cursor.execute("""
                            SELECT LINK FROM {0}
                            WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                        """.format(LINKS_TO_SCRAP_TABLE), 
                           source_url=top_level_source_url, user_id=user_id)
                        
                        existing_links = set(row[0] for row in cursor.fetchall())
                        
                        # Process links in parallel using thread pool
                        futures = []
                        for link_id, url_to_crawl, depth in links_to_process:
                            if stop_event.is_set():
                                print(f"Stop event triggered during batch setup. Breaking out.")
                                break
                            
                            # Calculate remaining capacity for this link
                            current_remaining = remaining_capacity - batch_links_added
                            if current_remaining <= 0:
                                print(f"No remaining capacity for batch. Breaking.")
                                break
                            
                            # Submit each link to the thread pool with remaining capacity
                            futures.append(
                                executor.submit(
                                    crawl_single_link, 
                                    url_to_crawl, 
                                    depth, 
                                    original_domain,
                                    top_level_source_url,
                                    user_id,  # Pass user_id 
                                    current_remaining,  # Pass remaining capacity specific to this link
                                    existing_links  # Pass existing links for checking
                                )
                            )
                        
                        # Process results as they complete with improved error handling
                        completed_futures = []
                        try:
                            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                                if stop_event.is_set():
                                    print(f"Stop event triggered during batch processing. Breaking out.")
                                    break
                                
                                completed_futures.append((i, future))
                        except Exception as concurrent_error:
                            print(f"Error managing futures: {concurrent_error}")
                            # Continue with the futures we've collected so far
                        
                        # Process the results we've collected
                        for i, future in completed_futures:
                            try:
                                if i >= len(links_to_process):
                                    print(f"Warning: Future index {i} out of range for links_to_process (len: {len(links_to_process)})")
                                    continue
                                    
                                link_id, url_to_crawl, depth = links_to_process[i]
                                
                                # Get the result from the completed future with proper error handling
                                try:
                                    result = future.result()
                                except Exception as future_error:
                                    print(f"Error getting future result for {url_to_crawl}: {future_error}")
                                    batch_errors += 1
                                    
                                    # Try to update the database to mark the link as failed
                                    try:
                                        cursor.execute("""
                                            UPDATE {0} SET
                                                IS_CRAWLED = 1,
                                                CRAWLED_AT = CURRENT_TIMESTAMP,
                                                ERROR = :error
                                            WHERE ID = :id
                                        """.format(LINKS_TO_SCRAP_TABLE), 
                                            id=link_id,
                                            error=str(future_error)[:4000]
                                        )
                                        
                                        connection.commit()
                                    except Exception as db_error:
                                        print(f"Database error marking link as crawled: {str(db_error)}")
                                    
                                    continue
                                
                                links_found = result.get('links_found', 0)
                                successful_links = result.get('successful_links', 0)
                                error = result.get('error', None)
                                
                                # If depth is 0, mark that we've crawled the initial page
                                if depth == 0:
                                    initial_page_crawled = True
                                
                                # Check if the URL was skipped
                                if result.get('skipped', False):
                                    # Update the link as skipped
                                    try:
                                        cursor.execute("""
                                            UPDATE {0} SET
                                                IS_CRAWLED = 1,
                                                CRAWLED_AT = CURRENT_TIMESTAMP,
                                                SKIPPED = 1,
                                                SKIP_REASON = :skip_reason
                                            WHERE ID = :id
                                        """.format(LINKS_TO_SCRAP_TABLE), 
                                            id=link_id,
                                            skip_reason=result.get('skip_reason', 'Unknown reason')[:100]  # Limit to column size
                                        )
                                        
                                        connection.commit()
                                        print(f"Marked link as skipped: {url_to_crawl} - Reason: {result.get('skip_reason', 'Unknown')}")
                                        batch_skipped += 1
                                        continue
                                    except Exception as update_error:
                                        print(f"Error updating link as skipped: {update_error}")
                                        # Continue processing other results
                                
                                # Update the current link as crawled
                                try:
                                    cursor.execute("""
                                        UPDATE {0} SET
                                            IS_CRAWLED = 1,
                                            CRAWLED_AT = CURRENT_TIMESTAMP,
                                            LINKS_FOUND = :links_found,
                                            LINKS_ADDED = :links_added,
                                            ERROR = :error
                                        WHERE ID = :id
                                    """.format(LINKS_TO_SCRAP_TABLE), 
                                        id=link_id,
                                        links_found=links_found,
                                        links_added=successful_links,
                                        error=error[:4000] if error else None
                                    )
                                    
                                    connection.commit()
                                    
                                    # Add the successful links to our counts
                                    if successful_links > 0:
                                        batch_links_added += successful_links
                                    
                                    if error:
                                        batch_errors += 1
                                except Exception as update_error:
                                    print(f"Error updating link as crawled: {update_error}")
                                    # Continue processing other results
                            
                            except Exception as process_error:
                                batch_errors += 1
                                print(f"Error processing future for URL {url_to_crawl if 'url_to_crawl' in locals() else 'unknown'}: {str(process_error)}")
                                traceback.print_exc()
                                
                                # Try to update the database
                                try:
                                    if 'link_id' in locals():
                                        cursor.execute("""
                                            UPDATE {0} SET
                                                IS_CRAWLED = 1,
                                                CRAWLED_AT = CURRENT_TIMESTAMP,
                                                ERROR = :error
                                            WHERE ID = :id
                                        """.format(LINKS_TO_SCRAP_TABLE), 
                                            id=link_id,
                                            error=str(process_error)[:4000]
                                        )
                                        connection.commit()
                                except Exception as db_error:
                                    print(f"Database error marking link as crawled: {str(db_error)}")
                                
                                # Continue with the next link - don't break
                                continue
                        
                        # Update stats after batch
                        links_added += batch_links_added
                        errors_encountered += batch_errors
                        skipped_urls_count += batch_skipped
                        
                        print(f"Batch completed: Added {batch_links_added} links, encountered {batch_errors} errors, skipped {batch_skipped} URLs")
                        
                        # CRITICAL: Check total link count after batch to enforce the limit
                        cursor.execute("""
                            SELECT COUNT(*) FROM {0}
                            WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                        """.format(LINKS_TO_SCRAP_TABLE), 
                           source_url=top_level_source_url, user_id=user_id)
                        
                        total_links_after_batch = cursor.fetchone()[0]
                        
                        if link_limit > 0 and total_links_after_batch >= link_limit:
                            print(f"REACHED LINK LIMIT AFTER BATCH: {total_links_after_batch} total links meets or exceeds limit of {link_limit}. Exiting.")
                            break
                        
                        # Brief pause between batches - much shorter than before
                        time.sleep(0.1)
                    
                    finally:
                        # Make sure to clean up cursor and connection resources
                        if 'cursor' in locals() and cursor:
                            try:
                                cursor.close()
                            except:
                                pass
                            
                        if connection:
                            try:
                                connection.close()
                            except:
                                pass
                    
                except Exception as batch_error:
                    print(f"Error processing batch: {str(batch_error)}")
                    traceback.print_exc()
                    errors_encountered += 1
                    time.sleep(1)  # Brief delay on error before continuing
                    continue  # IMPORTANT: Continue rather than break to process more links
            
            # When we exit the loop, check if we need to finish up early
            if consecutive_empty_depths >= max_consecutive_empty_depths:
                print(f"Crawl job terminated early due to {consecutive_empty_depths} consecutive empty depths")
                print(f"This indicates all available links have been processed for {top_level_source_url}")
                
        # Get final statistics
        try:
            with get_jsondb_connection() as connection:
                with connection.cursor() as cursor:
                    # Count total links for this source
                    cursor.execute("""
                        SELECT COUNT(*) FROM {0}
                        WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                    """.format(LINKS_TO_SCRAP_TABLE), 
                       source_url=top_level_source_url, user_id=user_id)
                    
                    final_total_count = cursor.fetchone()[0]
                    
                    # Count crawled links
                    cursor.execute("""
                        SELECT COUNT(*) FROM {0}
                        WHERE IS_CRAWLED = 1 AND TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                    """.format(LINKS_TO_SCRAP_TABLE), 
                       source_url=top_level_source_url, user_id=user_id)
                    
                    final_crawled_count = cursor.fetchone()[0]
                    
                    # FINAL CHECK: If we have more links than limit, delete the excess
                    if link_limit > 0 and final_total_count > link_limit:
                        print(f"FINAL CHECK: Found {final_total_count} links, limit is {link_limit}. Removing excess.")
                        
                        # Keep only the first link_limit links
                        cursor.execute(f"""
                            SELECT ID FROM (
                                SELECT ID FROM {LINKS_TO_SCRAP_TABLE}
                                WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                                ORDER BY ID ASC
                            ) WHERE ROWNUM <= :limit
                        """, source_url=top_level_source_url, user_id=user_id, limit=link_limit)
                        
                        keep_ids = [row[0] for row in cursor.fetchall()]
                        
                        if keep_ids:
                            # Delete links not in the keep list
                            placeholders = ','.join([str(id) for id in keep_ids])
                            
                            cursor.execute(f"""
                                DELETE FROM {LINKS_TO_SCRAP_TABLE}
                                WHERE TOP_LEVEL_SOURCE = :source_url 
                                AND USER_ID = :user_id
                                AND ID NOT IN ({placeholders})
                            """, source_url=top_level_source_url, user_id=user_id)
                            
                            deleted_count = cursor.rowcount
                            print(f"Final cleanup: Deleted {deleted_count} excess links to enforce limit of {link_limit}")
                            
                            # Get new counts after deletion
                            cursor.execute("""
                                SELECT COUNT(*) FROM {0}
                                WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                            """.format(LINKS_TO_SCRAP_TABLE), 
                               source_url=top_level_source_url, user_id=user_id)
                            
                            final_total_count = cursor.fetchone()[0]
                            
                            cursor.execute("""
                                SELECT COUNT(*) FROM {0}
                                WHERE IS_CRAWLED = 1 AND TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                            """.format(LINKS_TO_SCRAP_TABLE), 
                               source_url=top_level_source_url, user_id=user_id)
                            
                            final_crawled_count = cursor.fetchone()[0]
        except Exception as stats_error:
            print(f"Error getting final statistics: {str(stats_error)}")
            traceback.print_exc()
            # Use the last known values if there was an error
            final_total_count = total_links_count if 'total_links_count' in locals() else 0
            final_crawled_count = already_crawled_count if 'already_crawled_count' in locals() else 0
        
        # Return comprehensive stats
        return {
            'links_crawled': final_crawled_count,
            'links_added': links_added,
            'errors_encountered': errors_encountered,
            'skipped_urls_count': skipped_urls_count,
            'final_link_count': final_total_count,
            'link_limit': link_limit,
            'link_limit_enforced': True,
            'total_time_seconds': round(time.time() - start_time, 2),
            'links_per_second': round(links_added / (time.time() - start_time) if time.time() - start_time > 0 else 0, 2),
            'process_time': f"{time.time() - start_time:.2f}s",
            'consecutive_empty_depths': consecutive_empty_depths,
            'early_termination': consecutive_empty_depths >= max_consecutive_empty_depths
        }
            
    except Exception as e:
        print(f"Error in continuous crawl job: {str(e)}")
        traceback.print_exc()
        return {
            'error': str(e),
            'traceback': traceback.format_exc(),
            'process_time': f"{time.time() - start_time:.2f}s"
        }
    
def crawl_single_link(url_to_crawl, current_depth, original_domain, top_level_source_url, user_id=None, remaining_capacity=10, already_crawled_links=None):
    """
    Crawl a single link and return the results, strictly enforcing link limit.
    
    Args:
        url_to_crawl: URL to crawl
        current_depth: Current crawl depth
        original_domain: Original domain to filter links by
        top_level_source_url: Top level source URL for attribution
        user_id: User ID to associate with the links
        remaining_capacity: Maximum number of links that can still be added (default: 10)
        already_crawled_links: Set of URLs that have already been crawled
        
    Returns:
        dict: Dictionary containing crawl results
    """
    try:
        # Initialize already_crawled_links if not provided
        if already_crawled_links is None:
            already_crawled_links = set()
            
        # CRITICAL FIX: Validate and enforce remaining capacity before processing
        if remaining_capacity <= 0:
            print(f"No remaining capacity for {url_to_crawl}. Skipping.")
            return {
                'links_found': 0,
                'successful_links': 0,
                'url': url_to_crawl,
                'skipped': True,
                'skip_reason': 'No remaining link capacity'
            }
            
        # Check if URL should be skipped
        if is_problematic_url(url_to_crawl):
            print(f"Skipping problematic URL: {url_to_crawl}")
            return {
                'links_found': 0,
                'successful_links': 0,
                'url': url_to_crawl,
                'skipped': True,
                'skip_reason': 'Problematic URL pattern'
            }
            
        # Add user agent to avoid being blocked
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Ensure user_id is available
        if not user_id:
            print(f"No user_id provided for {url_to_crawl}")
            return {
                'links_found': 0,
                'successful_links': 0,
                'url': url_to_crawl,
                'skipped': True,
                'skip_reason': 'No user ID'
            }
        
        # Make request to the URL with reduced timeout
        print(f"Making HTTP request to: {url_to_crawl}")
        
        try:
            response = requests.get(url_to_crawl, headers=headers, timeout=20)
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code in [404, 403, 410, 500]:
                print(f"Skipping URL due to HTTP error {http_err.response.status_code}: {url_to_crawl}")
                return {
                    'links_found': 0,
                    'successful_links': 0,
                    'url': url_to_crawl,
                    'skipped': True,
                    'skip_reason': f'HTTP {http_err.response.status_code} error'
                }
            else:
                # Re-raise for other status codes
                raise
        
        # Check content type before parsing
        content_type = response.headers.get('Content-Type', '').lower()
        if not ('text/html' in content_type or 'application/xhtml+xml' in content_type):
            print(f"Skipping non-HTML content type ({content_type}): {url_to_crawl}")
            return {
                'links_found': 0,
                'successful_links': 0,
                'url': url_to_crawl,
                'skipped': True,
                'skip_reason': f'Non-HTML content: {content_type}'
            }
        
        # Check content length to avoid large files
        content_length = response.headers.get('Content-Length')
        if content_length and int(content_length) > 5 * 1024 * 1024:  # 5MB
            print(f"Skipping large content ({content_length} bytes): {url_to_crawl}")
            return {
                'links_found': 0,
                'successful_links': 0,
                'url': url_to_crawl,
                'skipped': True,
                'skip_reason': f'Content too large: {content_length} bytes'
            }
        
        # Parse the HTML content
        print(f"Parsing HTML content from: {url_to_crawl}")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all anchor tags
        all_links = soup.find_all('a', href=True)
        
        # Base URL for resolving relative URLs
        base_url = url_to_crawl
        
        # Check if the HTML has a base tag
        base_tag = soup.find('base', href=True)
        if base_tag:
            base_url = base_tag['href']
        
        # Extract URLs
        unique_links = []
        
        for link in all_links:
            href = link['href'].strip()
            
            # Skip empty hrefs, javascript:, mailto:, tel: links
            if not href or href.startswith(('javascript:', 'mailto:', 'tel:', '#')):
                continue
            
            try:
                # Convert relative URLs to absolute URLs
                full_url = urljoin(base_url, href)
                
                # Check if the URL belongs to the same domain
                link_domain = extract_domain(full_url)
                if link_domain != original_domain:
                    continue
                
                # Skip invalid URLs, non-content URLs, problematic URLs, and social media URLs
                if (not is_valid_url(full_url) or 
                    not is_valid_content_url(full_url) or 
                    is_social_media_url(full_url) or
                    is_problematic_url(full_url)):
                    continue
                
                # CRITICAL FIX: Skip links we've already crawled
                if full_url in already_crawled_links:
                    continue
                
                unique_links.append(full_url)
            except Exception as link_error:
                print(f"Error processing URL {href}: {str(link_error)}")
                continue
        
        # Remove duplicates
        unique_links = list(set(unique_links))
        print(f"Found {len(unique_links)} valid URLs on {url_to_crawl}")
        
        # CRITICAL: Strictly limit the number of links to the remaining capacity
        limited_links = unique_links[:remaining_capacity]
        print(f"Limited to {len(limited_links)} links based on remaining capacity: {remaining_capacity}")
        
        # Form values to be inserted with the strictly enforced link limit
        values_list = []
        for link in limited_links:
            # Skip links we're already tracking
            if link in already_crawled_links:
                continue
                
            has_text = 1 if contains_text_in_url(link) else 0
            values_list.append({
                'link': link,
                'top_level_source': top_level_source_url,
                'user_id': user_id,
                'source_url': url_to_crawl,
                'depth': current_depth + 1,
                'has_text': has_text
            })
        
        # Use connection pool to insert links with improved error handling
        successful_links = 0
        if values_list:
            try:
                with get_jsondb_connection() as connection:
                    # Use optimized batch insert with improved duplicate handling
                    successful_links = batch_insert_links(connection, values_list)
                    print(f"Successfully processed {successful_links} links out of {len(values_list)}")
            except Exception as db_error:
                print(f"Database error in link insertion: {str(db_error)}")
                traceback.print_exc()
                
        return {
            'links_found': len(unique_links),
            'successful_links': successful_links,
            'url': url_to_crawl,
            'link_limit': remaining_capacity
        }
        
    except requests.exceptions.RequestException as req_error:
        print(f"Request error processing URL {url_to_crawl}: {str(req_error)}")
        return {
            'links_found': 0,
            'successful_links': 0,
            'url': url_to_crawl,
            'error': str(req_error),
            'skipped': True,
            'skip_reason': f'Request error: {str(req_error)[:100]}'
        }
    except Exception as e:
        print(f"Unexpected error processing URL {url_to_crawl}: {str(e)}")
        traceback.print_exc()
        return {
            'links_found': 0,
            'successful_links': 0,
            'url': url_to_crawl,
            'error': str(e),
            'skipped': True,
            'skip_reason': f'Processing error: {str(e)[:100]}'
        }
    
def batch_insert_links(connection, links_data):
    """
    Perform a batch insert of links using a more efficient approach
    with better duplicate handling
    
    Args:
        connection: Database connection
        links_data: List of dictionaries with link data
        
    Returns:
        Number of successfully inserted links
    """
    if not links_data:
        return 0
        
    inserted_count = 0
    
    try:
        cursor = connection.cursor()
        
        # First, validate input data
        validated_links = []
        for link_data in links_data:
            # Comprehensive validation
            if not link_data.get('link'):
                print(f"Skipping link with no URL: {link_data}")
                continue
            
            # Ensure all required keys are present
            required_keys = ['link', 'top_level_source', 'user_id', 'source_url', 'depth', 'has_text']
            if not all(key in link_data for key in required_keys):
                print(f"Incomplete link data, missing required keys: {link_data}")
                continue
            
            # Additional URL validation
            if not is_valid_url(link_data['link']):
                print(f"Invalid URL format: {link_data['link']}")
                continue
            
            validated_links.append(link_data)
        
        # If no valid links, return 0
        if not validated_links:
            print("No valid links to insert")
            return 0
        
        # Insert links with improved duplicate handling
        for link_data in validated_links:
            try:
                # Use a PL/SQL block with exception handling for duplicates
                cursor.execute(f"""
                    BEGIN
                        -- Try to insert the link
                        INSERT INTO {LINKS_TO_SCRAP_TABLE} (
                            LINK, TOP_LEVEL_SOURCE, USER_ID, SOURCE_URL, 
                            DEPTH, HAS_TEXT_IN_URL, ADDED_AT, 
                            IS_CRAWLED, IS_PROCESSED
                        ) VALUES (
                            :link, :top_level_source, :user_id, :source_url,
                            :depth, :has_text, CURRENT_TIMESTAMP,
                            0, 'false'
                        );
                        
                    EXCEPTION
                        -- Handle unique constraint violation
                        WHEN DUP_VAL_ON_INDEX THEN
                            -- Instead of failing, update the existing record if depth is lower
                            UPDATE {LINKS_TO_SCRAP_TABLE} 
                            SET DEPTH = LEAST(DEPTH, :depth)
                            WHERE LINK = :link AND USER_ID = :user_id
                            AND :depth < DEPTH;
                    END;
                """, 
                    link=link_data['link'],
                    top_level_source=link_data['top_level_source'],
                    user_id=link_data['user_id'],
                    source_url=link_data['source_url'],
                    depth=link_data['depth'],
                    has_text=link_data['has_text']
                )
                inserted_count += 1  # Count as successful since it was handled appropriately
                
            except Exception as insert_error:
                # More robust error handling
                if "ORA-00001" in str(insert_error):  # Unique constraint violation
                    print(f"Link {link_data.get('link', 'unknown')} already exists - skipping via exception handler")
                    
                    # Try to update the record directly
                    try:
                        cursor.execute(f"""
                            UPDATE {LINKS_TO_SCRAP_TABLE} 
                            SET DEPTH = LEAST(DEPTH, :depth)
                            WHERE LINK = :link AND USER_ID = :user_id
                            AND :depth < DEPTH
                        """, 
                            link=link_data['link'],
                            user_id=link_data['user_id'],
                            depth=link_data['depth']
                        )
                        # Count as successful even though it's an update
                        inserted_count += 1
                    except Exception as update_error:
                        print(f"Error updating existing link: {update_error}")
                else:
                    # This is a different error
                    print(f"Error inserting link {link_data.get('link', 'unknown')}: {insert_error}")
        
        # Commit the entire batch
        connection.commit()
        
        print(f"Successfully processed {inserted_count} links out of {len(validated_links)}")
        return inserted_count
        
    except Exception as batch_error:
        # Comprehensive error handling
        print(f"Batch insert failed: {batch_error}")
        traceback.print_exc()
        
        # Rollback in case of any database-level errors
        try:
            connection.rollback()
        except Exception as rollback_error:
            print(f"Error during rollback: {rollback_error}")
        
        return inserted_count  # Return partial success count
    finally:
        # Ensure cursor is closed
        if cursor:
            try:
                cursor.close()
            except Exception as close_error:
                print(f"Error closing cursor: {close_error}")
    
def start_crawling_and_processing(user_id, source_url, link_limit=10, auto_vectorize=True):
    """
    Start the crawling and processing for a URL simultaneously with a shorter processing delay
    and improved concurrency using Thread objects, and now with simultaneous vectorization
    
    Args:
        user_id: User ID
        source_url: Source URL to crawl
        link_limit: Maximum number of links to crawl (default: 10)
        auto_vectorize: Whether to automatically vectorize after completion (default: True)
    """
    # Add to active jobs list
    add_active_job(user_id, source_url)
    
    # First stop any existing vectorization for this source
    stop_vectorization(user_id, source_url)
    
    # Create stop events for crawling and processing
    crawl_key = f"{user_id}:{source_url}"
    crawl_stop_event = Event()
    crawling_events[crawl_key] = crawl_stop_event
    
    process_key = f"{user_id}:{source_url}"
    process_stop_event = Event()
    processing_events[process_key] = process_stop_event
    
    # Start simultaneous vectorization if auto_vectorize is True
    if auto_vectorize:
        vectorization_stop_event = Event()
        vectorization_thread = start_simultaneous_vectorization(
            user_id=user_id,
            source_url=source_url,
            stop_event=vectorization_stop_event
        )
        
        # Store in the consolidated manager
        vectorization_manager['events'][crawl_key] = vectorization_stop_event
        vectorization_manager['threads'][crawl_key] = vectorization_thread
        vectorization_manager['status'][crawl_key] = 'running'
        
        # For backward compatibility, also store in the legacy attributes
        if not hasattr(start_crawling_and_processing, 'vectorization_events'):
            start_crawling_and_processing.vectorization_events = {}
        if not hasattr(start_crawling_and_processing, 'vectorization_threads'):
            start_crawling_and_processing.vectorization_threads = {}
            
        start_crawling_and_processing.vectorization_events[crawl_key] = vectorization_stop_event
        start_crawling_and_processing.vectorization_threads[crawl_key] = vectorization_thread
    
    # Create a shared variable to track completion status
    completion_status = {
        'crawling_done': False,
        'processing_done': False,
        'in_final_cooldown': False,
        'links_before_cooldown': 0,
        'auto_vectorize': auto_vectorize,  # Store auto_vectorize flag in shared status
        'link_limit': link_limit  # Store link_limit for sharing between threads
    }

    # Define a wrapper function to start the crawling
    def start_crawl(url, stop_event, user_id, link_limit, completion_status):
        try:
            print(f"Starting crawl thread for {url}, user: {user_id}, limit: {link_limit} links")
            result = continuous_crawl_job(url, stop_event, user_id, link_limit)
            print(f"Crawling completed with result: {result}")
            
            # Mark crawling as done
            completion_status['crawling_done'] = True
            print(f"Marked crawling as done for {url}, user: {user_id}")
            
            # When done, check if processing is also done
            if completion_status['processing_done']:
                # If both are done, start the final cool-down
                print(f"Both crawling and processing are done, starting final cooldown for {url}")
                start_final_cooldown(user_id, url, completion_status)
        except Exception as e:
            print(f"Error in crawl thread: {e}")
            traceback.print_exc()
            
            # Mark crawling as done even on error
            completion_status['crawling_done'] = True
            print(f"Marked crawling as done (with error) for {url}, user: {user_id}")
            
            # If there's an error, still check if we need to start cooldown
            if completion_status['processing_done']:
                print(f"Processing was already done, starting final cooldown for {url} despite crawling error")
                start_final_cooldown(user_id, url, completion_status)
    
    # Define a function for the processing thread with shorter delay
    def start_processing(url, stop_event, user_id, completion_status):
        try:
            # Use 5 seconds delay (reduced from 15)
            delay_seconds = 5
            # Get the link_limit from the shared status
            link_limit = completion_status.get('link_limit', 10)
            print(f"Starting processing thread for {url}, user: {user_id} with {delay_seconds}s delay, link limit: {link_limit}")
            
            # Pass the link_limit to continuous_processing_job
            result = continuous_processing_job(url, stop_event, delay_seconds, user_id, link_limit)
            print(f"Processing completed with result: {result}")
            
            # Mark processing as done
            completion_status['processing_done'] = True
            print(f"Marked processing as done for {url}, user: {user_id}")
            
            # When done, check if crawling is also done
            if completion_status['crawling_done']:
                # If both are done, start the final cool-down
                print(f"Both crawling and processing are done, starting final cooldown for {url}")
                start_final_cooldown(user_id, url, completion_status)
            else:
                # Wait for crawling to finish
                print(f"Processing finished, waiting for crawling to complete for {url}")
        except Exception as e:
            print(f"Error in processing thread: {e}")
            traceback.print_exc()
            
            # Mark processing as done even on error
            completion_status['processing_done'] = True
            print(f"Marked processing as done (with error) for {url}, user: {user_id}")
            
            # If there's an error, still check if we need to start cooldown
            if completion_status['crawling_done']:
                print(f"Crawling was already done, starting final cooldown for {url} despite processing error")
                start_final_cooldown(user_id, url, completion_status)
    
    # Function to start the final cool-down period (shorter duration)
    def start_final_cooldown(user_id, url, completion_status):
        try:
            # Check if we're already in cooldown to prevent multiple cooldown processes
            if completion_status.get('in_final_cooldown', False):
                print(f"Already in final cooldown for {url}, user: {user_id}. Skipping duplicate cooldown.")
                return
                
            print(f"Starting final cooldown for {url}, user: {user_id}")
            completion_status['in_final_cooldown'] = True
            
            with get_jsondb_connection() as connection:
                with connection.cursor() as cursor:
                    # Count total links before cooldown
                    cursor.execute("""
                        SELECT COUNT(*) FROM {0}
                        WHERE TOP_LEVEL_SOURCE = :url AND USER_ID = :user_id
                    """.format(LINKS_TO_SCRAP_TABLE), 
                       url=url, user_id=user_id)
                    
                    completion_status['links_before_cooldown'] = cursor.fetchone()[0]
                    
                    # Reduced cooldown from 40 to 20 seconds for faster completion
                    cooldown_duration = 20  # 20 seconds 
                    cooldown_start_time = time.time()
                    
                    # Check for new links every 5 seconds during the cool-down period
                    while time.time() - cooldown_start_time < cooldown_duration:
                        # Skip if the stop events are set
                        if crawl_key in crawling_events and crawling_events[crawl_key].is_set():
                            print(f"Crawling stop event is set during cooldown for {url}. Terminating cooldown.")
                            break
                            
                        if process_key in processing_events and processing_events[process_key].is_set():
                            print(f"Processing stop event is set during cooldown for {url}. Terminating cooldown.")
                            break
                        
                        # Calculate remaining time
                        elapsed = time.time() - cooldown_start_time
                        remaining = cooldown_duration - elapsed
                        print(f"In final cool-down period. {remaining:.1f} seconds remaining. Checking for new links...")
                        
                        # Check if any new links have been discovered
                        cursor.execute("""
                            SELECT COUNT(*) FROM {0}
                            WHERE TOP_LEVEL_SOURCE = :url AND USER_ID = :user_id
                        """.format(LINKS_TO_SCRAP_TABLE), 
                           url=url, user_id=user_id)
                        
                        current_link_count = cursor.fetchone()[0]
                        
                        # Check if any links still need processing
                        cursor.execute("""
                            SELECT COUNT(*) FROM {0}
                            WHERE TOP_LEVEL_SOURCE = :url AND USER_ID = :user_id
                            AND (IS_PROCESSED = 'false' OR IS_PROCESSED IS NULL)
                        """.format(LINKS_TO_SCRAP_TABLE), 
                           url=url, user_id=user_id)
                        
                        unprocessed_count = cursor.fetchone()[0]
                        
                        if current_link_count > completion_status['links_before_cooldown'] or unprocessed_count > 0:
                            print(f"New links discovered during cool-down! Links before: {completion_status['links_before_cooldown']}, Current: {current_link_count}, Unprocessed: {unprocessed_count}")
                            print(f"Restarting crawling and processing for {url}")
                            
                            # Reset completion status
                            completion_status['crawling_done'] = False
                            completion_status['processing_done'] = False
                            completion_status['in_final_cooldown'] = False
                            
                            # Start the crawling and processing again
                            if unprocessed_count > 0 or current_link_count > completion_status['links_before_cooldown']:
                                # Get the link_limit from the shared status
                                link_limit = completion_status.get('link_limit', 10)
                                
                                # Start the crawl thread again
                                new_crawl_thread = Thread(
                                    target=start_crawl,
                                    args=(url, crawl_stop_event, user_id, link_limit, completion_status),
                                    daemon=True
                                )
                                new_crawl_thread.start()
                                
                                # Start the process thread again
                                new_process_thread = Thread(
                                    target=start_processing,
                                    args=(url, process_stop_event, user_id, completion_status),
                                    daemon=True
                                )
                                new_process_thread.start()
                                
                                # Exit this cool-down function to let the new threads handle it
                                return
                        
                        # Sleep for 3 seconds before checking again (reduced from 5)
                        time.sleep(3)
                    
                    # Cool-down period has elapsed with no new links
                    print(f"Final cool-down period of {cooldown_duration} seconds has elapsed. No new links found. Completing job for {url}")
                    
                    # Pass auto_vectorize flag when marking as complete
                    mark_as_complete_and_process_next(user_id, url, completion_status['auto_vectorize'])
                    
        except Exception as e:
            print(f"Error in final cool-down: {str(e)}")
            traceback.print_exc()
            
            # Even if there's an error, try to move to the next URL
            mark_as_complete_and_process_next(user_id, url, completion_status['auto_vectorize'])
    
    # Start the crawling in a background thread
    crawler_thread = Thread(
        target=start_crawl,
        args=(source_url, crawl_stop_event, user_id, link_limit, completion_status),
        daemon=True
    )
    crawler_thread.start()
    
    # Start the processing in a background thread (with shorter delay)
    processor_thread = Thread(
        target=start_processing,
        args=(source_url, process_stop_event, user_id, completion_status),
        daemon=True
    )
    processor_thread.start()
    
    print(f"Started simultaneous crawling and processing for {source_url}, user {user_id}, link_limit: {link_limit}, auto_vectorize: {auto_vectorize}")
    return True

def mark_as_complete_and_process_next(user_id, source_url, auto_vectorize=True):
    """
    Mark a URL as complete in the queue, update SOURCE_URLS.IS_PROCESSED to 1,
    trigger vectorization if all crawling & scraping is complete,
    and start processing the next URL in queue ONLY after vectorization is done.
    
    Will mark as failed if processing takes more than 10 seconds with no other active jobs.
    """
    try:
        start_time = time.time()
        print(f"Marking URL as complete and checking for next URL: {source_url} for user {user_id}, auto_vectorize: {auto_vectorize}")
        
        # First stop any existing vectorization for this source
        stop_vectorization(user_id, source_url)
        
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Calculate if all links have been processed
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_links,
                        SUM(CASE WHEN IS_CRAWLED = 1 THEN 1 ELSE 0 END) as crawled_links,
                        SUM(CASE WHEN IS_PROCESSED = 'true' THEN 1 ELSE 0 END) as processed_links,
                        SUM(CASE WHEN IS_PROCESSED = 'Failed' THEN 1 ELSE 0 END) as failed_links
                    FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                """.format(LINKS_TO_SCRAP_TABLE), 
                   source_url=source_url, user_id=user_id)
                
                result = cursor.fetchone()
                total_links, crawled_links, processed_links, failed_links = result
                
                # Count scrapped documents
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                """.format(SCRAPPED_TEXT_TABLE), 
                   source_url=source_url, user_id=user_id)
                
                scrapped_count = cursor.fetchone()[0]
                
                print(f"Stats for {source_url}: Total={total_links}, Crawled={crawled_links}, Processed={processed_links}, Scrapped={scrapped_count}, Failed={failed_links}")
                
                # Check if we're exceeding the timeout
                elapsed_time = time.time() - start_time
                
                # Check if other processes are running for this user
                other_processes_running = False
                if user_id in active_user_jobs:
                    # Check if there are other active jobs besides the current one
                    other_jobs = [job for job in active_user_jobs[user_id] if job != source_url]
                    other_processes_running = len(other_jobs) > 0
                    
                # If no other processes are running and we're exceeding the timeout, mark as failed
                if not other_processes_running and elapsed_time > 10:
                    print(f"⚠️ Process timeout exceeded ({elapsed_time:.2f}s) with no other active jobs. Marking as failed: {source_url}")
                    
                    # Mark as failed in the processing queue
                    cursor.execute("""
                        UPDATE {0} SET 
                            PROCESSED = 1, 
                            PROCESSING_COMPLETED = CURRENT_TIMESTAMP
                        WHERE USER_ID = :user_id AND SOURCE_URL = :source_url
                    """.format(PROCESSING_QUEUE_TABLE),
                       user_id=user_id, source_url=source_url)
                    
                    # Mark as processed in SOURCE_URLS
                    cursor.execute("""
                        UPDATE {0}
                        SET IS_PROCESSED = 1
                        WHERE SOURCE_URL = :source_url AND USER_ID = :user_id
                    """.format(SOURCE_URLS_TABLE),
                       source_url=source_url, user_id=user_id)
                    
                    connection.commit()
                    
                    # Clean up tracking
                    if user_id in active_user_jobs and source_url in active_user_jobs[user_id]:
                        active_user_jobs[user_id].remove(source_url)
                        if not active_user_jobs[user_id]:
                            del active_user_jobs[user_id]
                    
                    # Move to next job
                    next_item = get_next_from_queue(user_id)
                    if next_item:
                        next_url = next_item['source_url']
                        link_limit = next_item.get('page_limit', 10)
                        next_auto_vectorize = next_item.get('auto_vectorize', True)
                        print(f"Starting to process next URL after timeout failure: {next_url}")
                        start_crawling_and_processing(user_id, next_url, link_limit, next_auto_vectorize)
                    
                    return True
                
                # Determine if the job is complete
                # Conditions for completion:
                # 1. Some documents are scraped, OR
                # 2. All identified links are processed (either successfully or failed)
                is_complete = (scrapped_count > 0) or (total_links > 0 and processed_links + failed_links == total_links)
                
                # Check if source URL is completely failed
                is_completely_failed = (
                    total_links > 0 and  # Ensure some links were attempted
                    (failed_links == total_links or  # All links failed
                     scrapped_count == 0)  # No documents scraped
                )
                
                print(f"Job completion check for {source_url}: is_complete = {is_complete}, is_completely_failed = {is_completely_failed}")
                
                # Clean up all tracking for this job
                crawl_key = f"{user_id}:{source_url}"
                process_key = f"{user_id}:{source_url}"
                
                # Update the active user jobs tracking immediately to prevent new jobs from starting
                if user_id in active_user_jobs:
                    if source_url in active_user_jobs[user_id]:
                        active_user_jobs[user_id].remove(source_url)
                        print(f"Removed {source_url} from active jobs for user {user_id}")
                    if not active_user_jobs[user_id]:
                        del active_user_jobs[user_id]
                        print(f"No more active jobs for user {user_id}, removed from tracking")
                
                # Clean up event objects for crawling and processing
                if crawl_key in crawling_events:
                    crawling_events[crawl_key].set()  # Set the event first to signal threads to terminate
                    del crawling_events[crawl_key]
                    print(f"Cleaned up crawling event for {crawl_key}")
                    
                if process_key in processing_events:
                    processing_events[process_key].set()  # Set the event first to signal threads to terminate
                    del processing_events[process_key]
                    print(f"Cleaned up processing event for {process_key}")
                
                # Mark the current URL as processed or failed in the queue
                cursor.execute("""
                    UPDATE {0} SET 
                        PROCESSED = 1, 
                        PROCESSING_COMPLETED = CURRENT_TIMESTAMP
                    WHERE USER_ID = :user_id AND SOURCE_URL = :source_url
                """.format(PROCESSING_QUEUE_TABLE),
                   user_id=user_id, source_url=source_url)
                
                # If the job is complete and auto_vectorize is True, perform vectorization
                if is_complete and auto_vectorize:
                    # Check again for timeout before vectorization
                    elapsed_time = time.time() - start_time
                    if not other_processes_running and elapsed_time > 10:
                        print(f"⚠️ Process timeout exceeded ({elapsed_time:.2f}s) before vectorization. Skipping vectorization and marking as complete.")
                    else:
                        # We'll create a temporary context to ensure the connection is properly managed
                        with get_jsondb_connection() as vec_connection:
                            with vec_connection.cursor() as vec_cursor:
                                # Thorough check for vectorization
                                print(f"Preparing for final vectorization of {source_url}")
                                vectorization_result = perform_final_vectorization(vec_connection, vec_cursor, user_id, source_url)
                
                # Only after vectorization (or timeout) update SOURCE_URLS status
                if is_complete or is_completely_failed:
                    cursor.execute("""
                        UPDATE {0}
                        SET IS_PROCESSED = 1
                        WHERE SOURCE_URL = :source_url AND USER_ID = :user_id
                    """.format(SOURCE_URLS_TABLE),
                       source_url=source_url, user_id=user_id)
                
                # Commit changes
                connection.commit()
                
                # Get the next URL from the queue
                next_item = get_next_from_queue(user_id)
                
                if next_item:
                    next_url = next_item['source_url']
                    link_limit = next_item.get('page_limit', 10)
                    next_auto_vectorize = next_item.get('auto_vectorize', True)
                    
                    print(f"Starting to process next URL: {next_url} with limit of {link_limit} links for user {user_id}")
                    
                    # Ensure there are no lingering events for this URL before starting
                    next_crawl_key = f"{user_id}:{next_url}"
                    next_process_key = f"{user_id}:{next_url}"
                    
                    if next_crawl_key in crawling_events:
                        del crawling_events[next_crawl_key]
                    if next_process_key in processing_events:
                        del processing_events[next_process_key]
                    
                    # Start processing the next URL with its link limit and auto_vectorize flag
                    start_crawling_and_processing(user_id, next_url, link_limit, next_auto_vectorize)
                    return True
                else:
                    print(f"No more URLs in queue for user {user_id}")
                    return False
    
    except Exception as e:
        print(f"Error marking as complete and processing next: {str(e)}")
        traceback.print_exc()
        
        # Handle exception - mark as failed and move to next job
        try:
            with get_jsondb_connection() as recovery_conn:
                with recovery_conn.cursor() as recovery_cursor:
                    # Mark as failed in the processing queue
                    recovery_cursor.execute("""
                        UPDATE {0} SET 
                            PROCESSED = 1, 
                            PROCESSING_COMPLETED = CURRENT_TIMESTAMP
                        WHERE USER_ID = :user_id AND SOURCE_URL = :source_url
                    """.format(PROCESSING_QUEUE_TABLE),
                       user_id=user_id, source_url=source_url)
                    
                    # Mark as processed in SOURCE_URLS
                    recovery_cursor.execute("""
                        UPDATE {0}
                        SET IS_PROCESSED = 1
                        WHERE SOURCE_URL = :source_url AND USER_ID = :user_id
                    """.format(SOURCE_URLS_TABLE),
                       source_url=source_url, user_id=user_id)
                    
                    recovery_conn.commit()
                    
                    # Clean up tracking
                    if user_id in active_user_jobs and source_url in active_user_jobs[user_id]:
                        active_user_jobs[user_id].remove(source_url)
                        if not active_user_jobs[user_id]:
                            del active_user_jobs[user_id]
                    
                    # Try to get next job
                    next_item = get_next_from_queue(user_id)
                    if next_item:
                        next_url = next_item['source_url']
                        link_limit = next_item.get('page_limit', 10)
                        next_auto_vectorize = next_item.get('auto_vectorize', True)
                        print(f"Starting to process next URL after error: {next_url}")
                        start_crawling_and_processing(user_id, next_url, link_limit, next_auto_vectorize)
        except Exception as recovery_error:
            print(f"Error during recovery: {recovery_error}")
            traceback.print_exc()
            
        return False

def perform_final_vectorization(connection, cursor, user_id, source_url):
    """
    Helper function to perform the final vectorization pass with improved checks
    
    Args:
        connection: Database connection
        cursor: Database cursor
        user_id: User ID
        source_url: Source URL to vectorize
    
    Returns:
        bool: Whether vectorization was successful
    """
    # Print visible start message
    for i in range(3):
        print(f"{'='*50}")
    print(f"🔄 FINAL VECTORIZATION ATTEMPTED FOR {source_url}")
    for i in range(3):
        print(f"{'='*50}")
    
    # IMPORTANT: Quick return without performing vectorization
    print(f"⚠️ SKIPPING VECTORIZATION FOR {source_url} AS PER CONFIGURATION")
    
    # Add progress history entry about skipping
    try:
        cursor.execute("""
            INSERT INTO PROGRESS_HISTORY (
                SOURCE_URL, TIMESTAMP, OPERATION_STATUS
            ) VALUES (
                :source_url, CURRENT_TIMESTAMP, :status
            )
        """,
           source_url=source_url,
           status="VECTORIZATION_SKIPPED: Configuration prevents vectorization")
        connection.commit()
    except Exception as e:
        print(f"Warning: Couldn't log vectorization skipping: {str(e)}")
    
    # Print very visible completion message
    for i in range(3):
        print(f"{'='*50}")
    print(f"✅ VECTORIZATION SKIPPED FOR {source_url}")
    for i in range(3):
        print(f"{'='*50}")
    
    return True  # Return True to indicate "successful" skipping

def start_simultaneous_vectorization(user_id, source_url, stop_event=None):
    """
    Start a separate thread that periodically checks for new scraped content
    and vectorizes it in batches, running simultaneously with crawling.
    
    Args:
        user_id: User ID for filtering content
        source_url: Top-level source URL to filter content by
        stop_event: Optional Event to signal stopping the process
        
    Returns:
        Thread object running the vectorization process
    """
    if stop_event is None:
        stop_event = Event()
    
    # Create a unique key for tracking this vectorization job
    job_key = f"{user_id}:{source_url}"
    
    # Check if a vectorization is already running for this key
    if job_key in vectorization_manager['threads']:
        # Get the existing thread
        existing_thread = vectorization_manager['threads'][job_key]
        if existing_thread.is_alive():
            print(f"⚠️ Vectorization already running for {source_url}. Using existing thread.")
            return existing_thread
        else:
            # Thread exists but is no longer running - clean it up
            print(f"Cleaning up completed vectorization thread for {source_url}")
            if job_key in vectorization_manager['events']:
                del vectorization_manager['events'][job_key]
            del vectorization_manager['threads'][job_key]
    
    def vectorization_worker():
        try:
            print(f"🔄 SIMULTANEOUS VECTORIZATION: Starting vectorization worker for {source_url}")
            
            # Keep track of already vectorized document IDs
            vectorized_ids = set()
            
            # Add a counter to limit the number of times we check the stop event
            stop_check_counter = 0
            
            # Number of consecutive empty runs before we slow down checking
            consecutive_empty_runs = 0
            batch_size = 5  # Process in small batches
            
            # Track the start time for reporting
            start_time = time.time()
            total_documents_vectorized = 0
            total_chunks_vectorized = 0
            
            while not stop_event.is_set():
                # Check stop event more frequently
                stop_check_counter += 1
                if stop_check_counter % 10 == 0:
                    if stop_event.is_set():
                        print(f"🛑 Stop event detected during processing. Exiting vectorization worker.")
                        break
                    # Also check if we're still in the tracking dictionary
                    if job_key not in vectorization_manager['threads']:
                        print(f"Vectorization thread for {source_url} no longer in tracking. Exiting.")
                        break
                
                try:
                    # Check if we should still be running (in case cleanup missed us)
                    if job_key not in vectorization_manager['threads']:
                        print(f"Vectorization thread for {source_url} no longer in tracking dictionary. Exiting.")
                        break
                    
                    # Get newly scraped content that hasn't been vectorized yet
                    with get_jsondb_connection() as connection:
                        with connection.cursor() as cursor:
                            # Find new documents that have content and haven't been vectorized
                            # Try querying the tracking table first
                            try:
                                cursor.execute("""
                                    SELECT ID, SCRAPPED_CONTENT, CONTENT_LINK, TOP_LEVEL_SOURCE, TITLE
                                    FROM SCRAPPED_TEXT
                                    WHERE TOP_LEVEL_SOURCE = :source_url 
                                    AND USER_ID = :user_id
                                    AND ID NOT IN (
                                        SELECT DOCUMENT_ID FROM VECTORIZED_DOCUMENTS
                                        WHERE SOURCE_URL = :source_url AND USER_ID = :user_id
                                    )
                                    FETCH FIRST :batch_size ROWS ONLY
                                """, 
                                   source_url=source_url, 
                                   user_id=user_id,
                                   batch_size=batch_size)
                            except Exception as query_error:
                                print(f"Error with tracking query: {str(query_error)}")
                                # Fallback to simpler query if VECTORIZED_DOCUMENTS doesn't exist
                                cursor.execute("""
                                    SELECT ID, SCRAPPED_CONTENT, CONTENT_LINK, TOP_LEVEL_SOURCE, TITLE
                                    FROM SCRAPPED_TEXT
                                    WHERE TOP_LEVEL_SOURCE = :source_url 
                                    AND USER_ID = :user_id
                                    AND ID NOT IN :vectorized_ids
                                    FETCH FIRST :batch_size ROWS ONLY
                                """, 
                                   source_url=source_url, 
                                   user_id=user_id,
                                   vectorized_ids=tuple(vectorized_ids) if vectorized_ids else (-1,),  # Default to impossible ID
                                   batch_size=batch_size)
                            
                            new_docs = cursor.fetchall()
                            
                            if not new_docs:
                                consecutive_empty_runs += 1
                                
                                # If no new documents for a while, sleep longer
                                if consecutive_empty_runs > 5:
                                    wait_time = 10  # 10 seconds after 5 empty runs
                                else:
                                    wait_time = 3   # 3 seconds initially
                                    
                                print(f"🔄 VECTORIZATION: No new documents found. Sleeping for {wait_time} seconds. (Empty run #{consecutive_empty_runs})")
                                
                                # Check if we should exit
                                if consecutive_empty_runs > 15:  # After 15 empty runs (around 2+ minutes)
                                    print(f"🔄 VECTORIZATION: No documents found after {consecutive_empty_runs} attempts. Exiting.")
                                    
                                    # Report summary
                                    elapsed = time.time() - start_time
                                    minutes = elapsed / 60
                                    print(f"🔄 VECTORIZATION SUMMARY: Vectorized {total_documents_vectorized} documents with {total_chunks_vectorized} chunks in {minutes:.1f} minutes.")
                                    
                                    # Update status
                                    vectorization_manager['status'][job_key] = 'idle'
                                    break
                                
                                # Check if stop event is set
                                if stop_event.is_set():
                                    print(f"🛑 Stop event detected before wait. Exiting vectorization worker.")
                                    break
                                
                                # Use stop_event.wait instead of time.sleep to respond to stop events
                                if stop_event.wait(wait_time):
                                    print("🛑 Stop event detected. Exiting vectorization worker.")
                                    break
                                    
                                continue
                            
                            # Reset consecutive empty runs counter
                            consecutive_empty_runs = 0
                            
                            print(f"🔄 VECTORIZATION: Found {len(new_docs)} new documents to vectorize")
                            
                            # Process these documents
                            doc_ids = [row[0] for row in new_docs]
                            
                            # Import necessary functions for vectorization
                            from oracle_chatbot import connect_to_vectdb
                            
                            # Check stop event before creating connections
                            if stop_event.is_set():
                                print(f"🛑 Stop event detected before database connections. Exiting vectorization worker.")
                                break
                            
                            # Use connection pool with context manager
                            with get_jsondb_connection() as jsondb_connection:
                                # Convert the documents to the format expected by the vectorization function
                                # We'll create filtered versions of the documents to vectorize
                                cursor.execute("""
                                    SELECT ID, SCRAPPED_CONTENT, CONTENT_LINK, TOP_LEVEL_SOURCE, TITLE
                                    FROM SCRAPPED_TEXT
                                    WHERE ID IN ({})
                                """.format(','.join([str(doc_id) for doc_id in doc_ids])))
                                
                                docs_to_vectorize = cursor.fetchall()
                                
                                # Create a connection to vector database
                                from oracle_chatbot import connect_to_vectdb
                                vectdb_connection = connect_to_vectdb()
                                
                                if not vectdb_connection:
                                    print("Failed to connect to vector database for vectorization")
                                    continue
                                
                                try:
                                    # Import and initialize the embedding function
                                    from oracle_chatbot import GoogleGenerativeAIEmbeddings
                                    from langchain_community.vectorstores import oraclevs
                                    from langchain_community.vectorstores.utils import DistanceStrategy
                                    from langchain.docstore.document import Document
                                    from langchain.text_splitter import RecursiveCharacterTextSplitter
                                    import os
                                    
                                    # Get API keys and table name
                                    GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
                                    VECTDB_TABLE_NAME = os.getenv("VECTDB_TABLE_NAME", "vector_files_with_10000_chunk_new")
                                    
                                    # Constants for chunking
                                    CHUNK_SIZE = 10000
                                    CHUNK_OVERLAP = 300
                                    
                                    # Initialize embeddings
                                    embeddings = GoogleGenerativeAIEmbeddings(
                                        google_api_key=GOOGLE_API_KEY,
                                        model="models/text-embedding-004"
                                    )
                                    
                                    # Initialize vector store
                                    vector_store = oraclevs.OracleVS(
                                        client=vectdb_connection,
                                        embedding_function=embeddings,
                                        table_name=VECTDB_TABLE_NAME,
                                        distance_strategy=DistanceStrategy.COSINE,
                                    )
                                    
                                    # Create documents from the fetched data
                                    documents = []
                                    for row in docs_to_vectorize:
                                        doc_id, content_lob, link, top_level_source, title = row
                                        
                                        # Convert Oracle LOB object to string
                                        if hasattr(content_lob, 'read'):
                                            content = content_lob.read()
                                        else:
                                            content = str(content_lob)
                                        
                                        # Create metadata
                                        metadata = {
                                            "url": link,
                                            "source": source_url,
                                            "title": title if title else "Untitled",
                                            "document_id": doc_id
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
                                    print(f"🔄 VECTORIZATION: Created {len(chunks)} chunks from {len(documents)} documents")
                                    
                                    # Check stop event before vectorization
                                    if stop_event.is_set():
                                        print(f"🛑 Stop event detected before vectorization. Exiting vectorization worker.")
                                        break
                                    
                                    # Vectorize the chunks
                                    vector_store.add_documents(chunks)
                                    print(f"🔄 VECTORIZATION: Successfully vectorized {len(chunks)} chunks")
                                    
                                    # Update summary stats
                                    total_documents_vectorized += len(documents)
                                    total_chunks_vectorized += len(chunks)
                                    
                                    # Record the vectorized documents in a tracking table
                                    # First, ensure the tracking table exists
                                    try:
                                        cursor.execute("""
                                            SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                                            WHERE TABLE_NAME = 'VECTORIZED_DOCUMENTS'
                                        """)
                                        
                                        if cursor.fetchone()[0] == 0:
                                            # Create the table
                                            cursor.execute("""
                                                CREATE TABLE VECTORIZED_DOCUMENTS (
                                                    ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                                    DOCUMENT_ID NUMBER NOT NULL,
                                                    SOURCE_URL VARCHAR2(2000) NOT NULL,
                                                    USER_ID VARCHAR2(50),
                                                    VECTORIZED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                    CHUNK_COUNT NUMBER DEFAULT 0,
                                                    CONSTRAINT UQ_DOC_SOURCE_USER UNIQUE (DOCUMENT_ID, SOURCE_URL, USER_ID)
                                                )
                                            """)
                                            
                                            # Create an index
                                            cursor.execute("""
                                                CREATE INDEX IDX_VECTORIZED_DOCS 
                                                ON VECTORIZED_DOCUMENTS (DOCUMENT_ID, SOURCE_URL, USER_ID)
                                            """)
                                            
                                            print("Created VECTORIZED_DOCUMENTS tracking table")
                                    except Exception as table_error:
                                        print(f"Error checking/creating tracking table: {str(table_error)}")
                                    
                                    # Record each vectorized document
                                    for doc_id in doc_ids:
                                        try:
                                            # Use a simpler insert with PL/SQL block to handle duplicates
                                            cursor.execute("""
                                                BEGIN
                                                    INSERT INTO VECTORIZED_DOCUMENTS (
                                                        DOCUMENT_ID, SOURCE_URL, USER_ID, CHUNK_COUNT
                                                    ) VALUES (
                                                        :doc_id, :source_url, :user_id, :chunk_count
                                                    );
                                                EXCEPTION
                                                    WHEN DUP_VAL_ON_INDEX THEN
                                                        NULL; -- Silently ignore duplicates
                                                END;
                                            """, 
                                               doc_id=doc_id,
                                               source_url=source_url,
                                               user_id=user_id,
                                               chunk_count=len(chunks) // len(documents) if documents else 0)
                                        except Exception as insert_error:
                                            print(f"Error recording vectorized document {doc_id}: {str(insert_error)}")
                                    
                                    connection.commit()
                                    
                                    # Add to our in-memory tracking
                                    vectorized_ids.update(doc_ids)
                                    
                                    # Add an entry to PROGRESS_HISTORY_TABLE about vectorization
                                    try:
                                        cursor.execute("""
                                            INSERT INTO PROGRESS_HISTORY (
                                                SOURCE_URL, TIMESTAMP, OPERATION_STATUS
                                            ) VALUES (
                                                :source_url, CURRENT_TIMESTAMP, :status
                                            )
                                        """,
                                           source_url=source_url,
                                           status=f"VECTORIZED_BATCH: {len(documents)} documents, {len(chunks)} chunks")
                                        connection.commit()
                                    except Exception as progress_error:
                                        print(f"Warning: Couldn't log vectorization progress: {str(progress_error)}")
                                    
                                finally:
                                    if vectdb_connection:
                                        vectdb_connection.close()
                    
                    # Check if stop event is set before sleeping
                    if stop_event.is_set():
                        print(f"🛑 Stop event detected after batch. Exiting vectorization worker.")
                        break
                    
                    # Sleep between batches (shorter interval since we're processing incrementally)
                    time.sleep(1)
                    
                except Exception as batch_error:
                    print(f"Error in vectorization batch: {str(batch_error)}")
                    traceback.print_exc()
                    time.sleep(5)  # Wait longer after an error
            
            # Final report
            print(f"🔄 VECTORIZATION: Worker for {source_url} completed or stopped.")
            print(f"🔄 VECTORIZATION SUMMARY: Vectorized {total_documents_vectorized} documents with {total_chunks_vectorized} chunks.")
            
            # Update status
            vectorization_manager['status'][job_key] = 'completed'
            
        except Exception as e:
            print(f"Error in vectorization worker: {str(e)}")
            traceback.print_exc()
            vectorization_manager['status'][job_key] = 'error'
    
    # Start the vectorization in a background thread
    vectorization_thread = Thread(target=vectorization_worker, daemon=True)
    vectorization_thread.start()
    
    # Store in manager
    vectorization_manager['events'][job_key] = stop_event
    vectorization_manager['threads'][job_key] = vectorization_thread
    vectorization_manager['status'][job_key] = 'running'
    
    print(f"Started simultaneous vectorization thread for {source_url}")
    return vectorization_thread
    
def get_next_from_queue(user_id):
    """Get the next URL from the queue for a user with improved error handling"""
    try:
        print(f"Getting next URL from queue for user: {user_id}")
        
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Find the oldest unprocessed URL for this user
                cursor.execute("""
                    SELECT ID, SOURCE_URL, PAGE_LIMIT, NVL(AUTO_VECTORIZE, 1) AS AUTO_VECTORIZE 
                    FROM {0}
                    WHERE USER_ID = :user_id AND PROCESSED = 0 AND PROCESSING_STARTED IS NULL
                    ORDER BY ADDED_AT ASC
                    FETCH FIRST 1 ROW ONLY
                """.format(PROCESSING_QUEUE_TABLE), 
                   user_id=user_id)
                
                row = cursor.fetchone()
                
                if row:
                    queue_id, source_url, link_limit, auto_vectorize = row
                    
                    # Mark as processing started with retry logic
                    retry_count = 0
                    max_retries = 3
                    
                    while retry_count < max_retries:
                        try:
                            cursor.execute("""
                                UPDATE {0} SET PROCESSING_STARTED = CURRENT_TIMESTAMP
                                WHERE ID = :id AND PROCESSING_STARTED IS NULL
                            """.format(PROCESSING_QUEUE_TABLE), 
                               id=queue_id)
                            
                            rows_affected = cursor.rowcount
                            connection.commit()
                            
                            if rows_affected > 0:
                                print(f"Successfully marked queue item {queue_id} as started for URL: {source_url}")
                                break
                            else:
                                # Item may have been picked up by another process
                                print(f"Queue item {queue_id} was already being processed by another worker")
                                retry_count += 1
                        except Exception as update_error:
                            print(f"Error updating queue item (attempt {retry_count+1}): {str(update_error)}")
                            retry_count += 1
                            time.sleep(0.5)  # Short delay before retry
                    
                    # Verify queue item is still available after update
                    cursor.execute("""
                        SELECT ID FROM {0}
                        WHERE ID = :id AND PROCESSING_STARTED IS NOT NULL AND PROCESSED = 0
                    """.format(PROCESSING_QUEUE_TABLE), 
                       id=queue_id)
                    
                    verify_row = cursor.fetchone()
                    
                    if verify_row:
                        print(f"Retrieved next URL from queue: {source_url} for user {user_id}")
                        
                        # Return the URL, link limit, and auto_vectorize flag
                        return {
                            'source_url': source_url,
                            'page_limit': link_limit or 10,  # Default to 10 if None (using page_limit for backward compatibility)
                            'auto_vectorize': bool(auto_vectorize)  # Convert to boolean
                        }
                    else:
                        print(f"Queue item {queue_id} could not be reserved, retrying with another item")
                        # Recursive call to get the next item
                        return get_next_from_queue(user_id)
                else:
                    print(f"No more URLs in queue for user {user_id}")
                    return None
    except Exception as e:
        print(f"Error getting next from queue: {str(e)}")
        traceback.print_exc()
        return None

def add_word_count_field():
    """
    Add a WORD_COUNT column to the SCRAPPED_TEXT table if it doesn't exist.
    This is a safe operation that can be run even if the column already exists.
    """
    try:
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Check if the column already exists
                cursor.execute("""
                    SELECT COUNT(*) FROM USER_TAB_COLUMNS 
                    WHERE TABLE_NAME = 'SCRAPPED_TEXT' AND COLUMN_NAME = 'WORD_COUNT'
                """)
                
                column_exists = cursor.fetchone()[0] > 0
                
                if not column_exists:
                    # Add the column
                    cursor.execute("""
                        ALTER TABLE SCRAPPED_TEXT 
                        ADD WORD_COUNT NUMBER DEFAULT 0
                    """)
                    
                    connection.commit()
                    print("Added WORD_COUNT column to SCRAPPED_TEXT table")
                else:
                    print("WORD_COUNT column already exists in SCRAPPED_TEXT table")
                    
                return not column_exists  # Return True if added, False if already existed
                
    except Exception as e:
        print(f"Error adding word count field: {str(e)}")
        traceback.print_exc()
        return False    

def calculate_word_count(text):
    """
    Calculate the number of words in a text.
    
    Args:
        text: The text to count words in
        
    Returns:
        int: Number of words
    """
    # Remove excessive whitespace and split by whitespace
    words = text.strip().split()
    return len(words)

def continuous_processing_job(top_level_source_url, stop_event, delay_seconds=5, user_id=None, link_limit=None):
    """
    Worker function to continuously process links in the Links_to_scrap collection
    until the specified number of links are processed or all links are processed.
    
    Args:
        top_level_source_url: The top-level source URL
        stop_event: Event to signal stopping the process
        delay_seconds: Initial delay before starting processing (reduced from 60 to 5)
        user_id: Optional user ID
        link_limit: Maximum number of links to process (None = process all discovered links)
    """
    try:
        print(f"Starting processing job for {top_level_source_url} with {delay_seconds}s delay for user {user_id}")
        
        # Wait for the specified delay before starting processing
        print(f"Waiting {delay_seconds} seconds before starting processing...")
        
        # Wait either for the delay or until the stop event is set
        if stop_event.wait(delay_seconds):
            print(f"Processing job for {top_level_source_url} was stopped during delay")
            return
            
        print(f"Delay complete, beginning processing for {top_level_source_url} for user {user_id}")
        
        # Stats counters
        links_processed = 0
        success_count = 0
        error_count = 0
        consecutive_empty_cycles = 0
        max_consecutive_empty_cycles = 10  # Allow more empty cycles before exiting
        
        # Batch size for processing - process more links at once
        batch_size = 10  # Process more links per batch for better efficiency
        
        # Keep processing until explicitly stopped or limit reached
        while not stop_event.is_set():
            # Check if we've reached the link limit
            if link_limit is not None and links_processed >= link_limit:
                print(f"REACHED LINK LIMIT: Processed {links_processed}/{link_limit} links. Exiting processing job.")
                break
                
            try:
                # Create a new DB connection for each iteration to prevent connection timeouts
                with get_jsondb_connection() as connection:
                    with connection.cursor() as cursor:
                        # Find unprocessed links for this source URL
                        query = """
                            SELECT ID, LINK, NVL(DEPTH, 0) AS DEPTH, TOP_LEVEL_SOURCE, SOURCE_URL, USER_ID
                            FROM {0}
                            WHERE (IS_PROCESSED = 'false' OR IS_PROCESSED IS NULL)
                            AND TOP_LEVEL_SOURCE = :source_url
                        """.format(LINKS_TO_SCRAP_TABLE)
                        
                        # Add user_id filter if provided
                        params = {"source_url": top_level_source_url}
                        if user_id:
                            query += " AND USER_ID = :user_id"
                            params["user_id"] = user_id
                        
                        # Adjust batch size if we're near the link limit
                        if link_limit is not None:
                            current_batch_size = min(batch_size, link_limit - links_processed)
                            if current_batch_size <= 0:
                                break
                        else:
                            current_batch_size = batch_size
                            
                        # Add row limit
                        query += f" FETCH FIRST {current_batch_size} ROWS ONLY"
                        
                        # First, check the count to avoid unnecessary cursor creation
                        count_query = """
                            SELECT COUNT(*) FROM {0}
                            WHERE (IS_PROCESSED = 'false' OR IS_PROCESSED IS NULL)
                            AND TOP_LEVEL_SOURCE = :source_url
                        """.format(LINKS_TO_SCRAP_TABLE)
                        
                        if user_id:
                            count_query += " AND USER_ID = :user_id"
                        
                        cursor.execute(count_query, params)
                        unprocessed_count = cursor.fetchone()[0]
                        
                        if unprocessed_count == 0:
                            # No unprocessed links found, wait and check again
                            consecutive_empty_cycles += 1
                            print(f"No unprocessed links found. Empty cycle #{consecutive_empty_cycles}/{max_consecutive_empty_cycles}")
                            
                            if consecutive_empty_cycles >= max_consecutive_empty_cycles:
                                print(f"Reached maximum consecutive empty cycles ({max_consecutive_empty_cycles}). Exiting processing job.")
                                break
                            
                            # Wait for a short time before checking again - reduced from 5 seconds to 2
                            sleep_time = 2
                            print(f"Waiting {sleep_time} seconds before checking for new links...")
                            
                            # Use stop_event.wait instead of time.sleep to respond to stop events
                            if stop_event.wait(sleep_time):
                                print("Stop event detected during wait. Exiting processing job.")
                                break
                            
                            continue
                        
                        # Reset the consecutive empty cycles counter since we found links
                        consecutive_empty_cycles = 0
                        
                        # Get a batch of unprocessed links
                        cursor.execute(query, params)
                        unprocessed_links = cursor.fetchall()
                        print(f"Processing batch of {len(unprocessed_links)} links")
                        
                        batch_processed = 0
                        batch_success = 0
                        batch_errors = 0
                        
                        # Process each link in the batch
                        for link_row in unprocessed_links:
                            if stop_event.is_set():
                                print(f"Stop event triggered. Exiting processing job for {top_level_source_url}.")
                                break
                            
                            try:
                                link_id, link_url, depth, top_level, source, link_user_id = link_row
                                
                                # Ensure link has user_id before processing
                                effective_user_id = link_user_id or user_id
                                
                                if not effective_user_id:
                                    print(f"No user_id available for link: {link_url}, skipping")
                                    continue
                                    
                                if not link_user_id and user_id:
                                    # Update the document in the database to include user_id
                                    cursor.execute("""
                                        UPDATE {0} SET USER_ID = :user_id
                                        WHERE ID = :id
                                    """.format(LINKS_TO_SCRAP_TABLE), id=link_id, user_id=user_id)
                                    
                                    connection.commit()
                                
                                # Create a link_doc dictionary to match the interface
                                link_doc = {
                                    'id': link_id,
                                    'link': link_url,
                                    'depth': depth,
                                    'top_level_source': top_level,
                                    'source_url': source,
                                    'user_id': effective_user_id
                                }
        
                                # Process the link
                                result = scrape_single_link(connection, link_doc, effective_user_id)
                                
                                if result['status'] == 'success':
                                    batch_success += 1
                                    success_count += 1
                                else:
                                    # Already marked as failed in scrape_single_link
                                    batch_errors += 1
                                    error_count += 1
                                
                                batch_processed += 1
                                links_processed += 1
                                
                                # Check if we've reached the link limit
                                if link_limit is not None and links_processed >= link_limit:
                                    print(f"REACHED LINK LIMIT during batch: {links_processed}/{link_limit} links processed. Will exit after batch.")
                                    break
                                
                            except Exception as e:
                                # Catch any unexpected errors during processing
                                error_msg = f"Unexpected error processing link {link_url}: {str(e)}"
                                print(error_msg)
                                tb = traceback.format_exc()
                                traceback.print_exc()
                                
                                # Mark the link as failed
                                try:
                                    cursor.execute("""
                                        UPDATE {0} SET
                                            IS_PROCESSED = 'Failed',
                                            PROCESSED_AT = CURRENT_TIMESTAMP,
                                            ERROR = :error,
                                            TRACEBACK = :traceback,
                                            USER_ID = :user_id
                                        WHERE ID = :id
                                    """.format(LINKS_TO_SCRAP_TABLE),
                                        id=link_id,
                                        error=error_msg[:4000],
                                        traceback=tb[:4000],
                                        user_id=effective_user_id
                                    )
                                    connection.commit()
                                except Exception as update_error:
                                    print(f"Error updating link status: {str(update_error)}")
                                
                                batch_errors += 1
                                error_count += 1
                                batch_processed += 1
                                links_processed += 1
                                
                                # Check if we've reached the link limit
                                if link_limit is not None and links_processed >= link_limit:
                                    print(f"REACHED LINK LIMIT after error: {links_processed}/{link_limit} links processed. Will exit after batch.")
                                    break
                                    
                                # Continue processing the next link instead of breaking
                                continue
                            
                            # Much shorter sleep between links
                            time.sleep(0.1)
                        
                        print(f"Batch complete. Processed {batch_processed} links ({batch_success} successful, {batch_errors} errors).")
                        
                        # Check if we've reached the link limit
                        if link_limit is not None and links_processed >= link_limit:
                            print(f"REACHED LINK LIMIT: {links_processed}/{link_limit} links processed. Exiting processing job.")
                            break
                        
                        # If the batch was smaller than the batch size, take a very short break before checking again
                        if batch_processed < current_batch_size:
                            sleep_time = 1  # Reduced from 3 seconds to 1
                            print(f"Processed less than batch size. Waiting {sleep_time} seconds before continuing...")
                            if stop_event.wait(sleep_time):
                                print("Stop event detected during wait. Exiting processing job.")
                                break
                
            except Exception as batch_error:
                print(f"Error processing batch: {str(batch_error)}")
                traceback.print_exc()
                
                # Shorter sleep before retrying
                time.sleep(2)
                # IMPORTANT: Continue with the while loop instead of breaking
                continue
        
        print(f"Processing job completed or stopped: {links_processed} links processed, {success_count} successful, {error_count} failed")
        
        # Return stats if we exit the loop
        return {
            'links_processed': links_processed,
            'success_count': success_count,
            'error_count': error_count
        }
            
    except Exception as e:
        print(f"Error in continuous processing job: {str(e)}")
        traceback.print_exc()
        return {
            'error': str(e),
            'traceback': traceback.format_exc()
        }
    

def scrape_single_link(connection, link_doc, user_id=None):
    """Helper function to scrape a single link with word count tracking"""
    link = link_doc['link']
    print(f"Starting to scrape link: {link} for user: {user_id}")
    
    is_wiki = 'wikipedia.org' in link or 'wiki' in link.lower()
    
    # Get the top-level source and immediate source URLs
    top_level_source = link_doc.get('top_level_source', link_doc.get('source_url', 'unknown'))
    source_url = link_doc.get('source_url', 'unknown')
    
    # If user_id is not in the link_doc but is passed as a parameter, use the parameter
    if 'user_id' not in link_doc and user_id:
        link_doc_user_id = user_id
        print(f"Using passed user_id: {user_id}")
    else:
        link_doc_user_id = link_doc.get('user_id')
        print(f"Using link_doc user_id: {link_doc_user_id}")
        
    # Ensure we have a valid user_id
    if not link_doc_user_id and not user_id:
        error_msg = "No valid user_id found for link"
        print(f"Error: {error_msg}")
        return {
            'status': 'error',
            'link': link,
            'error': error_msg,
            'top_level_source': top_level_source
        }
    
    # Use the passed user_id if link_doc_user_id is not available
    if not link_doc_user_id:
        link_doc_user_id = user_id

    cursor = None
    try:
        cursor = connection.cursor()
        
        print(f"Got connection and cursor. Starting to scrape content from {link}")
        
        # Add user agent to avoid being blocked
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Make request to the URL with increased timeout
        print(f"Making HTTP request to: {link}")
        try:
            response = requests.get(link, headers=headers, timeout=60)
            response.raise_for_status()
            print(f"HTTP response status: {response.status_code}")
        except requests.exceptions.RequestException as req_error:
            print(f"Request error: {str(req_error)}")
            # Update the link record to mark it as processed with error
            try:
                cursor.execute(f"""
                    UPDATE {LINKS_TO_SCRAP_TABLE} SET
                        IS_PROCESSED = 'Failed',
                        PROCESSED_AT = CURRENT_TIMESTAMP,
                        ERROR = :error,
                        USER_ID = :user_id,
                        TOP_LEVEL_SOURCE = :top_level_source
                    WHERE ID = :id
                """,
                    id=link_doc['id'],
                    error=f"Request error: {str(req_error)}"[:4000],  # Limit for Oracle CLOB
                    user_id=link_doc_user_id,
                    top_level_source=top_level_source
                )
                connection.commit()
            except Exception as update_error:
                print(f"Error updating link status: {str(update_error)}")
                
            return {
                'status': 'error',
                'link': link,
                'error': f"Request error: {str(req_error)}",
                'top_level_source': top_level_source
            }
            # Don't raise the exception - return error status instead
        
        # Parse the HTML content
        print("Parsing HTML content")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Get title
        title = soup.find('title')
        title_text = title.get_text().strip() if title else "Unknown Title"
        print(f"Page title: {title_text}")
        
        # Extract text based on the site type
        if is_wiki:
            print("Wiki page detected, using specialized extraction")
            # For Wikipedia, focus on the content div
            content_div = soup.find('div', {'id': 'mw-content-text'})
            if content_div:
                # Remove unwanted elements
                for unwanted in content_div.select('.thumb, .navbox, .infobox, table'):
                    if unwanted:
                        unwanted.extract()
                
                # Extract text from paragraphs
                paragraphs = content_div.find_all(['p', 'h2', 'h3', 'h4', 'h5', 'h6'])
                text_parts = []
                
                for p in paragraphs:
                    text = p.get_text().strip()
                    if text:
                        if p.name.startswith('h'):
                            text_parts.append(f"\n## {text}\n")
                        else:
                            text_parts.append(text)
                
                text = "\n\n".join(text_parts)
                text = f"# {title_text}\n\n{text}"
            else:
                print("Wiki content div not found, falling back to standard extraction")
                # Fallback to standard extraction
                for script in soup(["script", "style"]):
                    script.extract()
                text = soup.get_text(separator=' ', strip=True)
        else:
            print("Standard page extraction")
            # Standard extraction for non-Wikipedia sites
            for script in soup(["script", "style"]):
                script.extract()
            
            # Get text and clean it
            text = soup.get_text(separator=' ', strip=True)
            
            # Add title to the beginning
            text = f"# {title_text}\n\n{text}"
        
        # Remove excessive whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text)
        
        # Calculate word count
        word_count = calculate_word_count(text)
        
        print(f"Extracted text length: {len(text)} characters, {word_count} words")
        
        # Check if content already exists to avoid duplicates
        cursor.execute(f"""
            SELECT COUNT(*) FROM {SCRAPPED_TEXT_TABLE}
            WHERE CONTENT_LINK = :link AND USER_ID = :user_id
        """, link=link, user_id=link_doc_user_id)
        
        if cursor.fetchone()[0] > 0:
            print(f"Content already exists for {link}, skipping insertion")
            
            # Get the content ID
            cursor.execute(f"""
                SELECT ID FROM {SCRAPPED_TEXT_TABLE}
                WHERE CONTENT_LINK = :link AND USER_ID = :user_id
            """, link=link, user_id=link_doc_user_id)
            
            content_id = cursor.fetchone()[0]
            
            # Update the word count
            cursor.execute(f"""
                UPDATE {SCRAPPED_TEXT_TABLE} 
                SET WORD_COUNT = :word_count
                WHERE ID = :id
            """, word_count=word_count, id=content_id)
            
            connection.commit()
            print(f"Updated word count ({word_count}) for existing content ID: {content_id}")
        else:
            # Insert into content collection
            print(f"Inserting content into database for {link}")
            try:
                # Create a variable to hold the returned ID
                content_id_var = cursor.var(int)
                
                cursor.execute(f"""
                    INSERT INTO {SCRAPPED_TEXT_TABLE} (
                        SCRAPPED_CONTENT, CONTENT_LINK, SCRAPE_DATE, LINK_ID,
                        SOURCE_URL, TOP_LEVEL_SOURCE, DEPTH, TITLE, USER_ID, WORD_COUNT
                    ) VALUES (
                        :content, :link, CURRENT_TIMESTAMP, :link_id,
                        :source_url, :top_level_source, :depth, :title, :user_id, :word_count
                    ) RETURNING ID INTO :content_id
                """,
                    content=text,
                    link=link,
                    link_id=link_doc['id'],
                    source_url=source_url,
                    top_level_source=top_level_source,
                    depth=link_doc.get('depth', 0),
                    title=title_text[:1000],  # Limit to column size
                    user_id=link_doc_user_id,
                    word_count=word_count,
                    content_id=content_id_var
                )

                content_id = content_id_var.getvalue()[0]
                connection.commit()
                print(f"Content inserted with ID: {content_id}, word count: {word_count}")
                
            except Exception as db_error:
                print(f"Database error inserting content: {str(db_error)}")
                traceback.print_exc()
                # Don't raise, just return error status
                return {
                    'status': 'error',
                    'link': link,
                    'error': f"Database error: {str(db_error)}",
                    'top_level_source': top_level_source
                }
        
        # Update the link as processed
        print(f"Updating link status to processed for {link}")
        try:
            cursor.execute(f"""
                UPDATE {LINKS_TO_SCRAP_TABLE} SET
                    IS_PROCESSED = 'true',
                    PROCESSED_AT = CURRENT_TIMESTAMP,
                    TOP_LEVEL_SOURCE = :top_level_source,
                    USER_ID = :user_id
                WHERE ID = :id
            """,
                id=link_doc['id'],
                top_level_source=top_level_source,
                user_id=link_doc_user_id
            )
            
            connection.commit()
            print(f"Link status update result: Link marked as processed")
        except Exception as update_error:
            print(f"Database error updating link status: {str(update_error)}")
            traceback.print_exc()
            # Don't raise, just return error status
            return {
                'status': 'error',
                'link': link,
                'error': f"Status update error: {str(update_error)}",
                'top_level_source': top_level_source
            }
        
        print(f"Successfully processed {link}")
        return {
            'status': 'success',
            'link': link,
            'content_length': len(text),
            'word_count': word_count,
            'title': title_text,
            'content_id': str(content_id),
            'top_level_source': top_level_source,
            'source_url': source_url
        }
    
    except requests.exceptions.RequestException as e:
        error_msg = f"Request error: {str(e)}"
        print(f"Failed to scrape {link}: {error_msg}")
        traceback.print_exc()
        
        # Update the link as failed
        try:
            if cursor:
                cursor.execute(f"""
                    UPDATE {LINKS_TO_SCRAP_TABLE} SET
                        IS_PROCESSED = 'Failed',
                        PROCESSED_AT = CURRENT_TIMESTAMP,
                        ERROR = :error,
                        USER_ID = :user_id,
                        TOP_LEVEL_SOURCE = :top_level_source
                    WHERE ID = :id
                """,
                    id=link_doc['id'],
                    error=error_msg[:4000],  # Limit for Oracle CLOB
                    user_id=link_doc_user_id,
                    top_level_source=top_level_source
                )
                connection.commit()
        except Exception as update_error:
            print(f"Error updating link status: {str(update_error)}")
        
        return {
            'status': 'error',
            'link': link,
            'error': error_msg,
            'top_level_source': top_level_source
        }
    
    except Exception as e:
        error_msg = f"Processing error: {str(e)}"
        tb = traceback.format_exc()
        print(f"Failed to scrape {link}: {error_msg}")
        print(tb)
        
        # Update the link as failed
        try:
            if cursor:
                cursor.execute(f"""
                    UPDATE {LINKS_TO_SCRAP_TABLE} SET
                        IS_PROCESSED = 'Failed',
                        PROCESSED_AT = CURRENT_TIMESTAMP,
                        ERROR = :error,
                        TRACEBACK = :traceback,
                        USER_ID = :user_id,
                        TOP_LEVEL_SOURCE = :top_level_source
                    WHERE ID = :id
                """,
                    id=link_doc['id'],
                    error=error_msg[:4000],  # Limit for Oracle CLOB
                    traceback=tb[:4000],     # Limit for Oracle CLOB
                    user_id=link_doc_user_id,
                    top_level_source=top_level_source
                )
                connection.commit()
        except Exception as update_error:
            print(f"Error updating link status: {str(update_error)}")
        
        return {
            'status': 'error',
            'link': link,
            'error': error_msg,
            'traceback': tb,
            'top_level_source': top_level_source
        }
    finally:
        if cursor:
            cursor.close()

def shutdown_application():
    """
    Clean shutdown procedure to stop all background threads
    Called when the application is exiting
    """
    print("Application shutdown initiated, stopping all background threads...")
    
    # Stop all vectorization threads using the consolidated manager
    for key, event in list(vectorization_manager['events'].items()):
        print(f"Setting stop event for vectorization thread {key}")
        event.set()
    
    # Also check legacy tracking if it exists
    if hasattr(start_crawling_and_processing, 'vectorization_events'):
        for key, event in list(start_crawling_and_processing.vectorization_events.items()):
            print(f"Setting stop event for legacy vectorization thread {key}")
            event.set()
    
    # Stop all crawling threads
    for key, event in list(crawling_events.items()):
        print(f"Setting stop event for crawling thread {key}")
        event.set()
    
    # Stop all processing threads
    for key, event in list(processing_events.items()):
        print(f"Setting stop event for processing thread {key}")
        event.set()
    
    # Allow time for threads to terminate gracefully
    print("Waiting for threads to terminate...")
    time.sleep(3)
    
    # Final cleanup
    # Clear all tracking dictionaries
    vectorization_manager['events'].clear()
    vectorization_manager['threads'].clear()
    vectorization_manager['status'].clear()
    crawling_events.clear()
    processing_events.clear()
    active_user_jobs.clear()
    
    print("Application shutdown complete")

def clean_vectorization_threads(max_age_minutes=15):
    """
    Clean up completed or stalled vectorization threads
    Called periodically by maintenance worker
    
    Args:
        max_age_minutes: Maximum age in minutes for a thread to be considered stalled
        
    Returns:
        int: Number of threads cleaned up
    """
    cleaned_count = 0
    
    try:
        print(f"Checking for completed or stalled vectorization threads...")
        
        # Check each thread in new tracking system
        for key, thread in list(vectorization_manager['threads'].items()):
            # Check if thread is still alive
            if not thread.is_alive():
                # Thread is no longer running, clean up
                print(f"Cleaning up completed vectorization thread for {key}")
                
                # Set event to ensure thread cleans up resources
                if key in vectorization_manager['events']:
                    vectorization_manager['events'][key].set()
                    del vectorization_manager['events'][key]
                    
                # Remove thread from tracking
                del vectorization_manager['threads'][key]
                
                # Update status
                vectorization_manager['status'][key] = 'completed'
                
                cleaned_count += 1
        
        # Also clean legacy tracking if it exists
        if hasattr(start_crawling_and_processing, 'vectorization_threads'):
            for key, thread in list(start_crawling_and_processing.vectorization_threads.items()):
                if not thread.is_alive():
                    # Thread is completed, remove it
                    del start_crawling_and_processing.vectorization_threads[key]
                    print(f"Cleaned up legacy completed vectorization thread reference for {key}")
                    
                    # Also clean up the event
                    if (hasattr(start_crawling_and_processing, 'vectorization_events') and 
                        key in start_crawling_and_processing.vectorization_events):
                        del start_crawling_and_processing.vectorization_events[key]
                        print(f"Cleaned up legacy vectorization event for {key}")
                        
                    cleaned_count += 1
                    
        return cleaned_count
        
    except Exception as e:
        print(f"Error cleaning vectorization threads: {str(e)}")
        traceback.print_exc()
        return 0

@file_api.route('/recursive-crawl', methods=['POST'])
@token_required
def recursive_crawl(user_id, *args, **kwargs):  # Allow additional args
    start_time = time.time()
    try:
        print(f"Starting recursive crawl for user ID: {user_id}")
        
        # Get the URL and optional link limit from the request body
        data = request.get_json()
        print(f"Request data: {data}")
        
        if not data or 'url' not in data:
            print("URL is missing in request body")
            return standardize_error_response('URL is required in the POST body.', 'MISSING_URL', 400, 
                                              process_time=f"{time.time() - start_time:.2f}s")
        
        # Get the top-level source URL provided by the user
        top_level_source_url = data['url']
        
        # Get the link limit with validation
        link_limit = data.get('limit', 10)  # Default to 10 if not provided
        
        # Validate link_limit is an integer and within allowed range
        try:
            link_limit = int(link_limit)
            if link_limit < 0:
                link_limit = 0  # No limit
            elif link_limit > 5000:
                link_limit = 5000  # Max allowed
        except (ValueError, TypeError):
            print(f"Invalid link limit: {link_limit}, using default of 10")
            link_limit = 10  # Default to 10 if invalid
            
        # Get auto_vectorize setting with default
        auto_vectorize = data.get('auto_vectorize', True)
            
        print(f"Top level source URL: {top_level_source_url}, Link limit: {link_limit}, Auto vectorize: {auto_vectorize}")
        
        # Validate URL format
        if not is_valid_url(top_level_source_url):
            print(f"Invalid URL format: {top_level_source_url}")
            return standardize_error_response(f'Invalid URL format: {top_level_source_url}', 'INVALID_URL', 400,
                                             process_time=f"{time.time() - start_time:.2f}s")
        
        # Use connection pooling with context manager
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Check if the URL has already been crawled for this user
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                """.format(LINKS_TO_SCRAP_TABLE), source_url=top_level_source_url, user_id=user_id)
                
                existing_links_count = cursor.fetchone()[0]
                print(f"Found {existing_links_count} existing links for this URL and user")
                
                # For recrawling, we should make sure the initial URL is crawlable
                if existing_links_count > 0:
                    # If this is a recrawl, reset the main URL to be crawled again
                    cursor.execute("""
                        UPDATE {0} SET
                            IS_CRAWLED = 0,
                            CRAWLING_STARTED = NULL,
                            CRAWLED_AT = NULL,
                            ERROR = NULL
                        WHERE LINK = :link AND USER_ID = :user_id
                    """.format(LINKS_TO_SCRAP_TABLE),
                       link=top_level_source_url, user_id=user_id)
                    
                    connection.commit()
                    print(f"Reset crawl status for main URL: {top_level_source_url}")
                
                # Add new entry to SOURCE_URLS or update existing with new limit
                try:
                    print(f"Updating source URL information for {top_level_source_url}")
                    
                    # First check if the document already exists
                    cursor.execute("""
                        SELECT COUNT(*) FROM {0}
                        WHERE SOURCE_URL = :source_url AND USER_ID = :user_id
                    """.format(SOURCE_URLS_TABLE), source_url=top_level_source_url, user_id=user_id)
                    
                    existing_source_count = cursor.fetchone()[0]
                    
                    if existing_source_count > 0:
                        # If exists, update the timestamp and link limit
                        cursor.execute("""
                            UPDATE {0} SET 
                                TIMESTAMP = CURRENT_TIMESTAMP, 
                                PAGE_LIMIT = :link_limit,
                                AUTO_VECTORIZE = :auto_vectorize
                            WHERE SOURCE_URL = :source_url AND USER_ID = :user_id
                        """.format(SOURCE_URLS_TABLE),
                            link_limit=link_limit,
                            auto_vectorize=1 if auto_vectorize else 0,
                            source_url=top_level_source_url,
                            user_id=user_id
                        )
                        print(f"Updated timestamp, link limit, and auto_vectorize for existing source URL record")
                    else:
                        # If not exists, create new entry
                        cursor.execute("""
                            INSERT INTO {0} (
                                SOURCE_URL, USER_ID, TIMESTAMP, PAGE_LIMIT, AUTO_VECTORIZE
                            ) VALUES (
                                :source_url, :user_id, CURRENT_TIMESTAMP, :link_limit, :auto_vectorize
                            )
                        """.format(SOURCE_URLS_TABLE),
                            source_url=top_level_source_url,
                            user_id=user_id,
                            link_limit=link_limit,
                            auto_vectorize=1 if auto_vectorize else 0
                        )
                        print(f"Created new source URL record with auto_vectorize={auto_vectorize}")
                        
                    connection.commit()
                        
                except Exception as e:
                    print(f"Error managing source URL: {str(e)}")
                    traceback.print_exc()
                    raise
                
                # Check if the URL exists in Links_to_scrap
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE LINK = :link AND USER_ID = :user_id
                """.format(LINKS_TO_SCRAP_TABLE), link=top_level_source_url, user_id=user_id)
                
                existing_link_count = cursor.fetchone()[0]
                print(f"Existing entry for initial URL found: {existing_link_count > 0}")
                
                # If the URL is not in Links_to_scrap, add it as a new starting point
                if existing_link_count == 0:
                    try:
                        print(f"Inserting initial URL to Links_to_scrap: {top_level_source_url}")
                        cursor.execute("""
                            INSERT INTO {0} (
                                LINK, ADDED_AT, IS_CRAWLED, IS_PROCESSED, 
                                DEPTH, SOURCE_URL, TOP_LEVEL_SOURCE, USER_ID
                            ) VALUES (
                                :link, CURRENT_TIMESTAMP, 0, 'false',
                                0, :source_url, :top_level_source, :user_id
                            )
                        """.format(LINKS_TO_SCRAP_TABLE),
                            link=top_level_source_url,
                            source_url=top_level_source_url,
                            top_level_source=top_level_source_url,
                            user_id=user_id
                        )
                        
                        connection.commit()
                        print(f"URL inserted successfully: {top_level_source_url}")
                        
                    except Exception as e:
                        print(f"Error inserting URL: {str(e)}")
                        traceback.print_exc()
                        raise
                
                # Perform immediate initial crawl to fetch some initial links
                initial_links = []
                try:
                    # Calculate how many new links we need to reach the limit
                    links_needed = link_limit - existing_links_count if link_limit > 0 else 50
                    initial_crawl_limit = max(0, links_needed)
                    
                    print(f"Starting initial crawl with limit of {initial_crawl_limit} new links")
                    
                    # Use thread pool for parallel processing
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(
                            perform_initial_crawl,
                            top_level_source_url,
                            user_id,
                            initial_crawl_limit
                        )
                        
                        # Set a timeout to ensure we don't block too long
                        try:
                            initial_links = future.result(timeout=5)  # 5-second timeout
                        except concurrent.futures.TimeoutError:
                            print("Initial crawl timed out, continuing with normal process")
                            initial_links = []
                except Exception as initial_error:
                    print(f"Initial crawl attempt encountered error: {str(initial_error)}")
                    # Continue with normal process even if initial crawl fails
                
        # Check if user has an active job
        if user_has_active_job(user_id):
            print(f"User {user_id} already has an active job. Adding URL to queue: {top_level_source_url}")
            
            # Add the URL to the queue with link limit and auto_vectorize
            queue_doc = {
                'page_limit': link_limit,  # Keep the same parameter name for compatibility
                'auto_vectorize': auto_vectorize
            }
            queue_result = add_to_queue(user_id, top_level_source_url, queue_doc)
            
            process_time = time.time() - start_time
            if queue_result:
                return jsonify({
                    'status': 'queued',
                    'message': f'URL {top_level_source_url} has been added to the processing queue with a limit of {link_limit} links.',
                    'initial_links_found': len(initial_links) if 'initial_links' in locals() else 0,
                    'existing_links': existing_links_count if 'existing_links_count' in locals() else 0,
                    'timestamp': datetime.now().isoformat(),
                    'source_url': top_level_source_url,
                    'link_limit': link_limit,
                    'auto_vectorize': auto_vectorize,
                    'process_time': f"{process_time:.2f}s"
                })
            else:
                return standardize_error_response(
                    f'Failed to add URL {top_level_source_url} to the queue.', 
                    'QUEUE_ERROR', 
                    500,
                    process_time=f"{process_time:.2f}s"
                )
        else:
            print(f"No active job for user {user_id}. Starting crawling for: {top_level_source_url} with limit: {link_limit}")
            
            # Start crawling and processing with the specified link limit and auto_vectorize
            result = start_crawling_and_processing(user_id, top_level_source_url, link_limit, auto_vectorize)
            
            process_time = time.time() - start_time
            if result:
                return jsonify({
                    'status': 'success',
                    'message': f'Continuous crawling started for {top_level_source_url} with a limit of {link_limit} links',
                    'initial_links_found': len(initial_links) if 'initial_links' in locals() else 0,
                    'existing_links': existing_links_count if 'existing_links_count' in locals() else 0,
                    'timestamp': datetime.now().isoformat(),
                    'source_url': top_level_source_url,
                    'link_limit': link_limit,
                    'auto_vectorize': auto_vectorize,
                    'process_time': f"{process_time:.2f}s"
                })
            else:
                return standardize_error_response(
                    f'Failed to start crawling for {top_level_source_url}', 
                    'CRAWL_ERROR', 
                    500,
                    process_time=f"{process_time:.2f}s"
                )
    
    except Exception as e:
        traceback_str = traceback.format_exc()
        print(f"Error in crawling: {str(e)}\n{traceback_str}")
        return standardize_error_response(
            str(e), 
            'SERVER_ERROR', 
            500,
            process_time=f"{time.time() - start_time:.2f}s"
        )

def perform_initial_crawl(url, user_id, link_limit=10):
    """
    Perform a quick initial crawl to fetch some initial links for better responsiveness,
    respecting the link limit provided by the user.
    
    Args:
        url: URL to crawl initially
        user_id: User ID for database operations
        link_limit: Maximum number of links to return (default: 10)
        
    Returns:
        list: List of initially found URLs
    """
    try:
        # Don't add more links than specified in the link_limit
        actual_limit = link_limit if link_limit > 0 else 50
        
        if actual_limit <= 0:
            print(f"No additional links needed (limit is {actual_limit}), skipping initial crawl")
            return []
        
        # Get already crawled links for filtering
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT LINK FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                """.format(LINKS_TO_SCRAP_TABLE), 
                   source_url=url, user_id=user_id)
                
                existing_links = set(row[0] for row in cursor.fetchall())
                print(f"Found {len(existing_links)} existing links to avoid duplicates")
        
        initial_links = []
        # Add user agent to avoid being blocked
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Make immediate request to the URL with short timeout
        print(f"Making immediate HTTP request to: {url}")
        
        try:
            response = requests.get(url, headers=headers, timeout=3)
            response.raise_for_status()
        except requests.exceptions.RequestException as req_error:
            print(f"Error in initial crawl request: {str(req_error)}")
            return []
        
        if response.status_code == 200:
            # Check content type before parsing
            content_type = response.headers.get('Content-Type', '').lower()
            if not ('text/html' in content_type or 'application/xhtml+xml' in content_type):
                print(f"Skipping non-HTML content type in initial crawl: {content_type}")
                return []
            
            # Extract the domain for same-domain filtering
            original_domain = extract_domain(url)
            
            # Get immediate links from the page
            soup = BeautifulSoup(response.text, 'html.parser')
            all_links = soup.find_all('a', href=True)
            
            # Quick extract of valid links
            for link in all_links:
                href = link['href'].strip()
                if not href or href.startswith(('javascript:', 'mailto:', 'tel:', '#')):
                    continue
                    
                try:
                    full_url = urljoin(url, href)
                    link_domain = extract_domain(full_url)
                    
                    if link_domain != original_domain:
                        continue
                        
                    if (not is_valid_url(full_url) or 
                        not is_valid_content_url(full_url) or 
                        is_problematic_url(full_url) or
                        full_url in existing_links):  # Skip already existing links
                        continue
                        
                    initial_links.append(full_url)
                except:
                    continue
                    
            initial_links = list(set(initial_links))
            print(f"Quick initial crawl found {len(initial_links)} new links")
            
            # Strictly limit the initial links to respect the user's link limit
            initial_links_to_process = initial_links[:actual_limit]
            print(f"Using {len(initial_links_to_process)} links (strictly limited by user's link limit: {actual_limit})")
            
            # Insert these links into the database in a batch
            if initial_links_to_process:
                with get_jsondb_connection() as conn:
                    values_list = []
                    for link in initial_links_to_process:
                        values_list.append({
                            'link': link,
                            'top_level_source': url,
                            'user_id': user_id,
                            'source_url': url,
                            'depth': 1,
                            'has_text': 1 if contains_text_in_url(link) else 0
                        })
                    
                    # Use batch insert for better performance
                    if values_list:
                        inserted_count = batch_insert_links(conn, values_list)
                        print(f"Initially inserted {inserted_count} links during quick crawl (strictly respecting link limit of {actual_limit})")
        else:
            print(f"Initial crawl request failed with status code: {response.status_code}")
            
        return initial_links
    except Exception as e:
        print(f"Error in initial crawl: {str(e)}")
        traceback.print_exc()
        return []  
      
@file_api.route('/progress-bar', methods=['GET'])
@token_required
def get_progress_bar(user_id):
    """
    Get the progress information for crawling and scraping operations
    to display in a progress bar in the frontend.
    """
    try:
        # Get source URL from query parameters
        source_url = request.args.get('source_url')

        if not source_url:
            return jsonify({
                'status': 'error',
                'message': 'source_url is required.',
                'timestamp': datetime.now().isoformat()
            }), 400

        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Verify that the source URL belongs to the authenticated user
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE SOURCE_URL = :source_url AND USER_ID = :user_id
                """.format(SOURCE_URLS_TABLE), 
                   source_url=source_url, user_id=user_id)
                
                source_url_count = cursor.fetchone()[0]
                
                if source_url_count == 0:
                    return jsonify({
                        'status': 'error',
                        'message': 'Source URL not found or not authorized.',
                        'timestamp': datetime.now().isoformat()
                    }), 403

                # Check if this URL is in the queue or active
                cursor.execute("""
                    SELECT 
                        PROCESSED, 
                        PROCESSING_STARTED, 
                        PAGE_LIMIT
                    FROM {0}
                    WHERE USER_ID = :user_id AND SOURCE_URL = :source_url
                """.format(PROCESSING_QUEUE_TABLE), 
                   user_id=user_id, source_url=source_url)
                
                queue_row = cursor.fetchone()
                
                # Determine queue and active status
                in_queue = False
                is_active = False
                link_limit = None
                
                if queue_row:
                    processed, processing_started, link_limit = queue_row
                    
                    # Check if it's in an active job
                    if user_id in active_user_jobs and source_url in active_user_jobs.get(user_id, set()):
                        is_active = True
                    
                    # Check if it's in the queue but not processed
                    if processed == 0 and processing_started is None:
                        in_queue = True

                # Get total links for this source URL
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                """.format(LINKS_TO_SCRAP_TABLE), source_url=source_url, user_id=user_id)
                
                total_links = cursor.fetchone()[0]

                # Skip calculation if no links found
                if total_links == 0:
                    current_time = datetime.now()
                    return jsonify({
                        'status': 'pending',
                        'message': f'No links found for {source_url}',
                        'crawl_progress': 0,
                        'scrape_progress': 0,
                        'crawled_count': 0,
                        'total_links': 0,
                        'scraped_count': 0,
                        'skipped_count': 0,
                        'is_active': is_active,
                        'in_queue': in_queue,
                        'link_limit': link_limit,
                        'timestamp': current_time.isoformat()
                    })

                # Calculate crawling progress
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id AND IS_CRAWLED = 1
                """.format(LINKS_TO_SCRAP_TABLE), source_url=source_url, user_id=user_id)
                
                crawled_count = cursor.fetchone()[0]
                
                # Get count of skipped URLs
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id AND SKIPPED = 1
                """.format(LINKS_TO_SCRAP_TABLE), source_url=source_url, user_id=user_id)
                
                skipped_count = cursor.fetchone()[0]
                
                # Adjust the total for progress calculation (exclude skipped URLs)
                adjusted_total = total_links - skipped_count
                if adjusted_total <= 0:
                    adjusted_total = 1  # Prevent division by zero
                
                crawl_progress = round((crawled_count / adjusted_total) * 100, 1) if adjusted_total > 0 else 0

                # Calculate scraping progress based on is_processed field
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id AND IS_PROCESSED = 'true'
                """.format(LINKS_TO_SCRAP_TABLE), source_url=source_url, user_id=user_id)
                
                scraped_count = cursor.fetchone()[0]
                scrape_progress = round((scraped_count / adjusted_total) * 100, 1) if adjusted_total > 0 else 0

                # Determine the overall status
                if is_active:
                    if crawled_count < adjusted_total:
                        status = 'crawling'
                    else:
                        status = 'processing'
                elif in_queue:
                    status = 'queued'
                elif crawl_progress >= 100 and scrape_progress >= 100:
                    status = 'completed'
                else:
                    status = 'pending'

                # Add current progress to history
                cursor.execute("""
                    INSERT INTO {0} (
                        SOURCE_URL, TIMESTAMP, CRAWL_PROGRESS, SCRAPE_PROGRESS,
                        CRAWLED_COUNT, SCRAPED_COUNT, TOTAL_LINKS, OPERATION_STATUS
                    ) VALUES (
                        :source_url, CURRENT_TIMESTAMP, :crawl_progress, :scrape_progress,
                        :crawled_count, :scraped_count, :total_links, :operation_status
                    )
                """.format('PROGRESS_HISTORY'),
                    source_url=source_url,
                    crawl_progress=crawl_progress,
                    scrape_progress=scrape_progress,
                    crawled_count=crawled_count,
                    scraped_count=scraped_count,
                    total_links=total_links,
                    operation_status=status
                )
                connection.commit()
                
                # Return the response
                return jsonify({
                    'status': 'success',
                    'operation_status': status,
                    'crawl_progress': crawl_progress,
                    'scrape_progress': scrape_progress,
                    'crawled_count': crawled_count,
                    'total_links': total_links,
                    'scraped_count': scraped_count,
                    'skipped_count': skipped_count,
                    'adjusted_total': adjusted_total,  # Total minus skipped URLs
                    'is_active': is_active,
                    'in_queue': in_queue,
                    'link_limit': link_limit,
                    'timestamp': datetime.now().isoformat()
                })

    except Exception as e:
        print(f"Error in progress_bar: {str(e)}")
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e),
            'error_details': traceback.format_exc(),
            'timestamp': datetime.now().isoformat()
        }), 500

@file_api.route('/process-all-links', methods=['POST'])
@token_required
def process_all_links(user_id):
    """
    Start a background thread to continuously process all links in the Links_to_scrap collection
    until all links are processed (is_processed: true).
    Includes an option to delay the start of processing.
    If a job is already active for the user, the new URL will be queued.
    """
    try:
        print(f"Starting process_all_links for user ID: {user_id}")
        
        # Get the delay parameter (default: 60 seconds) and source_url
        data = request.get_json() or {}
        delay_seconds = data.get('delay', 5)  # Reduced from 60 to 5 seconds for better performance
        source_url = data.get('source_url')
        
        print(f"Request data: delay={delay_seconds}, source_url={source_url}")
        
        if not source_url:
            print("source_url is missing in request body")
            return jsonify({
                'status': 'error',
                'message': 'source_url is required in the POST body.',
                'timestamp': datetime.now().isoformat()
            }), 400

        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Check how many unprocessed links exist for this user and source
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE (IS_PROCESSED = 'false' OR IS_PROCESSED IS NULL)
                    AND TOP_LEVEL_SOURCE = :source_url
                    AND USER_ID = :user_id
                """.format(LINKS_TO_SCRAP_TABLE), source_url=source_url, user_id=user_id)
                
                unprocessed_count = cursor.fetchone()[0]
                
                print(f"Found {unprocessed_count} unprocessed links for source: {source_url}, user: {user_id}")
                
                if unprocessed_count == 0:
                    print(f"No unprocessed links found for {source_url}")
                    return jsonify({
                        'status': 'complete',
                        'message': f'No unprocessed links found for {source_url}',
                        'timestamp': datetime.now().isoformat()
                    })
        
        # Check if user has an active job
        if user_has_active_job(user_id):
            print(f"User {user_id} already has an active job. Adding URL to queue: {source_url}")
            
            # Add the URL to the queue
            queue_result = add_to_queue(user_id, source_url)
            
            if queue_result:
                return jsonify({
                    'status': 'queued',
                    'message': f'URL {source_url} has been added to the processing queue.',
                    'timestamp': datetime.now().isoformat(),
                    'source_url': source_url
                })
            else:
                return jsonify({
                    'status': 'error',
                    'message': f'Failed to add URL {source_url} to the queue.',
                    'timestamp': datetime.now().isoformat()
                }), 500
        else:
            print(f"No active job for user {user_id}. Starting processing for: {source_url} with delay: {delay_seconds}s")
            
            # Start processing with the specified delay
            result = start_crawling_and_processing(user_id, source_url)
            
            if result:
                return jsonify({
                    'status': 'success',
                    'message': f'Continuous processing started for {source_url} with a delay of {delay_seconds} seconds',
                    'timestamp': datetime.now().isoformat(),
                    'source_url': source_url
                })
            else:
                return jsonify({
                    'status': 'error',
                    'message': f'Failed to start processing for {source_url}',
                    'timestamp': datetime.now().isoformat()
                }), 500
    
    except Exception as e:
        traceback_str = traceback.format_exc()
        print(f"Error in processing: {str(e)}\n{traceback_str}")
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': traceback_str,
            'timestamp': datetime.now().isoformat()
        }), 500
    
@file_api.route('/source-url-status', methods=['GET'])
@token_required
def get_source_url_status(user_id):
    """Get the status of a specific source URL for the authenticated user"""
    start_time = time.time()
    try:
        print(f"Getting source URL status for user: {user_id}")
        
        # Get source_url from request args
        source_url = request.args.get('source_url')
            
        if not source_url:
            print("source_url parameter is missing")
            return jsonify({
                'status': 'error',
                'message': 'source_url is required.',
                'timestamp': datetime.now().isoformat(),
                'process_time': f"{time.time() - start_time:.2f}s"
            }), 400
        
        print(f"Checking status for source URL: {source_url}")
        
        # Use connection pool with context manager for better performance
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Count total URLs associated with this source for this user
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                """.format(LINKS_TO_SCRAP_TABLE), source_url=source_url, user_id=user_id)
                
                total_urls = cursor.fetchone()[0]
                
                # Count successfully processed URLs for this source
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url 
                    AND IS_PROCESSED = 'true'
                    AND USER_ID = :user_id
                """.format(LINKS_TO_SCRAP_TABLE), source_url=source_url, user_id=user_id)
                
                successful_processed = cursor.fetchone()[0]
                
                # Count failed processed URLs for this source
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url 
                    AND IS_PROCESSED = 'Failed'
                    AND USER_ID = :user_id
                """.format(LINKS_TO_SCRAP_TABLE), source_url=source_url, user_id=user_id)
                
                failed_processed = cursor.fetchone()[0]
                
                # Calculate total processed (successful + failed)
                total_processed = successful_processed + failed_processed
                
                # Count scraped URLs for this source
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                """.format(SCRAPPED_TEXT_TABLE), source_url=source_url, user_id=user_id)
                
                total_scrapped = cursor.fetchone()[0]
                
                print(f"Stats: Total URLs: {total_urls}, Successful: {successful_processed}, Failed: {failed_processed}, Total Processed: {total_processed}, Scrapped: {total_scrapped}")
                
                # Check if URL is in the queue
                cursor.execute("""
                    SELECT COUNT(*), MIN(PROCESSED), MIN(PROCESSING_STARTED)
                    FROM {0}
                    WHERE USER_ID = :user_id AND SOURCE_URL = :source_url
                """.format(PROCESSING_QUEUE_TABLE), user_id=user_id, source_url=source_url)
                
                queue_count, is_processed, is_processing = cursor.fetchone()
                queue_item_exists = queue_count > 0
                
                # Determine the status
                if total_urls == 0:
                    # If no URLs are found for this source, it's pending
                    status = 'Pending'
                elif total_processed == total_urls:
                    # Only mark as "Completed" if all URLs are processed (either successfully or failed)
                    status = 'Completed'
                else:
                    # Check if it's actively being processed
                    is_active = False
                    if user_id in active_user_jobs and source_url in active_user_jobs[user_id]:
                        is_active = True
                        status = 'In Progress'
                    # Check if it's in the queue but not active
                    elif queue_item_exists and is_processed == 0 and is_processing is None:
                        status = 'Queued'
                    else:
                        # Otherwise, it's still pending
                        status = 'Pending'
                
                print(f"Source status determined as: {status}")
                
                # Get queue position if applicable
                queue_position = None
                if status == 'Queued' and queue_item_exists:
                    # Count how many unprocessed items are ahead in the queue
                    cursor.execute("""
                        SELECT COUNT(*) FROM {0} q1
                        WHERE q1.USER_ID = :user_id
                          AND q1.PROCESSED = 0
                          AND q1.PROCESSING_STARTED IS NULL
                          AND q1.ADDED_AT < (
                              SELECT q2.ADDED_AT FROM {0} q2
                              WHERE q2.USER_ID = :user_id 
                                AND q2.SOURCE_URL = :source_url
                                AND q2.PROCESSED = 0
                                AND q2.PROCESSING_STARTED IS NULL
                          )
                    """.format(PROCESSING_QUEUE_TABLE), user_id=user_id, source_url=source_url)
                    
                    ahead_count = cursor.fetchone()[0]
                    queue_position = ahead_count + 1  # Add 1 for human-readable position (1-based indexing)
                
                # Get link limit for this source
                cursor.execute("""
                    SELECT PAGE_LIMIT FROM {0}
                    WHERE USER_ID = :user_id AND SOURCE_URL = :source_url
                """.format(SOURCE_URLS_TABLE), user_id=user_id, source_url=source_url)
                
                row = cursor.fetchone()
                link_limit = row[0] if row else None
                
                # Calculate progress percentages
                crawl_progress = round((total_processed / total_urls * 100), 1) if total_urls > 0 else 0
                scrape_progress = round((total_scrapped / total_urls * 100), 1) if total_urls > 0 else 0
                
                # Calculate process time for performance monitoring
                process_time = time.time() - start_time
                
        return jsonify({
            'status': 'success',
            'source_url': source_url,
            'data': {
                'status': status,
                'total_urls': total_urls,
                'successful_processed': successful_processed,
                'failed_processed': failed_processed,
                'total_processed': total_processed,
                'scraped_urls': total_scrapped,
                'queue_position': queue_position,
                'link_limit': link_limit,  # This is now the link limit instead of page limit
                'crawl_progress': crawl_progress,
                'scrape_progress': scrape_progress,
                'is_active': user_id in active_user_jobs and source_url in active_user_jobs.get(user_id, set()),
                'in_queue': queue_item_exists
            },
            'process_time': f"{process_time:.2f}s",
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        print(f"Error getting source URL status: {str(e)}")
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': traceback.format_exc(),
            'process_time': f"{time.time() - start_time:.2f}s",
            'timestamp': datetime.now().isoformat()
        }), 500



@file_api.route('/get-scrapped-links', methods=['GET'])
@token_required
def realtime_scrapped_links(user_id):
    """Get the count of scraped links with optimized DB connection"""
    try:
        source_url = request.args.get('source_url')
        
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                query = """
                    SELECT COUNT(*) FROM {0}
                    WHERE USER_ID = :user_id
                """.format(SCRAPPED_TEXT_TABLE)
                
                params = {'user_id': user_id}
                
                if source_url:
                    query += " AND TOP_LEVEL_SOURCE = :source_url"
                    params['source_url'] = source_url
                    
                cursor.execute(query, params)
                scraped_count = cursor.fetchone()[0]
                
        return jsonify({
            'status': 'success',
            'scrapped_links': scraped_count,
            'source_url': source_url,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        print(f"Error getting scraped links: {str(e)}")
        traceback.print_exc()
        return standardize_error_response(str(e), 'DB_ERROR', 500)

@file_api.route('/get-pending-links', methods=['GET'])
@token_required
def realtime_pending_links(user_id):
    """Get the count of pending links with optimized DB connection"""
    try:
        source_url = request.args.get('source_url')
        
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                query = """
                    SELECT COUNT(*) FROM {0}
                    WHERE (IS_PROCESSED = 'false' OR IS_PROCESSED IS NULL)
                    AND USER_ID = :user_id
                """.format(LINKS_TO_SCRAP_TABLE)
                
                params = {'user_id': user_id}
                
                if source_url:
                    query += " AND TOP_LEVEL_SOURCE = :source_url"
                    params['source_url'] = source_url
                    
                cursor.execute(query, params)
                pending_count = cursor.fetchone()[0]
                
        return jsonify({
            'status': 'success',
            'pending_links': pending_count,
            'source_url': source_url,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        print(f"Error getting pending links: {str(e)}")
        traceback.print_exc()
        return standardize_error_response(str(e), 'DB_ERROR', 500)
    
@file_api.route('/get-total-words-scrapped', methods=['GET'])
@token_required
def realtime_total_words_scrapped(user_id):
    """Get the total word count with optimized DB query using the WORD_COUNT column"""
    try:
        source_url = request.args.get('source_url')
        
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Build the base query that directly uses the WORD_COUNT column
                query = """
                    SELECT SUM(WORD_COUNT) AS total_words
                    FROM {0}
                    WHERE USER_ID = :user_id
                """.format(SCRAPPED_TEXT_TABLE)
                
                params = {'user_id': user_id}
                
                if source_url:
                    query += " AND TOP_LEVEL_SOURCE = :source_url"
                    params['source_url'] = source_url
                    
                cursor.execute(query, params)
                row = cursor.fetchone()
                total_words = row[0] if row and row[0] else 0
                
                # Also get document count
                count_query = """
                    SELECT COUNT(*) 
                    FROM {0}
                    WHERE USER_ID = :user_id
                """.format(SCRAPPED_TEXT_TABLE)
                
                if source_url:
                    count_query += " AND TOP_LEVEL_SOURCE = :source_url"
                    
                cursor.execute(count_query, params)
                doc_count = cursor.fetchone()[0] or 0
                
        return jsonify({
            'status': 'success',
            'total_words': total_words,
            'document_count': doc_count,
            'source_url': source_url,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        print(f"Error getting total words: {str(e)}")
        traceback.print_exc()
        return standardize_error_response(str(e), 'DB_ERROR', 500)
    
@file_api.route('/get-discovered-links', methods=['GET'])
@token_required
def get_discovered_links(user_id):
    """
    Get counts and statistics for discovered links associated with a parent source URL
    
    Query params:
    - source_url: The parent source URL to filter by
    """
    try:
        # Get source URL parameter
        source_url = request.args.get('source_url')
        
        # Validate required parameter
        if not source_url:
            return standardize_error_response('source_url parameter is required.', 'MISSING_PARAM', 400)
        
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Get comprehensive link statistics in a single query
                stats_query = f"""
                    SELECT 
                        COUNT(*) AS total_links,
                        SUM(CASE WHEN IS_CRAWLED = 1 THEN 1 ELSE 0 END) AS crawled_links,
                        SUM(CASE WHEN IS_PROCESSED = 'true' THEN 1 ELSE 0 END) AS processed_links,
                        SUM(CASE WHEN IS_PROCESSED = 'Failed' THEN 1 ELSE 0 END) AS failed_links,
                        SUM(CASE WHEN ERROR IS NOT NULL THEN 1 ELSE 0 END) AS error_count,
                        SUM(CASE WHEN HAS_TEXT_IN_URL = 1 THEN 1 ELSE 0 END) AS text_indicator_count
                    FROM {LINKS_TO_SCRAP_TABLE}
                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                """
                cursor.execute(stats_query, source_url=source_url, user_id=user_id)
                stats_row = cursor.fetchone()
                
                if not stats_row:
                    return jsonify({
                        'status': 'success',
                        'total_links': 0,
                        'source_url': source_url,
                        'timestamp': datetime.now().isoformat()
                    })
                
                # Get domain statistics with optimized query
                domain_query = """
                    SELECT 
                        CASE 
                            WHEN INSTR(LINK, '//') > 0 THEN 
                                LOWER(REGEXP_SUBSTR(SUBSTR(LINK, INSTR(LINK, '//') + 2), '[^/]+'))
                            ELSE 
                                LOWER(REGEXP_SUBSTR(LINK, '[^/]+'))
                        END AS DOMAIN,
                        COUNT(*) as COUNT
                    FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                    GROUP BY CASE 
                        WHEN INSTR(LINK, '//') > 0 THEN 
                            LOWER(REGEXP_SUBSTR(SUBSTR(LINK, INSTR(LINK, '//') + 2), '[^/]+'))
                        ELSE 
                            LOWER(REGEXP_SUBSTR(LINK, '[^/]+'))
                    END
                    ORDER BY COUNT DESC
                """.format(LINKS_TO_SCRAP_TABLE)
                
                cursor.execute(domain_query, source_url=source_url, user_id=user_id)
                domain_stats = [{'domain': row[0], 'count': row[1]} for row in cursor.fetchall()]
                
                # Build response with just the counts and statistics
                return jsonify({
                    'status': 'success',
                    'total_links': stats_row[0],
                    'source_url': source_url,
                    'stats': {
                        'total': stats_row[0],
                        'crawled': stats_row[1],
                        'processed': stats_row[2],
                        'failed': stats_row[3],
                        'error_count': stats_row[4],
                        'text_indicator_count': stats_row[5]
                    },
                    'crawl_progress': round((stats_row[1] / stats_row[0]) * 100, 1) if stats_row[0] > 0 else 0,
                    'processing_progress': round((stats_row[2] / stats_row[0]) * 100, 1) if stats_row[0] > 0 else 0,
                    'domain_stats': domain_stats,
                    'timestamp': datetime.now().isoformat()
                })
                
    except Exception as e:
        print(f"Error getting discovered links counts: {str(e)}")
        traceback.print_exc()
        return standardize_error_response(str(e), 'SERVER_ERROR', 500)

@file_api.route('/scrapped-sub-links', methods=['POST'])
def scrapped_sub_links():
    """
    Fetch links related to a specific source URL with pagination
    Returns 10 URLs at a time with their processing status
    Now includes user_id filtering for proper isolation
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'status': 'error',
                'message': 'Request body is required.',
                'timestamp': datetime.now().isoformat()
            }), 400
        
        source_url = data.get('source_url')
        user_id = data.get('user_id')  # Get user_id from request body
        page = data.get('page', 1)  # Default to page 1
        page_size = 10  # Fixed page size of 10 items
        
        if not source_url:
            return jsonify({
                'status': 'error',
                'message': 'source_url is required in the request body.',
                'timestamp': datetime.now().isoformat()
            }), 400
            
        # If user_id is not provided in request body, try to get from token
        if not user_id:
            # Try to get from authorization header
            token = request.headers.get('Authorization')
            if token and token.startswith("Bearer "):
                try:
                    decoded = jwt.decode(token.split(" ")[1], SECRET_KEY, algorithms=['HS256'])
                    user_id = decoded['user_id']
                    print(f"Extracted user_id from token: {user_id}")
                except Exception as token_error:
                    print(f"Error extracting user_id from token: {str(token_error)}")
                    return jsonify({
                        'status': 'error',
                        'message': 'user_id is required in the request body or valid authentication.',
                        'timestamp': datetime.now().isoformat()
                    }), 400
            else:
                return jsonify({
                    'status': 'error',
                    'message': 'user_id is required in the request body or valid authentication.',
                    'timestamp': datetime.now().isoformat()
                }), 400
        
        print(f"Fetching links for source_url: {source_url}, user_id: {user_id}, page: {page}")
        
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Calculate pagination parameters
                offset = (page - 1) * page_size
                
                # Get total count for pagination - now with user_id filter
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                """.format(LINKS_TO_SCRAP_TABLE), source_url=source_url, user_id=user_id)
                
                total_links = cursor.fetchone()[0]
                
                # Fetch the paginated links - now with user_id filter
                cursor.execute(f"""
                    SELECT LINK, IS_PROCESSED 
                    FROM (
                        SELECT LINK, IS_PROCESSED, ROW_NUMBER() OVER (ORDER BY ID) as rn
                        FROM {LINKS_TO_SCRAP_TABLE}
                        WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                    ) 
                    WHERE rn > :offset AND rn <= :end_row
                """, source_url=source_url, user_id=user_id, offset=offset, end_row=(offset + page_size))
                
                links_data = []
                
                for link_row in cursor.fetchall():
                    link, is_processed = link_row
                    
                    # Determine URL status
                    url_status = "Completed" if is_processed == 'true' else "Pending"
                    
                    # If is_processed is "Failed", mark as failed
                    if is_processed == 'Failed':
                        url_status = "Failed"
                        
                    links_data.append({
                        'url': link,
                        'url_status': url_status
                    })
                
                # Calculate pagination metadata
                total_pages = (total_links + page_size - 1) // page_size  # Ceiling division
                has_next = page < total_pages
                has_prev = page > 1
                
        return jsonify({
            'status': 'success',
            'data': links_data,
            'pagination': {
                'page': page,
                'page_size': page_size,
                'total_items': total_links,
                'total_pages': total_pages,
                'has_next': has_next,
                'has_prev': has_prev
            },
            'source_url': source_url,
            'user_id': user_id,  # Include user_id in response for transparency
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Error in scrapped_sub_links: {str(e)}")
        traceback.print_exc()
        return jsonify({
            'status': 'error',
            'message': str(e),
            'traceback': traceback.format_exc(),
            'timestamp': datetime.now().isoformat()
        }), 500
        
@file_api.route('/all-documents', methods=['GET'])
@token_required
def get_all_documents(user_id):
    """Get all document sources for a user with improved status determination"""
    try:
        print(f"Fetching all documents for user: {user_id}")

        # Ensure user_id is validated
        if isinstance(user_id, list):
            user_id = user_id[0]
        elif not isinstance(user_id, (int, str)):
            raise ValueError(f"Invalid user_id type: {type(user_id)}. Expected int or str.")

        # Get pagination parameters
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)

        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Fetch source URLs
                print(f"Querying {SOURCE_URLS_TABLE} for user: {user_id}")
                
                query = f"""
                    SELECT SOURCE_URL, 
                           TO_CHAR(TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as TIMESTAMP,
                           PAGE_LIMIT
                    FROM {SOURCE_URLS_TABLE}
                    WHERE USER_ID = :user_id
                    ORDER BY TIMESTAMP DESC
                    OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY
                """
                cursor.execute(query, {"user_id": user_id})
                source_urls = cursor.fetchall()
                
                print(f"Found {len(source_urls)} source URLs for user: {user_id}")

                if not source_urls:
                    return jsonify({
                        'status': 'success',
                        'documents': [],
                        'count': 0,
                        'timestamp': datetime.now().isoformat()
                    })

                # Extract source URLs for the IN clause
                source_url_list = [row[0] for row in source_urls]
                if not source_url_list:
                    return jsonify({
                        'status': 'success',
                        'documents': [],
                        'count': 0,
                        'timestamp': datetime.now().isoformat()
                    })

                # Construct a valid IN clause for Oracle
                url_in_clause = ", ".join(f"'{url}'" for url in source_url_list)

                # Query for link stats
                link_stats_query = f"""
                    SELECT 
                        TOP_LEVEL_SOURCE,
                        COUNT(*) as total_links,
                        SUM(CASE WHEN IS_CRAWLED = 1 THEN 1 ELSE 0 END) as crawled_links,
                        SUM(CASE WHEN IS_PROCESSED = 'true' THEN 1 ELSE 0 END) as processed_links,
                        SUM(CASE WHEN IS_PROCESSED = 'Failed' THEN 1 ELSE 0 END) as failed_links
                    FROM {LINKS_TO_SCRAP_TABLE}
                    WHERE USER_ID = :user_id
                    AND TOP_LEVEL_SOURCE IN ({url_in_clause})
                    GROUP BY TOP_LEVEL_SOURCE
                """
                cursor.execute(link_stats_query, {"user_id": user_id})
                link_stats = {row[0]: {'total': row[1], 'crawled': row[2], 'processed': row[3], 'failed': row[4]} for row in cursor.fetchall()}

                # Query for scrapped text stats
                scrapped_query = f"""
                    SELECT 
                        TOP_LEVEL_SOURCE,
                        COUNT(*) as scrapped_count
                    FROM {SCRAPPED_TEXT_TABLE}
                    WHERE USER_ID = :user_id
                    AND TOP_LEVEL_SOURCE IN ({url_in_clause})
                    GROUP BY TOP_LEVEL_SOURCE
                """
                cursor.execute(scrapped_query, {"user_id": user_id})
                scrapped_stats = {row[0]: row[1] for row in cursor.fetchall()}

                # Query for queue information
                queue_query = f"""
                    SELECT 
                        SOURCE_URL,
                        PROCESSED,
                        PROCESSING_STARTED
                    FROM {PROCESSING_QUEUE_TABLE}
                    WHERE USER_ID = :user_id
                    AND SOURCE_URL IN ({url_in_clause})
                """
                cursor.execute(queue_query, {"user_id": user_id})
                queue_info = {row[0]: {'processed': row[1], 'processing_started': row[2]} for row in cursor.fetchall()}

                # Construct documents array with more accurate status determination
                documents = []
                for source_url, timestamp, link_limit in source_urls:
                    # Default values if no stats found
                    total_links = 0
                    processed_count = 0
                    failed_count = 0
                    crawled_count = 0
                    scrapped_count = 0
                    
                    # Get stats if available
                    if source_url in link_stats:
                        stats = link_stats[source_url]
                        total_links = stats['total']
                        processed_count = stats['processed']
                        failed_count = stats['failed']
                        crawled_count = stats['crawled']
                    
                    if source_url in scrapped_stats:
                        scrapped_count = scrapped_stats[source_url]

                    # Calculate progress percentages
                    crawl_progress = round((crawled_count / total_links * 100), 1) if total_links > 0 else 0
                    scrape_progress = round((scrapped_count / total_links * 100), 1) if total_links > 0 else 0

                    # Check if URL is in queue
                    in_queue = source_url in queue_info
                    queue_item = queue_info.get(source_url, {})
                    is_processed = queue_item.get('processed', 0) == 1
                    is_processing = queue_item.get('processing_started') is not None

                    # Determine if URL is active job
                    is_active = user_id in active_user_jobs and source_url in active_user_jobs.get(user_id, set())

                    # New, more accurate status determination
                    if total_links == 0:
                        status = 'Pending'
                    # Only consider "Completed" if:
                    # 1. All links have been crawled (crawled_count == total_links)
                    # 2. All links have been either processed or failed (processed_count + failed_count == total_links)
                    # 3. We actually have some scraped content (scrapped_count > 0)
                    elif (crawled_count == total_links and 
                          processed_count + failed_count == total_links and 
                          scrapped_count > 0):
                        status = 'Processing'
                    elif is_active:
                        status = 'In Progress'
                    elif in_queue and not is_processed and not is_processing:
                        status = 'Queued'
                    else:
                        status = 'Completed'

                    # Create document object
                    document = {
                        'source_url': source_url,
                        'timestamp': timestamp,
                        'link_limit': link_limit,  # Now represents the link limit instead of page limit
                        'status': status,
                        'total_links': total_links,
                        'processed_links': processed_count,
                        'failed_links': failed_count,
                        'scrapped_links': scrapped_count,
                        'crawled_links': crawled_count,
                        'crawl_progress': crawl_progress,
                        'scrape_progress': scrape_progress,
                        'is_active': is_active,
                        'in_queue': in_queue,
                        'queue_position': None
                    }
                    documents.append(document)

                # Get the total count for pagination
                count_query = f"SELECT COUNT(*) FROM {SOURCE_URLS_TABLE} WHERE USER_ID = :user_id"
                cursor.execute(count_query, {"user_id": user_id})
                total_count = cursor.fetchone()[0]

        return jsonify({
            'status': 'success',
            'documents': documents,
            'count': len(documents),
            'total': total_count,
            'limit': limit,
            'offset': offset,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        print(f"Error getting all documents: {str(e)}")
        traceback.print_exc()
        return standardize_error_response(str(e), 'DB_ERROR', 500)
    
@file_api.route('/get-total-words', methods=['GET'])
@token_required
def get_total_words(user_id):
    """
    Get the total word count for a source URL or all user content
    
    Query parameters:
    - source_url: (Optional) Filter by parent source URL
    """
    try:
        # Get source_url parameter
        source_url = request.args.get('source_url')
        
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Build the base query
                base_query = f"""
                    SELECT 
                        COUNT(*) AS total_documents,
                        SUM(WORD_COUNT) AS total_words,
                        AVG(WORD_COUNT) AS avg_words_per_document,
                        MIN(WORD_COUNT) AS min_words,
                        MAX(WORD_COUNT) AS max_words
                    FROM {SCRAPPED_TEXT_TABLE}
                    WHERE USER_ID = :user_id
                """
                
                params = {'user_id': user_id}
                
                # Add source_url filter if provided
                if source_url:
                    base_query += " AND TOP_LEVEL_SOURCE = :source_url"
                    params['source_url'] = source_url
                
                # Execute the query
                cursor.execute(base_query, params)
                
                result = cursor.fetchone()
                if not result:
                    return jsonify({
                        'status': 'success',
                        'message': 'No documents found',
                        'total_documents': 0,
                        'total_words': 0,
                        'source_url': source_url if source_url else 'all',
                        'timestamp': datetime.now().isoformat()
                    })
                
                total_documents, total_words, avg_words, min_words, max_words = result
                
                # If source_url is provided, also get document-level word counts
                document_stats = []
                if source_url:
                    document_query = f"""
                        SELECT 
                            ID,
                            CONTENT_LINK,
                            TITLE,
                            WORD_COUNT,
                            TO_CHAR(SCRAPE_DATE, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as SCRAPE_DATE
                        FROM {SCRAPPED_TEXT_TABLE}
                        WHERE USER_ID = :user_id AND TOP_LEVEL_SOURCE = :source_url
                        ORDER BY WORD_COUNT DESC
                    """
                    
                    cursor.execute(document_query, user_id=user_id, source_url=source_url)
                    
                    for row in cursor.fetchall():
                        document_stats.append({
                            'id': row[0],
                            'url': row[1],
                            'title': row[2],
                            'word_count': row[3],
                            'scrape_date': row[4]
                        })
                
                return jsonify({
                    'status': 'success',
                    'total_documents': total_documents,
                    'total_words': total_words,
                    'average_words_per_document': round(avg_words, 1) if avg_words else 0,
                    'min_words': min_words,
                    'max_words': max_words,
                    'document_details': document_stats if source_url else [],
                    'source_url': source_url if source_url else 'all',
                    'timestamp': datetime.now().isoformat()
                })
                
    except Exception as e:
        print(f"Error getting total words: {str(e)}")
        traceback.print_exc()
        return standardize_error_response(str(e), 'SERVER_ERROR', 500)  
@file_api.route('/vectorization-ready', methods=['GET'])
@token_required
def check_vectorization_ready(user_id):
    """
    Check if vectorization is complete for a source URL and return "ready" when it's done
    
    Query parameters:
    - source_url: The URL to check for vectorization status
    
    Returns:
        JSON: {"status": "ready"} when vectorization is complete, or 
              {"status": "pending", "message": "..."} when still in progress
    """
    try:
        source_url = request.args.get('source_url')
        
        if not source_url:
            return standardize_error_response('source_url parameter is required.', 'MISSING_PARAM', 400)
        
        # Connect to vector database to check if vectors exist
        from oracle_chatbot import connect_to_vectdb
        from langchain_google_genai import GoogleGenerativeAIEmbeddings
        from langchain_community.vectorstores import OracleVS
        from langchain_community.vectorstores.utils import DistanceStrategy
        import os
        
        # Get the VECTDB_TABLE_NAME and GOOGLE_API_KEY
        VECTDB_TABLE_NAME = os.getenv("VECTDB_TABLE_NAME", "vector_files_with_10000_chunk_new")
        GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
        
        vectdb_connection = connect_to_vectdb()
        if not vectdb_connection:
            return jsonify({
                'status': 'error',
                'message': 'Failed to connect to vector database',
                'timestamp': datetime.now().isoformat()
            }), 500
        
        try:
            # Use a query to check if any vectors exist for this source_url
            with vectdb_connection.cursor() as cursor:
                # Check the vector store metadata to see if any entries are for this source_url
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE METADATA LIKE :source_pattern
                """.format(VECTDB_TABLE_NAME), 
                   source_pattern=f'%"source":"{source_url}"%')
                
                vector_count = cursor.fetchone()[0]
                
                # Also check if crawling/processing is complete
                with get_jsondb_connection() as processing_conn:
                    with processing_conn.cursor() as processing_cursor:
                        # Check if URL exists in the processing queue and is marked as complete
                        processing_cursor.execute("""
                            SELECT PROCESSED, PROCESSING_COMPLETED 
                            FROM {0}
                            WHERE SOURCE_URL = :source_url AND USER_ID = :user_id
                        """.format(PROCESSING_QUEUE_TABLE), 
                           source_url=source_url, user_id=user_id)
                        
                        processing_row = processing_cursor.fetchone()
                        crawling_complete = False
                        
                        if processing_row and processing_row[0] == 1:
                            crawling_complete = True
                
                # Check if there are any documents for this source
                with get_jsondb_connection() as text_conn:
                    with text_conn.cursor() as text_cursor:
                        text_cursor.execute("""
                            SELECT COUNT(*) FROM {0}
                            WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                        """.format(SCRAPPED_TEXT_TABLE), 
                           source_url=source_url, user_id=user_id)
                        
                        document_count = text_cursor.fetchone()[0]
                
                # Determine status
                if vector_count > 0 and crawling_complete:
                    status = "ready"
                    message = f"Vectorization is complete with {vector_count} vector entries."
                elif document_count == 0:
                    status = "pending"
                    message = "No documents have been crawled yet for this source URL."
                elif not crawling_complete:
                    status = "pending"
                    message = "Crawling and processing is still in progress."
                else:
                    status = "pending"
                    message = f"Vectorization is still in progress. {document_count} documents available, but no vectors found yet."
                
                # Return simple response based on status
                if status == "ready":
                    return jsonify({
                        'status': status,
                        'timestamp': datetime.now().isoformat()
                    })
                else:
                    return jsonify({
                        'status': status,
                        'message': message,
                        'timestamp': datetime.now().isoformat()
                    })
                
        finally:
            if vectdb_connection:
                vectdb_connection.close()
    
    except Exception as e:
        traceback_str = traceback.format_exc()
        print(f"Error checking vectorization ready status: {str(e)}\n{traceback_str}")
        return standardize_error_response(str(e), 'SERVER_ERROR', 500)      

@file_api.route('/vectorization-status', methods=['GET'])
@token_required
def get_vectorization_status(user_id):
    """Get the vectorization status for a source URL"""
    try:
        source_url = request.args.get('source_url')
        
        if not source_url:
            return standardize_error_response('source_url parameter is required.', 'MISSING_PARAM', 400)
        
        # Connect to vector database to check if vectors exist
        from oracle_chatbot import connect_to_vectdb
        from langchain_google_genai import GoogleGenerativeAIEmbeddings
        from langchain_community.vectorstores import OracleVS
        from langchain_community.vectorstores.utils import DistanceStrategy
        import os
        
        # Get the VECTDB_TABLE_NAME and GOOGLE_API_KEY
        VECTDB_TABLE_NAME = os.getenv("VECTDB_TABLE_NAME", "vector_files_with_10000_chunk_new")
        GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
        
        vectdb_connection = connect_to_vectdb()
        if not vectdb_connection:
            return standardize_error_response('Failed to connect to vector database', 'DB_ERROR', 500)
        
        try:
            # Initialize embeddings and vector store
            embeddings = GoogleGenerativeAIEmbeddings(
                google_api_key=GOOGLE_API_KEY,
                model="models/text-embedding-004"
            )
            
            vector_store = OracleVS(
                client=vectdb_connection,
                embedding_function=embeddings,
                table_name=VECTDB_TABLE_NAME,
                distance_strategy=DistanceStrategy.COSINE,
            )
            
            # Use a query to check if any vectors exist for this source_url
            # We'll use an SQL query directly on the vector table
            with vectdb_connection.cursor() as cursor:
                # Check the vector store metadata to see if any entries are for this source_url
                cursor.execute("""
                    SELECT COUNT(*) FROM {0}
                    WHERE METADATA LIKE :source_pattern
                """.format(VECTDB_TABLE_NAME), 
                   source_pattern=f'%"source":"{source_url}"%')
                
                vector_count = cursor.fetchone()[0]
                
                # Also check the VECTORIZED_DOCUMENTS tracking table
                try:
                    with get_jsondb_connection() as connection:
                        with connection.cursor() as tracking_cursor:
                            tracking_cursor.execute("""
                                SELECT COUNT(DISTINCT DOCUMENT_ID) 
                                FROM VECTORIZED_DOCUMENTS
                                WHERE SOURCE_URL = :source_url AND USER_ID = :user_id
                            """, source_url=source_url, user_id=user_id)
                            
                            vectorized_doc_count = tracking_cursor.fetchone()[0]
                            print(f"Found {vectorized_doc_count} documents tracked as vectorized in VECTORIZED_DOCUMENTS table")
                except Exception as table_error:
                    print(f"Note: VECTORIZED_DOCUMENTS table may not exist yet: {str(table_error)}")
                    vectorized_doc_count = 0
                
                # Get the total documents scraped for this source URL
                with get_jsondb_connection() as text_conn:
                    with text_conn.cursor() as text_cursor:
                        text_cursor.execute("""
                            SELECT COUNT(*) FROM {0}
                            WHERE TOP_LEVEL_SOURCE = :source_url AND USER_ID = :user_id
                        """.format(SCRAPPED_TEXT_TABLE), 
                           source_url=source_url, user_id=user_id)
                        
                        document_count = text_cursor.fetchone()[0]
                
                # Determine status
                if vector_count > 0:
                    status = "Vectorized"
                    message = f"Source URL has been vectorized with {vector_count} vector entries from {document_count} documents."
                elif document_count == 0:
                    status = "No Data"
                    message = "No scrapped documents found for this source URL."
                else:
                    status = "Pending"
                    message = f"Source URL has {document_count} documents that need to be vectorized."
                
                # Get processing status as well
                with get_jsondb_connection() as processing_conn:
                    with processing_conn.cursor() as processing_cursor:
                        processing_cursor.execute("""
                            SELECT PROCESSED, PROCESSING_COMPLETED 
                            FROM {0}
                            WHERE SOURCE_URL = :source_url AND USER_ID = :user_id
                        """.format(PROCESSING_QUEUE_TABLE), 
                           source_url=source_url, user_id=user_id)
                        
                        processing_row = processing_cursor.fetchone()
                        crawling_complete = False
                        
                        if processing_row and processing_row[0] == 1:
                            crawling_complete = True
                
                # Final status
                is_ready_for_chatbot = status == "Vectorized" and crawling_complete
                
                return jsonify({
                    'status': 'success',
                    'vectorization_status': status,
                    'message': message,
                    'vector_count': vector_count,
                    'document_count': document_count,
                    'vectorized_doc_count': vectorized_doc_count,
                    'crawling_complete': crawling_complete,
                    'ready_for_chatbot': is_ready_for_chatbot,
                    'source_url': source_url,
                    'timestamp': datetime.now().isoformat()
                })
                
        finally:
            if vectdb_connection:
                vectdb_connection.close()
    
    except Exception as e:
        traceback_str = traceback.format_exc()
        print(f"Error checking vectorization status: {str(e)}\n{traceback_str}")
        return standardize_error_response(str(e), 'SERVER_ERROR', 500)
    
def standardize_error_response(error, code=None, status_code=500, process_time=None):
    """
    Create a standardized error response with performance metrics
    
    Args:
        error: The error message or exception
        code: Error code for frontend handling
        status_code: HTTP status code
        process_time: Processing time for the request (added for performance tracking)
        
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
        
    if traceback_str and status_code >= 500:
        # Include traceback in response for server errors
        print(f"Server error: {error_message}\n{traceback_str}")
        response['error_details'] = traceback_str
        
    return jsonify(response), status_code
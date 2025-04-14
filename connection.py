import os
import time
import logging
import traceback
from contextlib import contextmanager
from dotenv import load_dotenv
import oracledb

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Get configuration from environment variables
ORACLE_USER = os.getenv('ORACLE_USER', 'ANIRUDDHA')
ORACLE_PASSWORD = os.getenv('ORACLE_PASSWORD', 'OracleDatabase&2025')
# Default connection strings (overridden by environment variables if available)
DEFAULT_JSONDB_DSN = "(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.ap-mumbai-1.oraclecloud.com))(connect_data=(service_name=g5d08999936e3df_jsondb_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))"
DEFAULT_VECTDB_DSN = "(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.ap-mumbai-1.oraclecloud.com))(connect_data=(service_name=g5d08999936e3df_vectdb_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))"

# Get DSNs from environment variables if available, otherwise use defaults
ORACLE_DSN = os.getenv('ORACLE_DSN', DEFAULT_JSONDB_DSN)
ORACLE_DSN_VECTDB = os.getenv("ORACLE_DSN_VECTDB", DEFAULT_VECTDB_DSN)

# Vector DB connection details
ORACLE_USER_VECTDB = os.getenv("ORACLE_USER_VECTDB", "ANIRUDDHA1")

# Connection pool and retry settings
MAX_RETRIES = 3
RETRY_DELAY = 1
jsondb_connection_pool = None
vectdb_connection_pool = None

def initialize_session(connection, requested_tag):
    """Session callback to initialize each connection in the pool"""
    cursor = connection.cursor()
    
    # Set session-specific settings for better performance
    cursor.execute("ALTER SESSION SET optimizer_mode = 'ALL_ROWS'")
    cursor.execute("ALTER SESSION SET nls_date_format = 'YYYY-MM-DD HH24:MI:SS'")
    
    cursor.close()

def initialize_jsondb_connection_pool(use_wallet=True):
    """
    Initialize Oracle connection pool for JSON DB with optimized settings
    
    Args:
        use_wallet: Whether to use wallet-based authentication or direct connection
    """
    global jsondb_connection_pool
    if jsondb_connection_pool is not None:
        return True
    
    try:
        # Optimized pool parameters
        pool_params = {
            "user": ORACLE_USER,
            "password": ORACLE_PASSWORD,
            "dsn": ORACLE_DSN,
            "min": 5,          # Minimum connections
            "max": 30,         # Maximum connections
            "increment": 5,    # Increment for better scalability
            "getmode": oracledb.POOL_GETMODE_WAIT,  # Wait for available connection
            "wait_timeout": 10000,  # 10 seconds wait timeout
            "max_lifetime_session": 28800,  # 8 hours
            "session_callback": initialize_session  # Add session callback for setup
        }
        
        # Add wallet parameters if using wallet authentication
        if use_wallet:
            pool_params.update({
                "config_dir": "Wallet_jsondb",
                "wallet_location": "Wallet_jsondb",
                "wallet_password": ORACLE_PASSWORD
            })
        
        jsondb_connection_pool = oracledb.create_pool(**pool_params)
        logger.info(f"JSON DB connection pool created successfully with {jsondb_connection_pool.min} to {jsondb_connection_pool.max} connections")
        return True
    except Exception as e:
        logger.error(f"Error creating JSON DB connection pool: {e}")
        traceback.print_exc()
        return False

def initialize_vectdb_connection_pool(use_wallet=True):
    """
    Initialize Oracle connection pool for Vector DB with optimized settings
    
    Args:
        use_wallet: Whether to use wallet-based authentication or direct connection
    """
    global vectdb_connection_pool
    if vectdb_connection_pool is not None:
        return True
    
    try:
        # Optimized pool parameters
        pool_params = {
            "user": ORACLE_USER_VECTDB,
            "password": ORACLE_PASSWORD,
            "dsn": ORACLE_DSN_VECTDB,
            "min": 3,          # Minimum connections
            "max": 20,         # Maximum connections
            "increment": 3,    # Increment for better scalability
            "getmode": oracledb.POOL_GETMODE_WAIT,  # Wait for available connection
            "wait_timeout": 10000,  # 10 seconds wait timeout
            "max_lifetime_session": 28800,  # 8 hours
            "session_callback": initialize_session  # Add session callback for setup
        }
        
        # Add wallet parameters if using wallet authentication
        if use_wallet:
            pool_params.update({
                "config_dir": "Wallet_VECTDB",
                "wallet_location": "Wallet_VECTDB",
                "wallet_password": ORACLE_PASSWORD
            })
        
        vectdb_connection_pool = oracledb.create_pool(**pool_params)
        logger.info(f"Vector DB connection pool created successfully with {vectdb_connection_pool.min} to {vectdb_connection_pool.max} connections")
        return True
    except Exception as e:
        logger.error(f"Error creating Vector DB connection pool: {e}")
        traceback.print_exc()
        return False

@contextmanager
def get_jsondb_connection(use_wallet=True):
    """
    Context manager for JSON DB connections with retry logic and connection pooling
    
    Args:
        use_wallet: Whether to use wallet-based authentication or direct connection
    """
    conn = None
    retries = 0
    
    while retries < MAX_RETRIES:
        try:
            global jsondb_connection_pool
            if jsondb_connection_pool is None:
                initialize_jsondb_connection_pool(use_wallet)
                
            if jsondb_connection_pool:
                conn = jsondb_connection_pool.acquire()
                logger.debug("Acquired connection from JSON DB pool")
            else:
                # Choose connection method based on use_wallet parameter
                if use_wallet:
                    conn = oracledb.connect(
                        user=ORACLE_USER,
                        password=ORACLE_PASSWORD,
                        dsn=ORACLE_DSN,
                        config_dir="Wallet_jsondb",
                        wallet_location="Wallet_jsondb",
                        wallet_password=ORACLE_PASSWORD
                    )
                    logger.debug("Created new JSON DB connection with wallet")
                else:
                    conn = oracledb.connect(
                        user=ORACLE_USER,
                        password=ORACLE_PASSWORD,
                        dsn=ORACLE_DSN
                    )
                    logger.debug("Created new direct JSON DB connection")
            
            try:
                # Yield the connection to the caller
                yield conn
                
                # Commit if no exception occurred and not in autocommit mode
                if conn and not conn.autocommit:
                    try:
                        conn.commit()
                    except Exception as commit_error:
                        logger.error(f"Error committing transaction: {commit_error}")
            except:
                # If an exception occurred during the yield, roll back the transaction
                if conn and not conn.autocommit:
                    try:
                        conn.rollback()
                    except Exception as rollback_error:
                        logger.error(f"Error during rollback: {rollback_error}")
                # Re-raise the exception
                raise
            finally:
                # Always ensure connection is closed or released
                if conn:
                    try:
                        if jsondb_connection_pool and isinstance(conn, oracledb.Connection):
                            jsondb_connection_pool.release(conn)
                            logger.debug("Released connection back to JSON DB pool")
                        else:
                            conn.close()
                            logger.debug("Closed direct JSON DB connection")
                    except Exception as close_error:
                        logger.error(f"Error closing connection: {close_error}")
                conn = None  # Set to None to prevent double-closing
            
            # If we got here without exceptions, we can break out of the retry loop
            break
            
        except oracledb.DatabaseError as e:
            retries += 1
            logger.warning(f"Database connection attempt {retries} failed: {e}")
            
            # Wait before retrying with exponential backoff
            time.sleep(RETRY_DELAY * (2 ** (retries - 1)))
            
        except Exception as e:
            logger.error(f"Unexpected error getting connection: {e}")
            traceback.print_exc()
            
            # Re-raise any non-database errors
            raise

    # If we exhausted all retries, raise an exception
    if retries >= MAX_RETRIES:
        raise Exception(f"Failed to establish database connection after {MAX_RETRIES} attempts")

@contextmanager
def get_vectdb_connection(use_wallet=True):
    """
    Context manager for Vector DB connections with retry logic and connection pooling
    
    Args:
        use_wallet: Whether to use wallet-based authentication or direct connection
    """
    conn = None
    retries = 0
    
    while retries < MAX_RETRIES:
        try:
            global vectdb_connection_pool
            if vectdb_connection_pool is None:
                initialize_vectdb_connection_pool(use_wallet)
                
            if vectdb_connection_pool:
                conn = vectdb_connection_pool.acquire()
                logger.debug("Acquired connection from Vector DB pool")
            else:
                # Choose connection method based on use_wallet parameter
                if use_wallet:
                    conn = oracledb.connect(
                        user=ORACLE_USER_VECTDB,
                        password=ORACLE_PASSWORD,
                        dsn=ORACLE_DSN_VECTDB,
                        config_dir="Wallet_VECTDB",
                        wallet_location="Wallet_VECTDB",
                        wallet_password=ORACLE_PASSWORD
                    )
                    logger.debug("Created new Vector DB connection with wallet")
                else:
                    conn = oracledb.connect(
                        user=ORACLE_USER_VECTDB,
                        password=ORACLE_PASSWORD,
                        dsn=ORACLE_DSN_VECTDB
                    )
                    logger.debug("Created new direct Vector DB connection")
                
            yield conn
            return  # Ensure the generator exits properly
        except oracledb.DatabaseError as e:
            retries += 1
            logger.warning(f"Vector DB connection attempt {retries} failed: {e}")
            
            if conn:
                try:
                    conn.close()
                except:
                    pass
                    
            # Wait before retrying with exponential backoff
            time.sleep(RETRY_DELAY * (2 ** (retries - 1)))
        except Exception as e:
            logger.error(f"Unexpected error getting Vector DB connection: {e}")
            traceback.print_exc()
            
            if conn:
                try:
                    conn.close()
                except:
                    pass
            
            raise
        finally:
            if conn:
                try:
                    if vectdb_connection_pool and isinstance(conn, oracledb.Connection):
                        vectdb_connection_pool.release(conn)
                        logger.debug("Released connection back to Vector DB pool")
                    else:
                        conn.close()
                        logger.debug("Closed direct Vector DB connection")
                except Exception as close_error:
                    logger.error(f"Error closing Vector DB connection: {close_error}")

    raise Exception(f"Failed to establish Vector DB connection after {MAX_RETRIES} attempts")

def connect_to_jsondb(use_wallet=True):
    """
    Direct connection method for Oracle JSON DB (for compatibility with existing code)
    Note: Prefer using get_jsondb_connection() context manager for new code
    
    Args:
        use_wallet: Whether to use wallet-based authentication or direct connection
    """
    try:
        # Try to get a connection from the pool first
        global jsondb_connection_pool
        if jsondb_connection_pool is not None:
            try:
                conn = jsondb_connection_pool.acquire()
                logger.info("Connected to JSON Database from pool!")
                return conn
            except Exception as pool_error:
                logger.warning(f"Could not get connection from pool: {pool_error}")
                # Fall through to direct connection
        
        if use_wallet:
            # Use wallet-based authentication (how your application currently connects)
            connection = oracledb.connect(
                user=ORACLE_USER,
                password=ORACLE_PASSWORD,
                dsn=ORACLE_DSN,
                config_dir="Wallet_jsondb",
                wallet_location="Wallet_jsondb",
                wallet_password=ORACLE_PASSWORD
            )
        else:
            # Use direct connection with DSN string (Oracle example approach)
            connection = oracledb.connect(
                user=ORACLE_USER,
                password=ORACLE_PASSWORD,
                dsn=ORACLE_DSN
            )
            
        logger.info("Connected to JSON Database!")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to JSON DB: {e}")
        traceback.print_exc()
        return None

def connect_to_vectdb(use_wallet=True):
    """
    Direct connection method for Oracle Vector DB (for compatibility with existing code)
    Note: Prefer using get_vectdb_connection() context manager for new code
    
    Args:
        use_wallet: Whether to use wallet-based authentication or direct connection
    """
    try:
        # Try to get a connection from the pool first
        global vectdb_connection_pool
        if vectdb_connection_pool is not None:
            try:
                conn = vectdb_connection_pool.acquire()
                logger.info("Connected to Vector Database from pool!")
                return conn
            except Exception as pool_error:
                logger.warning(f"Could not get connection from pool: {pool_error}")
                # Fall through to direct connection
        
        if use_wallet:
            # Use wallet-based authentication (how your application currently connects)
            connection = oracledb.connect(
                user=ORACLE_USER_VECTDB,
                password=ORACLE_PASSWORD,
                dsn=ORACLE_DSN_VECTDB,
                config_dir="Wallet_VECTDB",
                wallet_location="Wallet_VECTDB",
                wallet_password=ORACLE_PASSWORD
            )
        else:
            # Use direct connection with DSN string (Oracle example approach)
            connection = oracledb.connect(
                user=ORACLE_USER_VECTDB,
                password=ORACLE_PASSWORD,
                dsn=ORACLE_DSN_VECTDB
            )
            
        logger.info("Connected to Vector Database!")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to Vector DB: {e}")
        traceback.print_exc()
        return None

def execute_query(connection, query, params=None, fetch_all=True):
    """
    Execute a database query with proper error handling and improved performance
    
    Args:
        connection: Oracle database connection
        query: SQL query to execute
        params: Parameters for the query (dict or list)
        fetch_all: Whether to fetch all results or just one row
        
    Returns:
        Query results or None if error
    """
    cursor = None
    try:
        cursor = connection.cursor()
        
        # Set array size for better fetch performance when fetching many rows
        if fetch_all:
            cursor.arraysize = 100
            
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        if query.strip().upper().startswith(('SELECT', 'WITH')):
            if fetch_all:
                return cursor.fetchall()
            else:
                return cursor.fetchone()
        else:
            connection.commit()
            return cursor.rowcount
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        traceback.print_exc()
        if not query.strip().upper().startswith(('SELECT', 'WITH')):
            connection.rollback()
        return None
    finally:
        if cursor:
            cursor.close()

# Add a new method for batch execution
def execute_batch_query(connection, query, params_list, commit_every=10):
    """
    Execute a batch of queries with the same structure but different parameters
    
    Args:
        connection: Oracle database connection
        query: SQL query to execute
        params_list: List of parameter dictionaries or tuples
        commit_every: How often to commit (number of operations)
        
    Returns:
        List of results or None if error
    """
    cursor = None
    results = []
    
    try:
        cursor = connection.cursor()
        
        for i, params in enumerate(params_list):
            try:
                cursor.execute(query, params)
                
                if query.strip().upper().startswith(('SELECT', 'WITH')):
                    result = cursor.fetchall()
                    results.append(result)
                else:
                    results.append(cursor.rowcount)
                    
                # Commit periodically to avoid large transactions
                if not query.strip().upper().startswith(('SELECT', 'WITH')) and (i + 1) % commit_every == 0:
                    connection.commit()
                    
            except Exception as e:
                logger.error(f"Error in batch query at index {i}: {e}")
                results.append(None)  # Add None to maintain index alignment
                
        # Final commit if needed
        if not query.strip().upper().startswith(('SELECT', 'WITH')):
            connection.commit()
            
        return results
        
    except Exception as e:
        logger.error(f"Error executing batch query: {e}")
        traceback.print_exc()
        if not query.strip().upper().startswith(('SELECT', 'WITH')):
            connection.rollback()
        return None
    finally:
        if cursor:
            cursor.close()

# Add a utility function to check pool health
def check_connection_pools():
    """Check the health of connection pools and restart if needed"""
    try:
        result = {
            'jsondb_pool': {'status': 'not_initialized'},
            'vectdb_pool': {'status': 'not_initialized'}
        }
        
        # Check JSONDB pool
        global jsondb_connection_pool
        if jsondb_connection_pool is not None:
            try:
                # Try to acquire a connection to test the pool
                conn = jsondb_connection_pool.acquire()
                jsondb_connection_pool.release(conn)
                
                result['jsondb_pool'] = {
                    'status': 'healthy',
                    'min': jsondb_connection_pool.min,
                    'max': jsondb_connection_pool.max,
                    'busy': jsondb_connection_pool.busy, # Use busy property instead of _used
                    'open': jsondb_connection_pool.opened
                }
            except Exception as e:
                logger.error(f"JSON DB pool error: {e}")
                # Try to reinitialize the pool
                jsondb_connection_pool = None
                initialize_jsondb_connection_pool()
                
                result['jsondb_pool'] = {
                    'status': 'reinitialized',
                    'error': str(e)
                }
        else:
            # Initialize if not existing
            initialize_jsondb_connection_pool()
            result['jsondb_pool']['status'] = 'initialized'
        
        # Check VECTDB pool
        global vectdb_connection_pool
        if vectdb_connection_pool is not None:
            try:
                # Try to acquire a connection to test the pool
                conn = vectdb_connection_pool.acquire()
                vectdb_connection_pool.release(conn)
                
                result['vectdb_pool'] = {
                    'status': 'healthy',
                    'min': vectdb_connection_pool.min,
                    'max': vectdb_connection_pool.max,
                    'busy': vectdb_connection_pool.busy, # Use busy property instead of _used
                    'open': vectdb_connection_pool.opened
                }
            except Exception as e:
                logger.error(f"Vector DB pool error: {e}")
                # Try to reinitialize the pool
                vectdb_connection_pool = None
                initialize_vectdb_connection_pool()
                
                result['vectdb_pool'] = {
                    'status': 'reinitialized',
                    'error': str(e)
                }
        else:
            # Initialize if not existing
            initialize_vectdb_connection_pool()
            result['vectdb_pool']['status'] = 'initialized'
            
        return result
        
    except Exception as e:
        logger.error(f"Error checking connection pools: {e}")
        traceback.print_exc()
        return {'error': str(e)}

# Initialize connection pools on module import
# Default to wallet-based authentication for compatibility with existing code
initialize_jsondb_connection_pool(use_wallet=True)
initialize_vectdb_connection_pool(use_wallet=True)
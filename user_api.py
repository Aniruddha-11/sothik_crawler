from flask import request, jsonify, Blueprint
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
from datetime import datetime, timedelta
import re
import os
from connection import get_jsondb_connection
from dotenv import load_dotenv
import logging
import traceback
import time 
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get configuration from environment variables
SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-fallback')
VERIFICATION_SECRET_KEY = os.getenv('VERIFICATION_SECRET_KEY', 'verification-secret-key-fallback')
TOKEN_EXPIRY_HOURS = int(os.getenv('TOKEN_EXPIRY_HOURS', '24'))

# Create the blueprint
user_api = Blueprint('user_api', __name__)


# In your user_api.py or app.py
SECRET_KEY = os.getenv('SECRET_KEY')
if not SECRET_KEY:
    logger.critical("SECRET_KEY is not set in environment variables!")
    raise ValueError("SECRET_KEY must be set")

def initialize_user_tables():
    """Initialize all necessary tables for user authentication if they don't exist"""
    try:
        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Define the user profile table
                tables = {
                    "USER_PROFILES": """
                        CREATE TABLE USER_PROFILES (
                            ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            NAME VARCHAR2(100) NOT NULL,
                            USERNAME VARCHAR2(50) NOT NULL,
                            EMAIL VARCHAR2(100) NOT NULL,
                            PASSWORD VARCHAR2(255) NOT NULL,
                            EMAIL_VERIFIED NUMBER(1) DEFAULT 0,
                            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            LAST_LOGIN TIMESTAMP,
                            UPDATED_AT TIMESTAMP,
                            PASSWORD_RESET_AT TIMESTAMP,
                            VERIFIED_AT TIMESTAMP,
                            ROLE VARCHAR2(20) DEFAULT 'user',
                            CONSTRAINT UQ_USER_EMAIL UNIQUE (EMAIL),
                            CONSTRAINT UQ_USER_USERNAME UNIQUE (USERNAME)
                        )
                    """,
                    
                    "EMAIL_VERIFICATIONS": """
                        CREATE TABLE EMAIL_VERIFICATIONS (
                            ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                            USER_ID NUMBER NOT NULL,
                            TOKEN VARCHAR2(500) NOT NULL,
                            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            EXPIRES_AT TIMESTAMP,
                            CONSTRAINT FK_USER_ID FOREIGN KEY (USER_ID) REFERENCES USER_PROFILES(ID) ON DELETE CASCADE
                        )
                    """
                }
                
                # Check and create each table if it doesn't exist
                for table_name, create_sql in tables.items():
                    cursor.execute(f"""
                        SELECT COUNT(*) 
                        FROM USER_TABLES
                        WHERE TABLE_NAME = '{table_name}'
                    """)
                    
                    if cursor.fetchone()[0] == 0:
                        cursor.execute(create_sql)
                        logger.info(f"Created table {table_name}")
                        
                        # Create necessary indexes
                        if table_name == "USER_PROFILES":
                            # Create index on common search fields
                            cursor.execute(f"""
                                CREATE INDEX IDX_{table_name}_EMAIL 
                                ON {table_name} (EMAIL)
                            """)
                            cursor.execute(f"""
                                CREATE INDEX IDX_{table_name}_USERNAME 
                                ON {table_name} (USERNAME)
                            """)
                        elif table_name == "EMAIL_VERIFICATIONS":
                            # Create index for token searching
                            cursor.execute(f"""
                                CREATE INDEX IDX_{table_name}_TOKEN 
                                ON {table_name} (TOKEN)
                            """)
                
                connection.commit()
                logger.info("User authentication tables initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing tables: {e}")
        traceback.print_exc()

# Initialize tables on module load
initialize_user_tables()

def standardize_error_response(error, code=None, status_code=500):
    """
    Create a standardized error response
    
    Args:
        error: The error message or exception
        code: Error code for frontend handling
        status_code: HTTP status code
        
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
        
    if traceback_str and status_code >= 500:
        # Include traceback in response for server errors
        logger.error(f"Server error: {error_message}\n{traceback_str}")
        response['error_details'] = traceback_str
        
    return jsonify(response), status_code

def token_required(f):
    """Decorator for verifying JWT tokens"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        token = request.headers.get('Authorization')
        
        if not token or not token.startswith("Bearer "):
            return jsonify({
                'status': 'error',
                'message': 'Unauthorized access. Valid token required.',
                'timestamp': datetime.now().isoformat()
            }), 401

        token = token.split(" ")[1]
        
        try:
            decoded = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            user_id = decoded['user_id']
            
            # Call the decorated function with user_id as first argument
            return f(user_id, *args, **kwargs)
        except jwt.ExpiredSignatureError:
            return jsonify({
                'status': 'error',
                'message': 'Token has expired',
                'timestamp': datetime.now().isoformat()
            }), 401
        except jwt.InvalidTokenError:
            return jsonify({
                'status': 'error',
                'message': 'Invalid token',
                'timestamp': datetime.now().isoformat()
            }), 401
        except Exception as e:
            print(f"Token verification error: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': 'Authentication failed',
                'timestamp': datetime.now().isoformat()
            }), 401
            
    # Make sure to preserve the original function name
    wrapper.__name__ = f.__name__
    return wrapper

def generate_auth_token(user):
    """Generate JWT authentication token"""
    try:
        payload = {
            'user_id': user['id'],
            'name': user['name'],
            'username': user['username'],
            'email': user['email'],
            'exp': datetime.utcnow() + timedelta(hours=TOKEN_EXPIRY_HOURS)
        }
        return jwt.encode(payload, os.getenv('SECRET_KEY'), algorithm='HS256')
    except Exception as e:
        logger.error(f"Error generating auth token: {e}")
        traceback.print_exc()
        raise
    
@user_api.route('/auth/register', methods=['POST'])
def register_user():
    try:
        data = request.json
        name = data.get('name', '').strip()
        username = data.get('username', '').strip()
        email = data.get('email', '').strip().lower()
        password = data.get('password', '').strip()

        # Validate that all fields are provided
        if not all([name, username, email, password]):
            return standardize_error_response('All fields are required', 'VALIDATION_ERROR', 400)

        # Validate email format
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            return standardize_error_response('Invalid email format', 'VALIDATION_ERROR', 400)

        # Validate password format (must contain both letters and numbers, min 8 chars)
        if not re.match(r'^(?=.*[A-Za-z])(?=.*\d)[A-Za-z\d]{8,}$', password):
            return standardize_error_response(
                'Password must be at least 8 characters and include letters and numbers',
                'VALIDATION_ERROR',
                400
            )

        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Check if email already exists
                cursor.execute("""
                    SELECT COUNT(*) FROM USER_PROFILES WHERE EMAIL = :email
                """, email=email)
                
                if cursor.fetchone()[0] > 0:
                    return standardize_error_response('Email already exists', 'EMAIL_EXISTS', 409)

                # Check if username already exists
                cursor.execute("""
                    SELECT COUNT(*) FROM USER_PROFILES WHERE USERNAME = :username
                """, username=username)
                
                if cursor.fetchone()[0] > 0:
                    return standardize_error_response('Username already exists', 'USERNAME_EXISTS', 409)

                # Hash the password
                hashed_password = generate_password_hash(password)

                # Create a variable to hold the returned ID
                user_id_var = cursor.var(int)

                # Insert the new user into the database with email_verified set to true (no verification needed)
                cursor.execute("""
                    INSERT INTO USER_PROFILES (
                        NAME, USERNAME, EMAIL, PASSWORD, EMAIL_VERIFIED, 
                        CREATED_AT, LAST_LOGIN, ROLE
                    ) VALUES (
                        :name, :username, :email, :password, 1, 
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'user'
                    ) RETURNING ID INTO :user_id
                """, 
                    name=name, 
                    username=username, 
                    email=email, 
                    password=hashed_password,
                    user_id=user_id_var
                )
                
                # Get the user ID from the returned variable
                user_id = user_id_var.getvalue()[0]
                connection.commit()
                
                # Create user data for token generation
                user_data = {
                    'id': user_id,
                    'name': name,
                    'username': username,
                    'email': email
                }
                
                # Generate a JWT token for the new user
                token = generate_auth_token(user_data)

                logger.info(f"User registered successfully: {email}")
                
                # Return success response with the token and user ID
                return jsonify({
                    'message': 'User registered successfully',
                    'token': token,
                    'user_id': str(user_id)
                }), 201
        
    except Exception as e:
        logger.error(f"Registration error: {str(e)}")
        traceback.print_exc()
        return standardize_error_response('Registration failed', 'SERVER_ERROR', 500)

@user_api.route('/auth/login', methods=['POST'])
def login_user():
    try:
        data = request.json
        email = data.get('email', '').strip().lower()
        password = data.get('password', '').strip()

        if not email or not password:
            return standardize_error_response('Email and password are required', 'VALIDATION_ERROR', 400)

        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Find user by email
                cursor.execute("""
                    SELECT ID, NAME, USERNAME, EMAIL, PASSWORD FROM USER_PROFILES
                    WHERE EMAIL = :email
                """, email=email)
                
                user_row = cursor.fetchone()
                
                if not user_row or not check_password_hash(user_row[4], password):
                    return standardize_error_response('Invalid email or password', 'AUTH_FAILED', 401)

                # Update last login timestamp
                cursor.execute("""
                    UPDATE USER_PROFILES SET LAST_LOGIN = CURRENT_TIMESTAMP
                    WHERE ID = :user_id
                """, user_id=user_row[0])
                
                connection.commit()

                # Create user object for token generation
                user = {
                    'id': user_row[0],
                    'name': user_row[1],
                    'username': user_row[2],
                    'email': user_row[3]
                }
                
                # Generate a JWT token with user details
                token = generate_auth_token(user)

                logger.info(f"User logged in: {email}")
                
                return jsonify({
                    'message': 'Login successful',
                    'token': token
                }), 200
        
    except Exception as e:
        logger.error(f"Login error: {str(e)}")
        traceback.print_exc()
        return standardize_error_response('Login failed', 'SERVER_ERROR', 500)
    
@user_api.route('/auth/logout', methods=['POST'])
@token_required
def logout_user(user_id):
    """
    Logout endpoint that can be used to invalidate the user's current token.
    
    Since JWT tokens are stateless, true logout is handled client-side by 
    removing the token. This endpoint primarily serves as a hook for 
    additional logout-related operations if needed.
    
    Args:
        user_id (int): ID of the user logging out
    
    Returns:
        JSON response indicating successful logout
    """
    try:
        # Log the logout event
        logger.info(f"User with ID {user_id} logged out")
        
        # Return successful logout response
        return jsonify({
            'message': 'Logout successful',
            'status': 'success'
        }), 200
    except Exception as e:
        logger.error(f"Logout error for user {user_id}: {str(e)}")
        return standardize_error_response('Logout failed', 'LOGOUT_ERROR', 500)
    
# Add this method to allow user settings updates
@user_api.route('/user/update-settings', methods=['PUT'])
@token_required
def update_user_settings(user_id):
    try:
        data = request.json
        name = data.get('name', '').strip()
        username = data.get('username', '').strip()

        # Validate input
        if not name and not username:
            return standardize_error_response('At least one field (name or username) is required', 'VALIDATION_ERROR', 400)

        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # Prepare update query dynamically
                update_fields = []
                params = {'user_id': user_id}

                if name:
                    update_fields.append("NAME = :name")
                    params['name'] = name

                if username:
                    # Check if username is already taken
                    cursor.execute("""
                        SELECT COUNT(*) FROM USER_PROFILES 
                        WHERE USERNAME = :username AND ID != :user_id
                    """, username=username, user_id=user_id)
                    
                    if cursor.fetchone()[0] > 0:
                        return standardize_error_response('Username already exists', 'USERNAME_EXISTS', 409)
                    
                    update_fields.append("USERNAME = :username")
                    params['username'] = username

                # If no valid updates, return error
                if not update_fields:
                    return standardize_error_response('No valid update fields provided', 'VALIDATION_ERROR', 400)

                # Construct and execute update query
                update_query = f"""
                    UPDATE USER_PROFILES 
                    SET {', '.join(update_fields)}, UPDATED_AT = CURRENT_TIMESTAMP
                    WHERE ID = :user_id
                """
                
                cursor.execute(update_query, params)
                connection.commit()

                # Fetch updated user details
                cursor.execute("""
                    SELECT NAME, USERNAME, EMAIL FROM USER_PROFILES
                    WHERE ID = :user_id
                """, user_id=user_id)
                
                updated_user = cursor.fetchone()

                logger.info(f"User settings updated for user ID: {user_id}")
                
                return jsonify({
                    'message': 'User settings updated successfully',
                    'user': {
                        'name': updated_user[0],
                        'username': updated_user[1],
                        'email': updated_user[2]
                    }
                }), 200
        
    except Exception as e:
        logger.error(f"User settings update error: {str(e)}")
        traceback.print_exc()
        return standardize_error_response('Failed to update user settings', 'SERVER_ERROR', 500)

# Optional: Add a route to change password
@user_api.route('/user/change-password', methods=['PUT'])
@token_required
def change_user_password(user_id):
    try:
        data = request.json
        current_password = data.get('current_password', '').strip()
        new_password = data.get('new_password', '').strip()

        # Validate password
        if not current_password or not new_password:
            return standardize_error_response('Current and new passwords are required', 'VALIDATION_ERROR', 400)

        # Validate new password strength
        if not re.match(r'^(?=.*[A-Za-z])(?=.*\d)[A-Za-z\d]{8,}$', new_password):
            return standardize_error_response(
                'New password must be at least 8 characters and include letters and numbers',
                'VALIDATION_ERROR',
                400
            )

        with get_jsondb_connection() as connection:
            with connection.cursor() as cursor:
                # First, verify current password
                cursor.execute("""
                    SELECT PASSWORD FROM USER_PROFILES
                    WHERE ID = :user_id
                """, user_id=user_id)
                
                stored_password = cursor.fetchone()[0]
                
                if not check_password_hash(stored_password, current_password):
                    return standardize_error_response('Current password is incorrect', 'AUTH_FAILED', 401)

                # Hash the new password
                new_hashed_password = generate_password_hash(new_password)

                # Update password
                cursor.execute("""
                    UPDATE USER_PROFILES 
                    SET PASSWORD = :new_password, 
                        PASSWORD_RESET_AT = CURRENT_TIMESTAMP
                    WHERE ID = :user_id
                """, new_password=new_hashed_password, user_id=user_id)
                
                connection.commit()

                logger.info(f"Password changed for user ID: {user_id}")
                
                return jsonify({
                    'message': 'Password changed successfully'
                }), 200
        
    except Exception as e:
        logger.error(f"Password change error: {str(e)}")
        traceback.print_exc()
        return standardize_error_response('Failed to change password', 'SERVER_ERROR', 500)

# Ensure the blueprint is returned for registration in the main app
if __name__ == "__main__":
    print("User API module loaded")
import time
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import google.generativeai as genai
import os
import requests
import fitz  # PyMuPDF
from tenacity import retry, stop_after_attempt, wait_exponential
import oracledb
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

ORACLE_USER = "RIAD"
ORACLE_PASSWORD = "OracleDatabase&2025"  # Replace with your actual password
ORACLE_DSN_JSONDB = "jsondbchecking_high"  # Replace with your actual DSN

JSON_COLLECTION_NAME = "scraped_data"
URLS_COLLECTION = "discovered_urls"  # New collection
SCRAPPING_LIMIT = 5000
REQUEST_DELAY = 2  # Delay in seconds between requests
CHUNK_SIZE = 9000  # Maximum characters for each chunk (leave some buffer)

# Initialize Oracle JSON DB Connection at the module level
db_connection = None

safety_settings = [
    {
        "category": "HARM_CATEGORY_HATE_SPEECH",
        "threshold": "BLOCK_NONE",
    },
    {
        "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
        "threshold": "BLOCK_NONE",
    },
    {
        "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
        "threshold": "BLOCK_NONE",
    },
    {
        "category": "HARM_CATEGORY_HARASSMENT",
        "threshold": "BLOCK_NONE",
    },
]
genai.configure(api_key="AIzaSyDm4AuF7vILbSbwrT5hO0JlF3nq7UDozl0")  # Replace with your actual Gemini API key
model = genai.GenerativeModel('gemini-2.0-flash', safety_settings=safety_settings)
scraped_count = 0  # global counter

def connect_to_jsondb():
    try:
        connection = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=ORACLE_DSN_JSONDB,
            config_dir="Wallet_jsondbchecking",
            wallet_location="Wallet_jsondbchecking",
            wallet_password=ORACLE_PASSWORD
        )
        print("Connected to JSON Database!")
        return connection
    except Exception as e:
        print(f"Error connecting to JSON DB: {e}")
        return None

def initialize_json_database(jsondb_connection, table_name):
    """
    Initializes the JSON database table if it doesn't exist.

    Args:
        jsondb_connection: The Oracle database connection object.
        table_name: The name of the table to check and create.
    """
    try:
        with jsondb_connection.cursor() as cursor:
            # Check if table exists in the current user's schema
            sql = """
                SELECT COUNT(*)
                FROM USER_TABLES
                WHERE TABLE_NAME = :table_name
            """
            cursor.execute(sql, table_name=table_name.upper())  # TABLE_NAME is typically stored in uppercase
            count = cursor.fetchone()[0]

            if count == 0:
                print(f"Table {table_name} doesn't exist for user {ORACLE_USER}. Creating it now...")
                # Create the table in the current user's schema
                sql = f"""
                    CREATE TABLE {table_name} (
                        ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        DATA CLOB CHECK (DATA IS JSON),
                        CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                cursor.execute(sql)  # No bind variables for table names

                # Optionally create an index for better JSON query performance
                sql = f"""
                    CREATE SEARCH INDEX {table_name}_json_idx ON {table_name} (DATA)
                    FOR JSON
                """
                try:
                    cursor.execute(sql)
                except Exception as e:
                    print(f"Error creating search index (may not be supported): {e}")

                jsondb_connection.commit()
                print(f"Table {table_name} created successfully in {ORACLE_USER}'s schema!")
            else:
                print(f"Table {table_name} already exists in {ORACLE_USER}'s schema.")

        return True
    except Exception as e:
        print(f"Error initializing JSON database table {table_name}: {e}")
        return False

# Connect to the database
jsondb_connection = connect_to_jsondb()
if not jsondb_connection:
    print("Failed to connect to JSON DB. Exiting.")
    exit()

# Initialize the necessary tables
if not initialize_json_database(jsondb_connection, JSON_COLLECTION_NAME):
    print(f"Failed to initialize table {JSON_COLLECTION_NAME}. Exiting.")
    jsondb_connection.close()
    exit()

if not initialize_json_database(jsondb_connection, URLS_COLLECTION):
    print(f"Failed to initialize table {URLS_COLLECTION}. Exiting.")
    jsondb_connection.close()
    exit()

def init_driver():
    """Initialize Selenium WebDriver with appropriate options"""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")  # Required for root user
    options.add_argument("--disable-dev-shm-usage")  # Overcome limited resource issues
    options.add_argument("--disable-gpu")  # Disable GPU hardware acceleration.  Important for headless environments.
    options.add_argument("--enable-unsafe-swiftshader")  # Use SwiftShader for software WebGL (if needed, and with caution)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    # Create a unique user data directory using timestamp
    unique_dir = f"/tmp/chrome_data_{int(time.time())}"
    options.add_argument(f"--user-data-dir={unique_dir}")

    # Instead of using webdriver_manager, specify Chrome directly since you're on Linux
    driver = webdriver.Chrome(options=options)
    driver.temp_dir = unique_dir  # Store the temp directory path for cleanup
    return driver

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def store_url_in_jsondb(url, base_url):
    """Store discovered URL in Oracle JSON DB with retry."""
    global scraped_count
    if scraped_count >= SCRAPPING_LIMIT:
        print("Reached maximum limit of scrapping URLs !!!")
        return False
    try:
        connection = connect_to_jsondb()
        if not connection:
            print("Failed to connect to JSON DB, cannot store URL.")
            return False

        cursor = connection.cursor()
        sql = f"""
            SELECT COUNT(*) FROM {URLS_COLLECTION} WHERE JSON_VALUE(data, '$.url') = :url
        """
        cursor.execute(sql, url=url)
        count = cursor.fetchone()[0]

        if count == 0:
            data = {
                'url': url,
                'base_url': base_url,
                'discovered_at': time.time(),
                'is_scraped': False
            }
            sql = f"""
                INSERT INTO {URLS_COLLECTION} (data) VALUES (:data)
            """
            cursor.execute(sql, data=json.dumps(data))
            connection.commit()
            print(f"Stored URL in JSON DB: {url}")
            time.sleep(REQUEST_DELAY)
            return True
        else:
            print(f"URL already exists in JSON DB: {url}")
            return False
    except Exception as e:
        print(f"Error storing URL in JSON DB: {e}")
        raise  # Re-raise exception for tenacity
    finally:
        if connection:
            cursor.close()
            connection.close()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_page(driver, url, timeout=10):
    """Fetch webpage and wait for content to load with retry."""
    try:
        driver.get(url)
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        return driver.page_source
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        raise  # Re-raise the exception for tenacity to handle

def chunk_text(text, chunk_size=CHUNK_SIZE):
    """Chunk text into smaller segments."""
    chunks = []
    for i in range(0, len(text), chunk_size):
        chunks.append(text[i:i + chunk_size])
    return chunks

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def store_in_jsondb(url, text):
    """Store scraped data (chunked if necessary) in Oracle JSON DB with retry."""
    global scraped_count
    if scraped_count >= SCRAPPING_LIMIT:
        print("Reached maximum limit of scrapping URLs !!!")
        return False

    try:
        connection = connect_to_jsondb()
        if not connection:
            print("Failed to connect to JSON DB, cannot store data.")
            return False

        cursor = connection.cursor()
        sql = f"""
            SELECT COUNT(*) FROM {JSON_COLLECTION_NAME} WHERE JSON_VALUE(data, '$.url') = :url
        """
        cursor.execute(sql, url=url)
        count = cursor.fetchone()[0]

        if count > 0:
            print(f"{url} already exists in JSON DB.")
            return False

        if len(text) > CHUNK_SIZE:
            chunks = chunk_text(text)
            for i, chunk in enumerate(chunks):
                data = {
                    'url': f"{url}_chunk_{i}",
                    'original_url': url,  # Store the original URL
                    'chunk_index': i,
                    'text': chunk,
                    'timestamp': time.time()
                }
                sql = f"""
                    INSERT INTO {JSON_COLLECTION_NAME} (data) VALUES (:data)
                """
                cursor.execute(sql, data=json.dumps(data))
                print(f"Stored chunk {i+1} of {url} in JSON DB.")
        else:
            data = {
                'url': url,
                'text': text,
                'timestamp': time.time()
            }
            sql = f"""
                INSERT INTO {JSON_COLLECTION_NAME} (data) VALUES (:data)
            """
            cursor.execute(sql, data=json.dumps(data))
            print(f"Stored {url} in JSON DB.")

        connection.commit()
        scraped_count += 1
        time.sleep(REQUEST_DELAY)
        return True

    except Exception as e:
        print(f"Error storing in JSON DB: {e}")
        raise  # Re-raise exception for tenacity
    finally:
        if connection:
            cursor.close()
            connection.close()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def download_pdf(pdf_url, download_folder="downloads"):
    """Downloads a PDF from the given URL and saves it with a safe filename."""
    os.makedirs(download_folder, exist_ok=True)

    # Debug: Print the incoming URL
    print(f"Downloading PDF from: {pdf_url}")

    # Validate URL starts with http or https
    if not pdf_url.startswith(("http://", "https://")):
        print(f"Invalid URL: {pdf_url}")
        return None

    # Generate a safe filename by replacing invalid characters
    safe_filename = pdf_url.replace("://", "_").replace("/", "_")
    pdf_path = os.path.join(download_folder, safe_filename)

    # Download the PDF
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(pdf_url, stream=True, headers=headers, timeout=30)  # Add timeout
        response.raise_for_status()  # Raise an error for bad status codes (4xx, 5xx)

        with open(pdf_path, "wb") as pdf_file:
            for chunk in response.iter_content(chunk_size=1024):
                pdf_file.write(chunk)

        print(f"PDF downloaded successfully: {pdf_path}")
        return pdf_path
    except requests.exceptions.RequestException as e:
        print(f"Failed to download PDF. Error: {e}")
        raise  # Re-raise exception for tenacity

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def store_pdf_data_in_jsondb(pdf_url, text):
    """Store the URL and extracted text in Oracle JSON DB. Chunk if necessary."""
    global scraped_count
    if scraped_count >= SCRAPPING_LIMIT:
        print("Reached maximum limit of scrapping URLs !!!")
        return False

    try:
        connection = connect_to_jsondb()
        if not connection:
            print("Failed to connect to JSON DB, cannot store PDF data.")
            return

        cursor = connection.cursor()
        sql = f"""
            SELECT COUNT(*) FROM {JSON_COLLECTION_NAME} WHERE JSON_VALUE(data, '$.url') = :url
        """
        cursor.execute(sql, url=pdf_url)
        count = cursor.fetchone()[0]

        if count > 0:
            print(f"PDF with URL {pdf_url} already exists. Skipping saving text.")
            return

        if len(text) > CHUNK_SIZE:
            chunks = chunk_text(text)
            for i, chunk in enumerate(chunks):
                parsed_url = urlparse(pdf_url)
                pdf_name = parsed_url.path.split('/')[-1]

                data = {
                    "url": f"{pdf_url}_chunk_{i}",  # Unique URL for chunk
                    "original_url": pdf_url,  # Store original URL for reference
                    "pdf_name": pdf_name,
                    "chunk_index": i,  # chunk id
                    "text": chunk
                }

                sql = f"""
                    INSERT INTO {JSON_COLLECTION_NAME} (data) VALUES (:data)
                """
                cursor.execute(sql, data=json.dumps(data))
                print(f"Data stored for {pdf_name} chunk {i+1}")
        else:
            parsed_url = urlparse(pdf_url)
            pdf_name = parsed_url.path.split('/')[-1]

            data = {
                "url": pdf_url,
                "pdf_name": pdf_name,
                "text": text
            }

            sql = f"""
                INSERT INTO {JSON_COLLECTION_NAME} (data) VALUES (:data)
            """
            cursor.execute(sql, data=json.dumps(data))
            print(f"Data stored for {pdf_name}")

        connection.commit()
        scraped_count += 1
        time.sleep(REQUEST_DELAY)

    except Exception as e:
        print(f"Error storing data in JSON DB: {e}")
        raise  # Re-raise exception for tenacity

    finally:
        if connection:
            cursor.close()
            connection.close()
def process_pdf(pdf_url):
    """Download, extract text, store, and delete local PDF."""
    try:
        pdf_path = download_pdf(pdf_url)
        if pdf_path:
            try:
                text = extract_text_from_pdf(pdf_path)
                store_pdf_data_in_jsondb(pdf_url, text)
            except Exception as extract_error:
                print(f"Error extracting or storing PDF text: {extract_error}")
            finally:
                delete_local_pdf(pdf_path)
        else:
            print(f"Failed to download PDF from {pdf_url}, skipping processing.")
    except Exception as download_error:
        print(f"Failed to process PDF {pdf_url} after multiple retries: {download_error}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def preprocess_text(html_content):
    """Preprocess text with Gemini AI with retry."""
    try:
        response = model.generate_content(
            f"Convert the following HTML content into plain, readable text by removing all HTML tags, attributes, and syntax while preserving the full textual information: {html_content}."
            f"Ensure that no meaningful content is lost in the conversion. If the provided HTML content is empty, contains no useful text, or cannot be processed, return an empty string ('')."
        )
        return response.text
    except Exception as e:
        print(f"Error preprocessing with Gemini: {e}")
        raise  # Re-raise exception for tenacity

def discover_all_links(driver, start_url, visited):
    """First phase: Discover all links starting from the start URL"""
    print("Starting link discovery phase...")
    global scraped_count

    to_visit = [start_url]  # Changed to list for Oracle SQL compatibility
    discovered_links = set()
    visited_for_links = visited

    # Extract the base domain dynamically from start_url
    parsed_url = urlparse(start_url)
    base_domain = parsed_url.netloc  # Get the domain from the URL
    discovered_links.add(start_url)

    while to_visit:
        if scraped_count >= SCRAPPING_LIMIT:
            print("Reached maximum limit of scrapping URLs !!!")
            return discovered_links

        current_url = to_visit.pop(0)

        print(f"Discovering links from: {current_url}")
        html = fetch_page(driver, current_url)
        if html:
            new_links = extract_links_from_html(html, current_url)

            # Only add links that match the start_url pattern
            for link in new_links:
                if scraped_count >= SCRAPPING_LIMIT:
                    print("Reached maximum limit of scrapping URLs !!!")
                    return discovered_links

                if base_domain in link:
                    if link not in discovered_links and link not in visited_for_links:
                        discovered_links.add(link)
                        if link not in to_visit:
                            to_visit.append(link)
                elif link.endswith(".pdf"):
                    print(f"Extracting text from PDF: {link}")
                    process_pdf(link)
                else:
                    print(f'Ignoring external link: {link}')

        visited_for_links.add(current_url)

    print(f"Total unique links discovered: {len(discovered_links)}")
    return discovered_links

def process_html_and_extract_info(html_content):
    """Extract text content from HTML"""
    try:
        soup = BeautifulSoup(html_content, "html.parser")

        for tag in soup(["script", "style", "meta", "link", "head", "noscript", "iframe", "svg"]):
            tag.extract()

        main_content = soup.find("main") or soup.find("article") or soup.find("body")
        if main_content:
            return main_content.get_text(separator="\n", strip=True)
        else:
            return soup.get_text(separator="\n", strip=True)

    except Exception as e:
        print(f"Error processing HTML: {e}")
        return ""

def scrape_content(driver, urls, visited):
    """Second phase: Scrape content from discovered URLs"""
    print("\nStarting content scraping phase...")
    global scraped_count
    for url in urls:
        if scraped_count >= SCRAPPING_LIMIT:
            print("Reached maximum limit of scrapping URLs !!!")
            return

        if url.endswith(".pdf"):
            print(f"Skipping PDF {url} in scrape_content as it's handled in discover_all_links")
            continue

        print(f"Scraping content from: {url}")
        html = fetch_page(driver, url)

        if not html:
            continue

        text = process_html_and_extract_info(html)

        if not text.strip():
            print(f"No content found in {url}")
            continue

        preprocessed_html = preprocess_text(text)
        if not preprocessed_html:
            continue

        if store_in_jsondb(url, preprocessed_html):
            visited.add(url)

def load_visited_from_jsondb():
    """Load previously visited URLs from Oracle JSON DB"""
    visited = set()
    try:
        connection = connect_to_jsondb()
        if not connection:
            print("Failed to connect to JSON DB, cannot load visited URLs.")
            return visited

        cursor = connection.cursor()
        sql = f"""
            SELECT JSON_VALUE(data, '$.url') FROM {URLS_COLLECTION} WHERE JSON_VALUE(data, '$.is_scraped') = 'true'
        """
        cursor.execute(sql)
        rows = cursor.fetchall()
        for row in rows:
            visited.add(row[0])

    except Exception as e:
        print(f"Error loading visited URLs from JSON DB: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

    return visited

def extract_links_from_html(html_content, base_url):
    """Extract all valid links from HTML content"""
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        links = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href and not href.startswith(("#", "javascript:", "mailto:", "tel:")):
                full_url = urljoin(base_url, href)
                links.add(full_url)

        return links
    except Exception as e:
        print(f"Error extracting links from {base_url}: {e}")
        return set()

def delete_local_pdf(pdf_path):
    """Delete the local PDF file."""
    try:
        if os.path.exists(pdf_path):
            os.remove(pdf_path)
            print(f"Deleted local file: {pdf_path}")
        else:
            print(f"File not found: {pdf_path}")
    except Exception as e:
        print(f"Error deleting file: {e}")

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file."""
    text = ""
    doc = fitz.open(pdf_path)
    for page in doc:
        text += page.get_text("text")
    return text

def main(start_urls, max_retries=3):
    """Main function implementing two-phase scraping"""
    driver = None
    try:
        driver = init_driver()
        visited = load_visited_from_jsondb()
        print(f"Loaded {len(visited)} visited URLs from JSON DB.")
        global scraped_count
        scraped_count = len(visited)

        # if scraped_count >= SCRAPPING_LIMIT:
        #     return "Scrapping limit is crossed"

        # for start_url in start_urls:
        #     print(f"\nProcessing website: {start_url}")

        #     # Phase 1: Discover all links
        #     all_links = discover_all_links(driver, start_url, visited)
        #     print(f"\nDiscovered {len(all_links)} unique URLs")
        #     print(f"\nStoring all unique URLs")

        #     # After discovering all links, store them in Oracle JSON DB
        #     for link in all_links:
        #         store_url_in_jsondb(link, start_url)

        #     # Phase 2: Scrape content from discovered links
        #     scrape_content(driver, all_links, visited)

    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        if driver:
            driver.quit()
        print("\nScraping complete.")

if __name__ == "__main__":
    # Ensure Oracle Client libraries are in the PATH
    # before running this script.

    # Example:
    # export LD_LIBRARY_PATH=$ORACLE_HOME/lib
    # export PATH=$ORACLE_HOME/bin:$PATH

    start_urls = ["https://www.countyofmonterey.gov/government/departments-i-z/social-services/community-benefits"]
    #First Create tables
    # """
    # CREATE TABLE discovered_urls_new_2 (
    # data VARCHAR2(4000)
    # CONSTRAINT ensure_json CHECK (data IS JSON)
    # );
    # CREATE TABLE scraped_data_new_2 (
    # data VARCHAR2(4000)
    # CONSTRAINT ensure_json_data CHECK (data IS JSON)
    # );
    # """
    # db_connection = connect_to_jsondb()
    # if not db_connection:
    #     print("Failed to connect to JSON DB, exiting.")
    #     exit()
    main(start_urls)
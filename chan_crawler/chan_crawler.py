from chan_client import ChanClient
import logging
from pyfaktory import Client, Consumer, Job, Producer
import datetime
import psycopg2
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
import json
import time
import os
import atexit
register_adapter(dict, Json)

from chan_content_analyzer import ChanContentAnalyzer
content_analyzer = ChanContentAnalyzer()

from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configuration constants
DATABASE_URL = os.getenv('DATABASE_URL')
FAKTORY_SERVER_URL = os.getenv('FAKTORY_SERVER_URL')
API_KEY = os.getenv('API_KEY')
DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'
MAX_REQUESTS_PER_MINUTE = 60
BATCH_SIZE = 100

# Set up logging for background execution
log_dir = 'logs'
pid_dir = 'pid'
os.makedirs(log_dir, exist_ok=True)
os.makedirs(pid_dir, exist_ok=True)

logger = logging.getLogger("4chan client")
logger.propagate = False
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)

# File handler with more detailed formatting
file_handler = logging.FileHandler(os.path.join(log_dir, 'crawler.log'))
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s"
))
logger.addHandler(file_handler)

def cleanup():
    """Cleanup function to run on exit"""
    pid_file = os.path.join(pid_dir, 'crawler.pid')
    if os.path.exists(pid_file):
        os.remove(pid_file)
    logger.info("Crawler shutdown complete")

#cleanup function
atexit.register(cleanup)

# Saving the PID for process management
with open(os.path.join(pid_dir, 'crawler.pid'), 'w') as f:
    f.write(str(os.getpid()))

def clear_existing_jobs():
    """Clear all existing jobs from Faktory queues"""
    try:
        with Client(faktory_url=FAKTORY_SERVER_URL) as client:
            client.queue_remove_all("crawl-catalog")
            client.queue_remove_all("crawl-thread")
            logger.info("Cleared existing jobs from all queues")
    except Exception as e:
        logger.error(f"Error clearing job queues: {e}")

def load_boards():
    try:
        with open('boards.json', 'r') as f:
            config = json.load(f)
            boards = config.get('boards', [])
            logger.info(f"Loaded boards from config: {boards}")
            return boards
    except FileNotFoundError:
        logger.error("boards.json file not found")
        return []
    except json.JSONDecodeError:
        logger.error("Error parsing boards.json file")
        return []

BOARDS = load_boards()

def thread_numbers_from_catalog(catalog):
    thread_numbers = []
    if not catalog:
        logger.warning("Received empty catalog")
        return thread_numbers
        
    try:
        for page in catalog:
            if "threads" not in page:
                logger.warning(f"No threads found in page: {page}")
                continue
            for thread in page["threads"]:
                if "no" not in thread:
                    logger.warning(f"No thread number found in thread: {thread}")
                    continue
                thread_numbers.append(thread["no"])
        logger.debug(f"Extracted {len(thread_numbers)} thread numbers from catalog")
    except Exception as e:
        logger.error(f"Error extracting thread numbers: {e}")
    return thread_numbers

def find_dead_threads(previous_catalog_thread_numbers, current_catalog_thread_numbers):
    return set(previous_catalog_thread_numbers) - set(current_catalog_thread_numbers)

def crawl_thread(board, thread_number):
    # Check if board is still in current configuration
    if board not in BOARDS:
        logger.info(f"Skipping thread {thread_number} from board {board} as it's no longer monitored")
        return

    logger.info(f"Starting to crawl thread {thread_number} from board {board}")
    chan_client = ChanClient(api_key=API_KEY)
    
    max_retries = 3
    for attempt in range(max_retries):
        thread_data = chan_client.get_thread(board, thread_number)
        if thread_data is not None:
            break
        if attempt < max_retries - 1:
            logger.warning(f"Retry {attempt + 1}/{max_retries} for thread {thread_number}")
            time.sleep(2 ** attempt)
    
    if thread_data is None:
        logger.warning(f"Thread {thread_number} from board {board} not found or returned no data. Skipping.")
        return

    # Get current timestamp for the crawl
    crawl_time = datetime.datetime.now(datetime.UTC)

    try:
        with psycopg2.connect(dsn=DATABASE_URL) as conn:
            with conn.cursor() as cur:
                for post in thread_data["posts"]:
                    post_number = post["no"]
                    q = """
                    INSERT INTO posts (board, thread_number, post_number, data, crawl_datetime)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (board, thread_number, post_number)
                    DO UPDATE SET 
                        data = EXCLUDED.data,
                        crawl_datetime = EXCLUDED.crawl_datetime
                    RETURNING id
                    """
                    cur.execute(q, (board, thread_number, post_number, post, crawl_time))
                    db_id = cur.fetchone()[0]
                    logger.debug(f"Inserted/Updated DB id: {db_id}")
                    
                    # Analyze content in real-time
                    try:
                        content_analyzer.analyze_content(post)
                        logger.debug(f"Content analysis completed for post {post_number}")
                    except Exception as e:
                        logger.error(f"Content analysis failed for post {post_number}: {e}")
                
                conn.commit()
        logger.info(f"Finished crawling thread {thread_number} from board {board}")
    except psycopg2.Error as e:
        logger.error(f"Database error in crawl_thread: {e}")

def crawl_catalog(board, previous_catalog_thread_numbers=[]):
    # Check if board is still in current configuration
    if board not in BOARDS:
        logger.info(f"Skipping catalog crawl for {board} as it's no longer monitored")
        return

    logger.info(f"Starting to crawl catalog for board {board}")
    chan_client = ChanClient(api_key=API_KEY)
    
    max_retries = 3
    current_catalog = None
    for attempt in range(max_retries):
        current_catalog = chan_client.get_catalog(board)
        if current_catalog is not None:
            break
        if attempt < max_retries - 1:
            logger.warning(f"Retry {attempt + 1}/{max_retries} for board {board}")
            time.sleep(2 ** attempt)
    
    if current_catalog is None:
        logger.error(f"Failed to fetch catalog for board {board} after {max_retries} attempts")
        return
        
    current_catalog_thread_numbers = thread_numbers_from_catalog(current_catalog)
    dead_threads = find_dead_threads(previous_catalog_thread_numbers, current_catalog_thread_numbers)
    
    logger.info(f"Board: {board}, Dead threads: {dead_threads}")

    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        crawl_thread_jobs = [Job(jobtype="crawl-thread", args=(board, dead_thread), queue="crawl-thread")
                             for dead_thread in dead_threads]
        producer.push_bulk(crawl_thread_jobs)

        run_at = (datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=5)).isoformat()
        logger.debug(f"Next catalog crawl for {board} scheduled at: {run_at}")
        job = Job(jobtype="crawl-catalog",
                  args=(board, current_catalog_thread_numbers),
                  queue="crawl-catalog",
                  at=run_at)
        producer.push(job)
    logger.info(f"Finished crawling catalog for board {board}")

def crawl_all_boards():
    logger.info(f"Starting to crawl all boards")
    for board in BOARDS:
        crawl_catalog(board)

def schedule_initial_crawls():
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        for board in BOARDS:
            job = Job(jobtype="crawl-catalog", args=(board, []), queue="crawl-catalog")
            producer.push(job)
            logger.info(f"Scheduled initial crawl for board: {board}")

if __name__ == "__main__":
    if not BOARDS:
        logger.error("No boards configured in boards.json. Exiting.")
        exit(1)
        
    logger.info(f"Starting 4chan crawler with boards: {BOARDS}")
    
    clear_existing_jobs()
    
    # Log currently monitored boards vs all boards in database
    try:
        with psycopg2.connect(dsn=DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT board FROM posts")
                stored_boards = set(board[0] for board in cur.fetchall())
                logger.info(f"Boards in database: {stored_boards}")
                logger.info(f"Currently monitoring boards: {BOARDS}")
    except psycopg2.Error as e:
        logger.error(f"Database error while checking stored boards: {e}")
    
    schedule_initial_crawls()
    
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(client=client, queues=["crawl-catalog", "crawl-thread"], concurrency=5)
        consumer.register("crawl-catalog", crawl_catalog)
        consumer.register("crawl-thread", crawl_thread)
        consumer.run()
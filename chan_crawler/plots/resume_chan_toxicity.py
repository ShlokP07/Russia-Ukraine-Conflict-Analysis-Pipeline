import logging
import psycopg2
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import List, Tuple
import html
import re
import requests
import json
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('toxicity_analysis_resumed.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL')

# Define date range
START_DATE = datetime(2024, 11, 1, tzinfo=timezone.utc)
END_DATE = datetime(2024, 11, 15, tzinfo=timezone.utc)

# Resume configuration
RESUME_FROM_POST = 154695  # The last processed post
CREATE_NEW_TABLE = False  # Set to True if you want to create a new table instead of updating existing

class ToxicityAnalysisClient:
    TOXICITY_API_BASE = "https://api.moderatehatespeech.com/api/v1/moderate/"
    CONFIDENCE_THRESHOLD = 0.85

    def __init__(self):
        self.toxicity_api_key = os.getenv('TOXICITY_API_KEY')
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session = requests.Session()
        self.session.mount('https://', HTTPAdapter(max_retries=retry_strategy))

    def get_toxicity_classification(self, text: str) -> int:
        if not text:
            return 0

        data = {
            "token": self.toxicity_api_key,
            "text": text
        }

        try:
            response = self.session.post(
                self.TOXICITY_API_BASE, 
                json=data,
                timeout=10
            )
            response.raise_for_status()
            result = response.json()

            classification = result.get('class')
            confidence = float(result.get('confidence', 0))

            if confidence >= self.CONFIDENCE_THRESHOLD:
                score = -1 if classification == 'flag' else 1
            else:
                score = 0
                
            # Extra validation
            if score not in (-1, 0, 1):
                logger.warning(f"Unexpected toxicity score {score}, defaulting to 0")
                score = 0
                
            return score

        except (requests.exceptions.RequestException, json.JSONDecodeError, ValueError, TypeError, KeyError) as e:
            logger.error(f"Error in toxicity analysis: {str(e)}")
            time.sleep(1)  # Basic rate limiting
            return 0

def setup_database():
    """Create necessary tables and indexes for 4chan data and toxicity analysis."""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                logger.info("Starting database setup...")
                
                if CREATE_NEW_TABLE:
                    # Create a new table for resumed analysis
                    new_table_name = "chan_toxicity_analysis_resumed"
                    cur.execute(f"""
                        DROP TABLE IF EXISTS {new_table_name};
                        
                        CREATE TABLE {new_table_name} (
                            id BIGSERIAL PRIMARY KEY,
                            post_number BIGINT NOT NULL,
                            thread_number BIGINT NOT NULL,
                            board TEXT NOT NULL,
                            toxicity_score INTEGER NOT NULL,
                            created_utc TIMESTAMP WITH TIME ZONE NOT NULL,
                            CONSTRAINT toxicity_score_check CHECK (toxicity_score IN (-1, 0, 1)),
                            UNIQUE(board, thread_number, post_number)
                        );

                        CREATE INDEX idx_4chan_toxicity_board_resumed 
                            ON {new_table_name}(board);
                        CREATE INDEX idx_4chan_toxicity_created_resumed 
                            ON {new_table_name}(created_utc);
                        CREATE INDEX idx_4chan_toxicity_score_resumed 
                            ON {new_table_name}(toxicity_score);
                    """)
                    logger.info(f"Created new table: {new_table_name}")
                
                conn.commit()
                logger.info("Database setup completed successfully")

    except Exception as e:
        logger.error(f"Error in database setup: {str(e)}")
        raise

last_processed_id = RESUME_FROM_POST  # Add this as a global variable at the top of the script

def get_unanalyzed_posts(cur, batch_size: int = 100) -> List[Tuple]:
    """Get unanalyzed posts within the specified date range."""
    global last_processed_id
    table_name = "chan_toxicity_analysis_resumed" if CREATE_NEW_TABLE else "chan_toxicity_analysis"
    
    # Get total count first with more specific conditions
    cur.execute("""
        SELECT COUNT(DISTINCT p.id)
        FROM posts p
        WHERE p.id > %s
        AND (p.data->>'time')::numeric BETWEEN %s AND %s
        AND NOT EXISTS (
            SELECT 1 
            FROM """ + table_name + """ f
            WHERE f.board = p.board 
            AND f.thread_number = p.thread_number 
            AND f.post_number = p.post_number
        )
    """, (last_processed_id, START_DATE.timestamp(), END_DATE.timestamp()))
    
    total_pending = cur.fetchone()[0]
    logger.info(f"Total unanalyzed posts remaining in date range: {total_pending:,}")

    # Fetch batch with more specific conditions
    cur.execute("""
        SELECT p.id, p.post_number, p.thread_number, p.board, p.data
        FROM posts p
        WHERE p.id > %s
        AND (p.data->>'time')::numeric BETWEEN %s AND %s
        AND NOT EXISTS (
            SELECT 1 
            FROM """ + table_name + """ f
            WHERE f.board = p.board 
            AND f.thread_number = p.thread_number 
            AND f.post_number = p.post_number
        )
        ORDER BY p.id
        LIMIT %s
    """, (last_processed_id, START_DATE.timestamp(), END_DATE.timestamp(), batch_size))

    posts = cur.fetchall()
    if posts:
        logger.info(f"Fetched batch of {len(posts)} posts for analysis (IDs {posts[0][0]} to {posts[-1][0]})")
        last_processed_id = posts[-1][0]  # Update the last processed ID
    return posts

def clean_text(text: str) -> str:
    """Clean and prepare text for toxicity analysis."""
    if not text:
        return ""

    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
    text = re.sub(r'[^\w\s,.!?]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()

    return text

def process_posts(posts: List[Tuple], toxicity_client: ToxicityAnalysisClient) -> List[Tuple]:
    """Process a batch of posts and prepare them for insertion."""
    processed_posts = []
    start_time = time.time()
    error_count = 0
    skip_count = {"no_comment": 0, "no_cleaned_text": 0, "invalid_types": 0}

    for i, post in enumerate(posts, 1):
        try:
            post_id, post_number, thread_number, board, data = post
            comment_text = data.get('com', '')
            if not comment_text:
                skip_count["no_comment"] += 1
                logger.debug(f"Skipping post {post_id}: no comment text")
                continue

            cleaned_text = clean_text(comment_text)
            if not cleaned_text:
                skip_count["no_cleaned_text"] += 1
                logger.debug(f"Skipping post {post_id}: no cleaned text")
                continue

            toxicity_score = toxicity_client.get_toxicity_classification(cleaned_text)
            
            # Strict validation of toxicity score
            if not isinstance(toxicity_score, int) or toxicity_score not in (-1, 0, 1):
                logger.warning(f"Invalid toxicity score type or value: {toxicity_score} for post {post_id}")
                toxicity_score = 0
                error_count += 1
            
            created_utc = datetime.fromtimestamp(int(data.get('time', 0)), tz=timezone.utc)
            
            # Validate all fields before adding to processed posts
            if not all([
                isinstance(post_number, int),
                isinstance(thread_number, int),
                isinstance(board, str),
                isinstance(toxicity_score, int),
                isinstance(created_utc, datetime),
                toxicity_score in (-1, 0, 1)
            ]):
                skip_count["invalid_types"] += 1
                logger.warning(f"Invalid data types in post {post_id}, skipping")
                continue

            processed_posts.append((
                post_number,
                thread_number,
                board,
                toxicity_score,
                created_utc
            ))

        except Exception as e:
            logger.error(f"Error processing post {post_id}: {str(e)}")
            error_count += 1
            continue
    
    # Log detailed statistics
    logger.info(f"Processing statistics:")
    logger.info(f"- Total posts in batch: {len(posts)}")
    logger.info(f"- Successfully processed: {len(processed_posts)}")
    logger.info(f"- Skipped - no comment: {skip_count['no_comment']}")
    logger.info(f"- Skipped - no cleaned text: {skip_count['no_cleaned_text']}")
    logger.info(f"- Skipped - invalid types: {skip_count['invalid_types']}")
    logger.info(f"- Errors encountered: {error_count}")
    
    return processed_posts

def analyze_4chan_content(batch_size: int = 100):
    """Process 4chan posts for toxicity analysis in batches."""
    try:
        toxicity_client = ToxicityAnalysisClient()
        start_time = time.time()
        total_processed = 0

        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                logger.info(f"Resuming analysis from post {RESUME_FROM_POST}")
                logger.info(f"Analyzing posts from {START_DATE} to {END_DATE}")
                
                table_name = "chan_toxicity_analysis_resumed" if CREATE_NEW_TABLE else "chan_toxicity_analysis"
                
                while True:
                    batch_start_time = time.time()
                    posts = get_unanalyzed_posts(cur, batch_size)
                    
                    if not posts:
                        logger.info("No more posts to process")
                        break

                    processed_posts = process_posts(posts, toxicity_client)

                    if processed_posts:
                        execute_batch(cur, f"""
                            INSERT INTO {table_name}
                            (post_number, thread_number, board, toxicity_score, created_utc)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (board, thread_number, post_number) DO NOTHING
                        """, processed_posts)

                        total_processed += len(processed_posts)
                        batch_time = time.time() - batch_start_time
                        logger.info(
                            f"Batch completed: {len(processed_posts)} posts in {batch_time:.2f}s "
                            f"({batch_time/len(processed_posts):.2f}s per post). "
                            f"Total processed: {total_processed:,}"
                        )

                    conn.commit()

                if total_processed > 0:
                    total_time = time.time() - start_time
                    avg_time = total_time/total_processed
                    logger.info(
                        f"Processing completed: {total_processed:,} total posts in {total_time:.2f}s "
                        f"({avg_time:.2f}s per post)"
                    )
                else:
                    logger.info(f"No posts processed in the date range")

                # Generate summary statistics
                cur.execute(f"""
                    SELECT
                        board,
                        COUNT(*) as total_posts,
                        SUM(CASE WHEN toxicity_score = -1 THEN 1 ELSE 0 END) as toxic_posts,
                        SUM(CASE WHEN toxicity_score = 1 THEN 1 ELSE 0 END) as normal_posts,
                        SUM(CASE WHEN toxicity_score = 0 THEN 1 ELSE 0 END) as uncertain_posts
                    FROM {table_name}
                    WHERE created_utc BETWEEN %s AND %s
                    GROUP BY board
                """, (START_DATE, END_DATE))

                stats = cur.fetchall()
                logger.info(f"\nToxicity Analysis Summary ({START_DATE.date()} to {END_DATE.date()}):")
                for stat in stats:
                    logger.info(f"Board /{stat[0]}/:")
                    logger.info(f"  Total Posts: {stat[1]:,}")
                    logger.info(f"  Toxic Posts: {stat[2]:,} ({(stat[2]/stat[1]*100):.1f}%)")
                    logger.info(f"  Normal Posts: {stat[3]:,} ({(stat[3]/stat[1]*100):.1f}%)")
                    logger.info(f"  Uncertain Posts: {stat[4]:,} ({(stat[4]/stat[1]*100):.1f}%)")

    except Exception as e:
        logger.error(f"Error in analyze_4chan_content: {str(e)}")
        raise

def main():
    """Main function to run the 4chan toxicity analysis."""
    try:
        logger.info("Starting 4chan toxicity analysis")
        if CREATE_NEW_TABLE:
            setup_database()
        analyze_4chan_content()
        logger.info("4chan toxicity analysis completed successfully")

    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()
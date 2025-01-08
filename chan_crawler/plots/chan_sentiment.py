import logging
import psycopg2
from psycopg2.extras import execute_batch
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import List, Dict, Tuple
import html
import re

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize VADER analyzer
vader = SentimentIntensityAnalyzer()

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL')

# Define date range
START_DATE = datetime(2024, 11, 1, tzinfo=timezone.utc)
END_DATE = datetime(2024, 11, 15, tzinfo=timezone.utc)

def setup_database():
    """Create tables and indexes for 4chan data and sentiment analysis."""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
 
                # Add unique constraint
                cur.execute("""
                    DO $$ 
                    BEGIN 
                        IF NOT EXISTS (
                            SELECT 1 
                            FROM pg_constraint 
                            WHERE conname = 'posts_board_thread_post_unique'
                        ) THEN
                            ALTER TABLE posts 
                            ADD CONSTRAINT posts_board_thread_post_unique 
                            UNIQUE (board, thread_number, post_number);
                        END IF;
                    END $$;
                """)

                # Create 4chan sentiment analysis table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS chan_sentiment_analysis (
                        id BIGSERIAL PRIMARY KEY,
                        post_number BIGINT NOT NULL,
                        thread_number BIGINT NOT NULL,
                        board TEXT NOT NULL,
                        sentiment_score FLOAT NOT NULL CHECK (sentiment_score BETWEEN -1 AND 1),
                        created_utc TIMESTAMP WITH TIME ZONE NOT NULL,
                        UNIQUE(board, thread_number, post_number)
                    );

                    CREATE INDEX IF NOT EXISTS idx_4chan_sentiment_board 
                    ON chan_sentiment_analysis(board);
                    CREATE INDEX IF NOT EXISTS idx_4chan_sentiment_created 
                    ON chan_sentiment_analysis(created_utc);
                    CREATE INDEX IF NOT EXISTS idx_4chan_sentiment_score 
                    ON chan_sentiment_analysis(sentiment_score);
                    CREATE INDEX IF NOT EXISTS idx_4chan_sentiment_thread 
                    ON chan_sentiment_analysis(thread_number);
                """)

                conn.commit()
                logger.info("Database setup completed successfully")

    except Exception as e:
        logger.error(f"Error in database setup: {str(e)}")
        raise

def clean_text(text: str) -> str:
    """
    Clean and prepare text for sentiment analysis.
    Args:
        text (str): Raw text from post
    Returns:
        str: Cleaned text
    """
    if not text:
        return ""
    
    # Decode HTML entities
    text = html.unescape(text)
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    
    # Remove URLs
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
    
    # Remove special characters and extra whitespace
    text = re.sub(r'[^\w\s,.!?]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def analyze_sentiment(text: str) -> float:
    """
    Analyze sentiment in text using VADER and return compound score.
    """
    try:
        if not text or not isinstance(text, str):
            return 0.0
        
        # Clean the text before analysis
        cleaned_text = clean_text(text)
        if not cleaned_text:
            return 0.0
            
        scores = vader.polarity_scores(cleaned_text)
        logger.debug(f"VADER scores for text: {scores}")
        return scores['compound']
    except Exception as e:
        logger.error(f"Error in analyze_sentiment: {str(e)}")
        return 0.0

def get_unanalyzed_posts(cur, batch_size: int = 1000) -> List[Tuple]:
    """
    Get posts within date range that haven't been analyzed yet.
    """
    cur.execute("""
        SELECT p.post_number, p.thread_number, p.board, p.data
        FROM posts p
        LEFT JOIN chan_sentiment_analysis f
        ON p.board = f.board 
        AND p.thread_number = f.thread_number
        AND p.post_number = f.post_number
        WHERE f.id IS NULL
        AND (p.data->>'time')::numeric BETWEEN %s AND %s
        LIMIT %s
    """, (START_DATE.timestamp(), END_DATE.timestamp(), batch_size))
    
    return cur.fetchall()

def process_posts(posts: List[Tuple]) -> List[Tuple]:
    """
    Process a batch of posts and prepare them for insertion.
    """
    processed_posts = []
    
    for post in posts:
        post_number, thread_number, board, data = post
        
        # Extract comment text from the 'com' field
        comment_text = data.get('com', '')
        if not comment_text:
            continue
        
        # Calculate sentiment score
        sentiment_score = analyze_sentiment(comment_text)
        
        # Get created_utc from timestamp
        created_utc = datetime.fromtimestamp(int(data.get('time', 0)), tz=timezone.utc)
        
        processed_posts.append((
            post_number,
            thread_number,
            board,
            sentiment_score,
            created_utc
        ))
    
    return processed_posts

def analyze_4chan_content(batch_size: int = 1000):
    """
    Process 4chan posts for sentiment analysis in batches within date range.
    """
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                total_processed = 0
                
                while True:
                    # Get batch of unanalyzed posts
                    posts = get_unanalyzed_posts(cur, batch_size)
                    if not posts:
                        break
                    
                    # Process the posts
                    processed_posts = process_posts(posts)
                    
                    if processed_posts:
                        # Insert sentiment analysis results in batch
                        execute_batch(cur, """
                            INSERT INTO chan_sentiment_analysis
                            (post_number, thread_number, board, sentiment_score, created_utc)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (board, thread_number, post_number) DO NOTHING
                        """, processed_posts)
                        
                        total_processed += len(processed_posts)
                        logger.info(f"Processed {total_processed} posts")
                    
                    conn.commit()
                
                logger.info(f"Completed processing {total_processed} posts")
                
                # Generate summary statistics for date range
                cur.execute("""
                    SELECT 
                        board,
                        COUNT(*) as total_posts,
                        AVG(sentiment_score) as avg_sentiment,
                        MIN(sentiment_score) as min_sentiment,
                        MAX(sentiment_score) as max_sentiment
                    FROM chan_sentiment_analysis
                    WHERE created_utc BETWEEN %s AND %s
                    GROUP BY board
                """, (START_DATE, END_DATE))
                
                stats = cur.fetchall()
                logger.info(f"\nSentiment Analysis Summary ({START_DATE.date()} to {END_DATE.date()}):")
                for stat in stats:
                    logger.info(f"Board /{stat[0]}/:")
                    logger.info(f"  Total Posts: {stat[1]:,}")
                    logger.info(f"  Average Sentiment: {stat[2]:.3f}")
                    logger.info(f"  Min Sentiment: {stat[3]:.3f}")
                    logger.info(f"  Max Sentiment: {stat[4]:.3f}")
                
    except Exception as e:
        logger.error(f"Error in analyze_4chan_content: {str(e)}")
        raise

def main():
    """Main function to run the 4chan sentiment analysis for specified date range."""
    try:
        logger.info(f"Starting 4chan sentiment analysis for period: {START_DATE.date()} to {END_DATE.date()}")
        
        # Set up database tables
        logger.info("Setting up database tables...")
        setup_database()
        
        # Run sentiment analysis
        logger.info("Starting content analysis...")
        analyze_4chan_content()
        
        logger.info("4chan sentiment analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()
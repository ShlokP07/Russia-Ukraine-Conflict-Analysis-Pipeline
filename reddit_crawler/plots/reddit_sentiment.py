import logging
import psycopg2
from psycopg2.extras import Json
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import os
from dotenv import load_dotenv
from urllib.parse import urlparse

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(name)

# Initialize VADER analyzer
vader = SentimentIntensityAnalyzer()

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

def setup_database() -> None:
    """Create necessary tables and indexes for Reddit data and sentiment analysis."""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Create Reddit sentiment analysis table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS reddit_sentiment_analysis (
                        id BIGSERIAL PRIMARY KEY,
                        content_type TEXT NOT NULL CHECK (content_type IN ('post', 'comment')),
                        content_id TEXT NOT NULL,
                        subreddit TEXT NOT NULL,
                        sentiment_score FLOAT NOT NULL CHECK (sentiment_score BETWEEN -1 AND 1),
                        media_metadata JSONB NOT NULL,
                        created_utc TIMESTAMP WITH TIME ZONE NOT NULL,
                        score INTEGER NOT NULL,
                        num_comments INTEGER,
                        UNIQUE(content_type, content_id)
                    );

                    CREATE INDEX IF NOT EXISTS idx_reddit_sentiment_subreddit 
                    ON reddit_sentiment_analysis(subreddit);
                    CREATE INDEX IF NOT EXISTS idx_reddit_sentiment_created 
                    ON reddit_sentiment_analysis(created_utc);
                    CREATE INDEX IF NOT EXISTS idx_reddit_sentiment_score 
                    ON reddit_sentiment_analysis(sentiment_score);
                    CREATE INDEX IF NOT EXISTS idx_reddit_sentiment_metadata 
                    ON reddit_sentiment_analysis USING gin (media_metadata);
                """)

                conn.commit()
                logger.info("Database setup completed successfully")

    except psycopg2.Error as e:
        logger.error(f"Database error in setup_database: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in setup_database: {str(e)}")
        raise

def analyze_sentiment(text: str) -> float:
    """
    Analyze sentiment in text using VADER and return compound score.
    Args:
        text (str): The text to analyze
    Returns:
        float: Compound sentiment score (-1 to 1)
    """
    try:
        if not text or not isinstance(text, str):
            logger.warning(f"Invalid text input for sentiment analysis: {type(text)}")
            return 0.0
        scores = vader.polarity_scores(text)
        logger.debug(f"VADER scores: {scores}")
        return scores['compound']
    except Exception as e:
        logger.error(f"Error in analyze_sentiment: {str(e)}")
        return 0.0

def get_content_types(post_data: Optional[Dict]) -> Dict[str, bool]:
    """
    Analyze Reddit post and return content types with improved error handling and logging.
    
    Args:
        post_data (Optional[Dict]): Reddit post data dictionary. Can be None.
        
    Returns:
        Dict[str, bool]: Dictionary indicating presence of content types
    """
    content_types = {
        'text': False,
        'image': False,
        'video': False
    }
    
    if post_data is None:
        logger.warning("Received None post_data in get_content_types")
        return content_types
        
    try:
        if not isinstance(post_data, dict):
            logger.warning(f"post_data is not a dictionary: {type(post_data)}")
            return content_types
            
        # Text content analysis with safe type handling
        selftext = str(post_data.get('selftext', '')).strip()
        selftext_html = str(post_data.get('selftext_html', '')).strip()
        title = str(post_data.get('title', '')).strip()
            
        content_types['text'] = any([
            bool(selftext),
            post_data.get('is_self', False),
            bool(selftext_html),
            bool(title)
        ])
        
        # Video content analysis with safe dictionary access
        media = post_data.get('media', {}) or {}  # Convert None to empty dict
        content_types['video'] = any([
            post_data.get('is_video', False),
            media.get('type') == 'youtube.com',
            post_data.get('post_hint') == 'rich:video',
            post_data.get('post_hint') == 'hosted:video',
            post_data.get('domain') == 'v.redd.it',
            _is_video_url(post_data.get('url', ''))
        ])
        
        # Image content analysis
        preview = post_data.get('preview', {}) or {}  # Convert None to empty dict
        url = post_data.get('url', '').lower()
        images = preview.get('images', [])
        
        content_types['image'] = any([
            post_data.get('post_hint') == 'image',
            post_data.get('domain') == 'i.redd.it',
            _is_image_url(url),
            bool(images),
            _has_gallery_metadata(post_data)
        ])
        
    except Exception as e:
        logger.error(f"Error in get_content_types: {str(e)}")
        # Keep default values on error
    
    return content_types

def _is_image_url(url: str) -> bool:
    """Check if URL points to an image file."""
    try:
        return bool(url) and url.endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp'))
    except Exception:
        return False

def _is_video_url(url: str) -> bool:
    """Check if URL points to a video file."""
    try:
        video_domains = {'youtube.com', 'youtu.be', 'vimeo.com', 'v.redd.it'}
        video_extensions = {'.mp4', '.webm', '.mov'}
        parsed = urlparse(url)
        return (
            parsed.netloc in video_domains or
            any(url.lower().endswith(ext) for ext in video_extensions)
        )
    except Exception:
        return False

def _has_gallery_metadata(post_data: Dict) -> bool:
    """Check if post contains gallery metadata."""
    try:
        return bool(
            post_data.get('gallery_data', {}) or
            post_data.get('media_metadata', {})
        )
    except Exception:
        return False

def process_post(cur: psycopg2.extensions.cursor, post: Tuple) -> bool:
    """
    Process a single post and its comments for sentiment analysis.
    """
    try:
        if not post or len(post) < 6:
            logger.warning(f"Invalid post data: {post}")
            return False
            
        post_id, subreddit, title, data, created_utc, score = post
        
        if any(v is None for v in (post_id, subreddit, created_utc)):
            logger.warning(f"Missing required fields for post {post_id}")
            return False
            
        # Handle case where data is not a dictionary
        if not isinstance(data, dict):
            logger.warning(f"Data for post {post_id} is not a dictionary (type: {type(data)})")
            data = {}
        
        # Convert created_utc to timestamp if it's an integer
        if isinstance(created_utc, (int, float)):
            created_utc = datetime.fromtimestamp(created_utc)
        
        # Get content types with verified dictionary
        content_types = get_content_types(data)
        
        # First, process all comments for this post
        cur.execute("""
            SELECT comment_id, body, score, created_utc, data
            FROM reddit_comments
            WHERE post_id = %s
        """, (post_id,))
        comments = cur.fetchall()
        
        # Calculate sentiment for each comment and store it
        comment_sentiments = []
        for comment in comments:
            comment_id, body, comment_score, comment_created_utc, comment_data = comment
            comment_sentiment = analyze_sentiment(body)
            comment_sentiments.append(comment_sentiment)
            
            # Store individual comment sentiment
            cur.execute("""
                INSERT INTO reddit_sentiment_analysis
                (content_type, content_id, subreddit, sentiment_score, 
                media_metadata, created_utc, score, num_comments)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (content_type, content_id) 
                DO UPDATE SET 
                sentiment_score = EXCLUDED.sentiment_score,
                score = EXCLUDED.score
                """, (
                'comment',
                comment_id,
                subreddit,
                comment_sentiment,
                Json({}),  # Empty JSON for comments as they don't have media metadata
                comment_created_utc,
                comment_score,
                0  # Comments don't have nested comments count
            ))
        
        # Calculate post sentiment as combination of title and comments
        title_sentiment = analyze_sentiment(title)
        selftext_sentiment = analyze_sentiment(data.get('selftext', ''))
        
        # Calculate overall post sentiment
        # Use weighted average: title (40%), selftext (30%), comments (30%)
        num_comments = len(comment_sentiments)
        if num_comments > 0:
            comments_avg_sentiment = sum(comment_sentiments) / num_comments
            overall_sentiment = (
                0.4 * title_sentiment +
                0.3 * selftext_sentiment +
                0.3 * comments_avg_sentiment
            )
        else:
            # If no comments, weight title and selftext more
            overall_sentiment = 0.6 * title_sentiment + 0.4 * selftext_sentiment
        
        # checking sentiment is within -1 to 1 range
        overall_sentiment = max(-1, min(1, overall_sentiment))
        
        # Insert post sentiment analysis
        cur.execute("""
            INSERT INTO reddit_sentiment_analysis
            (content_type, content_id, subreddit, sentiment_score, 
            media_metadata, created_utc, score, num_comments)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (content_type, content_id) 
            DO UPDATE SET 
            sentiment_score = EXCLUDED.sentiment_score,
            media_metadata = EXCLUDED.media_metadata,
            score = EXCLUDED.score,
            num_comments = EXCLUDED.num_comments
            """, (
            'post',
            post_id,
            subreddit,
            overall_sentiment,
            Json(content_types),
            created_utc,
            score,
            num_comments
        ))
        return True
        
    except psycopg2.Error as e:
        logger.error(f"Database error processing post {post_id if 'post_id' in locals() else 'unknown'}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error processing post {post_id if 'post_id' in locals() else 'unknown'}: {str(e)}")
        return False

def process_batch(cur: psycopg2.extensions.cursor, items: List[Tuple], batch_size: int = 1000) -> None:
    """
    Process a batch of posts for sentiment analysis.
    Args:
        cur: Database cursor
        items: List of items to process
        batch_size: Size of each batch
    """
    total_processed = 0
    total_items = len(items)
    
    try:
        for i in range(0, total_items, batch_size):
            batch = items[i:i + batch_size]
            successful = 0
            
            for item in batch:
                try:
                    with cur.connection.cursor() as batch_cur:
                        if process_post(batch_cur, item):
                            successful += 1
                            batch_cur.connection.commit()
                except Exception as e:
                    logger.error(f"Error processing item in batch: {str(e)}")
                    if batch_cur.connection:
                        batch_cur.connection.rollback()
                    continue
            
            total_processed += successful
            logger.info(f"Processed batch {i//batch_size + 1}: {successful}/{len(batch)} successful "
                       f"({total_processed}/{total_items} total)")
            
    except Exception as e:
        logger.error(f"Error in process_batch: {str(e)}")
        raise

def analyze_reddit_content() -> None:
    """Process Reddit posts and their associated comments for sentiment analysis."""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            # Process posts and their comments
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        rp.post_id, 
                        rp.subreddit, 
                        rp.title, 
                        COALESCE(rp.data, '{}'::jsonb) as data, 
                        rp.created_utc, 
                        rp.score
                    FROM reddit_posts rp
                    LEFT JOIN reddit_sentiment_analysis rsa 
                        ON rp.post_id = rsa.content_id 
                        AND rsa.content_type = 'post'
                    WHERE rsa.content_id IS NULL
                """)
                posts = cur.fetchall()
                logger.info(f"Found {len(posts)} posts to analyze")
                
                if posts:
                    process_batch(cur, posts)
            
            conn.commit()
            logger.info("Content analysis completed successfully")
            
    except psycopg2.Error as e:
        logger.error(f"Database error in analyze_reddit_content: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error in analyze_reddit_content: {str(e)}")
        raise

def main() -> None:
    """Main function to run the Reddit sentiment analysis."""
    try:
        logger.info("Starting Reddit sentiment analysis")
        
        # Set up database tables
        logger.info("Setting up database tables...")
        setup_database()
        
        # Run sentiment analysis
        logger.info("Starting content analysis...")
        analyze_reddit_content()
        
        logger.info("Reddit sentiment analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error in main function: {str(e)}")
        raise

if __name__ == "__main__":
    main()
import logging
import psycopg2
from psycopg2.extras import Json
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import requests
import json
import os
import time
from dotenv import load_dotenv
from urllib.parse import urlparse
import concurrent.futures
from ratelimit import limits, sleep_and_retry

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

class ToxicityAnalysisClient:
    TOXICITY_API_BASE = "https://api.moderatehatespeech.com/api/v1/moderate/"
    CONFIDENCE_THRESHOLD = 0.85
    TOXICITY_API_KEY = os.getenv('TOXICITY_API_KEY')
    CALLS_PER_MINUTE = 60  # Adjust based on API limits
    
    @sleep_and_retry
    @limits(calls=CALLS_PER_MINUTE, period=60)
    def analyze_toxicity(self, text: str) -> dict:
        """Rate-limited toxicity analysis."""
        start_time = time.time()
        try:
            if not text or not isinstance(text, str):
                return {"error": "Invalid input text"}
                
            data = {
                "token": self.TOXICITY_API_KEY,
                "text": text
            }
            
            response = requests.post(self.TOXICITY_API_BASE, json=data, timeout=10)
            response.raise_for_status()
            
            duration = time.time() - start_time
            logger.debug(f"API call took {duration:.2f} seconds")
            
            return response.json()
            
        except requests.exceptions.Timeout:
            logger.error("API request timed out")
            return {"error": "Request timed out"}
        except Exception as e:
            logger.error(f"API request failed: {str(e)}")
            return {"error": str(e)}

    def get_toxicity_classification(self, text: str) -> float:
        """
        Get toxicity classification and convert to a score between -1 and 1.
        """
        if not text or not isinstance(text, str):
            return 0.0

        response = self.analyze_toxicity(text)
        
        if "error" in response:
            logger.error(f"Error in toxicity analysis: {response['error']}")
            return 0.0
        
        try:
            classification = response.get('class')
            confidence = float(response.get('confidence', 0))
            
            if confidence >= self.CONFIDENCE_THRESHOLD:
                if classification == 'flag':
                    return -1.0 * confidence
                elif classification == 'normal':
                    return 1.0 * confidence
            
            return 0.0
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Error processing toxicity response: {str(e)}")
            return 0.0

def setup_database() -> None:
    """Create necessary tables and indexes for Reddit data and toxicity analysis."""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS reddit_toxicity_analysis (
                        id BIGSERIAL PRIMARY KEY,
                        content_type TEXT NOT NULL CHECK (content_type IN ('post', 'comment')),
                        content_id TEXT NOT NULL,
                        subreddit TEXT NOT NULL,
                        toxicity_score FLOAT NOT NULL CHECK (toxicity_score BETWEEN -1 AND 1),
                        media_metadata JSONB NOT NULL,
                        created_utc TIMESTAMP WITH TIME ZONE NOT NULL,
                        score INTEGER NOT NULL,
                        num_comments INTEGER,
                        UNIQUE(content_type, content_id)
                    );

                    CREATE INDEX IF NOT EXISTS idx_reddit_toxicity_subreddit 
                    ON reddit_toxicity_analysis(subreddit);
                    CREATE INDEX IF NOT EXISTS idx_reddit_toxicity_created 
                    ON reddit_toxicity_analysis(created_utc);
                    CREATE INDEX IF NOT EXISTS idx_reddit_toxicity_score 
                    ON reddit_toxicity_analysis(toxicity_score);
                    CREATE INDEX IF NOT EXISTS idx_reddit_toxicity_metadata 
                    ON reddit_toxicity_analysis USING gin (media_metadata);
                """)
                conn.commit()
                logger.info("Database setup completed successfully")

    except psycopg2.Error as e:
        logger.error(f"Database error in setup_database: {str(e)}")
        raise

def get_content_types(post_data: Optional[Dict]) -> Dict[str, bool]:
    """Analyze Reddit post and return content types."""
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
            
        # Text content analysis
        selftext = str(post_data.get('selftext', '')).strip()
        selftext_html = str(post_data.get('selftext_html', '')).strip()
        title = str(post_data.get('title', '')).strip()
            
        content_types['text'] = any([
            bool(selftext),
            post_data.get('is_self', False),
            bool(selftext_html),
            bool(title)
        ])
        
        # Video content analysis
        media = post_data.get('media', {}) or {}
        content_types['video'] = any([
            post_data.get('is_video', False),
            media.get('type') == 'youtube.com',
            post_data.get('post_hint') == 'rich:video',
            post_data.get('post_hint') == 'hosted:video',
            post_data.get('domain') == 'v.redd.it',
            _is_video_url(post_data.get('url', ''))
        ])
        
        # Image content analysis
        preview = post_data.get('preview', {}) or {}
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
    """Process a single post and its comments for toxicity analysis."""
    start_time = time.time()
    try:
        if not post or len(post) < 6:
            logger.warning(f"Invalid post data: {post}")
            return False
            
        post_id, subreddit, title, data, created_utc, score = post
        logger.debug(f"Starting to process post {post_id}")
        
        if any(v is None for v in (post_id, subreddit, created_utc)):
            logger.warning(f"Missing required fields for post {post_id}")
            return False
        
        if not isinstance(data, dict):
            logger.warning(f"Data for post {post_id} is not a dictionary (type: {type(data)})")
            data = {}
        
        if isinstance(created_utc, (int, float)):
            created_utc = datetime.fromtimestamp(created_utc)
        
        content_types = get_content_types(data)
        
        # Process comments
        cur.execute("""
            SELECT comment_id, body, score, created_utc, data
            FROM reddit_comments
            WHERE post_id = %s
        """, (post_id,))
        comments = cur.fetchall()
        
        # Calculate toxicity for each comment
        comment_toxicity_scores = []
        
        for comment in comments:
            comment_id, body, comment_score, comment_created_utc, comment_data = comment
            comment_toxicity = toxicity_analyzer.get_toxicity_classification(body)
            comment_toxicity_scores.append(comment_toxicity)
            
            # Store individual comment toxicity
            cur.execute("""
                INSERT INTO reddit_toxicity_analysis
                (content_type, content_id, subreddit, toxicity_score, 
                media_metadata, created_utc, score, num_comments)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (content_type, content_id) 
                DO UPDATE SET 
                toxicity_score = EXCLUDED.toxicity_score,
                score = EXCLUDED.score
            """, (
                'comment',
                comment_id,
                subreddit,
                comment_toxicity,
                Json({}),
                comment_created_utc,
                comment_score,
                0
            ))
            cur.connection.commit()  # Commit after each comment
        
        # Calculate post toxicity
        title_toxicity = toxicity_analyzer.get_toxicity_classification(title)
        selftext_toxicity = toxicity_analyzer.get_toxicity_classification(data.get('selftext', ''))
        
        # Calculate overall toxicity score
        # Weight: title (40%), selftext (30%), comments (30%)
        num_comments = len(comment_toxicity_scores)
        if num_comments > 0:
            comments_avg_toxicity = sum(comment_toxicity_scores) / num_comments
            overall_toxicity = (
                0.4 * title_toxicity +
                0.3 * selftext_toxicity +
                0.3 * comments_avg_toxicity
            )
        else:
            overall_toxicity = 0.6 * title_toxicity + 0.4 * selftext_toxicity
        
        # Ensure toxicity is within -1 to 1 range
        overall_toxicity = max(-1, min(1, overall_toxicity))
        
        # Insert post toxicity analysis
        cur.execute("""
            INSERT INTO reddit_toxicity_analysis
            (content_type, content_id, subreddit, toxicity_score, 
            media_metadata, created_utc, score, num_comments)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (content_type, content_id) 
            DO UPDATE SET 
            toxicity_score = EXCLUDED.toxicity_score,
            media_metadata = EXCLUDED.media_metadata,
            score = EXCLUDED.score,
            num_comments = EXCLUDED.num_comments
        """, (
            'post',
            post_id,
            subreddit,
            overall_toxicity,
            Json(content_types),
            created_utc,
            score,
            num_comments
        ))
        cur.connection.commit()  # Commit after post

        duration = time.time() - start_time
        logger.debug(f"Successfully processed post {post_id} in {duration:.2f} seconds")
        return True
        
    except Exception as e:
        logger.error(f"Error processing post {post_id if 'post_id' in locals() else 'unknown'}: {str(e)}")
        return False

def process_batch_parallel(items: List[Tuple], max_workers: int = 5) -> None:
    """Process posts in parallel with connection pooling."""
    
    def process_single_item(item: Tuple) -> bool:
        try:
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    start_time = time.time()
                    success = process_post(cur, item)
                    duration = time.time() - start_time
                    logger.debug(f"Processing post {item[0]} took {duration:.2f} seconds")
                    if success:
                        conn.commit()
                    return success
        except Exception as e:
            logger.error(f"Error processing item {item[0] if item else 'unknown'}: {str(e)}")
            return False

    total_processed = 0
    total_items = len(items)
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_item = {executor.submit(process_single_item, item): item for item in items}
        
        for future in concurrent.futures.as_completed(future_to_item):
            item = future_to_item[future]
            try:
                if future.result():
                    total_processed += 1
                
                if total_processed % 10 == 0:  # Log progress every 10 items
                    elapsed_time = time.time() - start_time
                    progress = (total_processed / total_items) * 100
                    rate = total_processed / (elapsed_time / 60)  # items per minute
                    logger.info(
                        f"Progress: {progress:.2f}% ({total_processed}/{total_items}) "
                        f"Rate: {rate:.1f} items/minute"
                    )
            except Exception as e:
                logger.error(f"Error processing future: {str(e)}")

def analyze_reddit_content() -> None:
    """Process Reddit posts with improved batch handling."""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Process in smaller batches
                batch_size = 100
                offset = 0
                
                while True:
                    cur.execute("""
                        SELECT 
                            rp.post_id, 
                            rp.subreddit, 
                            rp.title, 
                            COALESCE(rp.data, '{}'::jsonb) as data, 
                            rp.created_utc, 
                            rp.score
                        FROM reddit_posts rp
                        LEFT JOIN reddit_toxicity_analysis rta 
                            ON rp.post_id = rta.content_id 
                            AND rta.content_type = 'post'
                        WHERE rta.content_id IS NULL
                        ORDER BY rp.created_utc
                        LIMIT %s OFFSET %s
                    """, (batch_size, offset))
                    
                    posts = cur.fetchall()
                    if not posts:
                        break
                        
                    logger.info(f"Processing batch of {len(posts)} posts (offset: {offset})")
                    process_batch_parallel(posts)
                    
                    offset += batch_size
                    
                logger.info("Content analysis completed successfully")
                
    except Exception as e:
        logger.error(f"Error in analyze_reddit_content: {str(e)}")
        raise

# Initialize toxicity analyzer
toxicity_analyzer = None

def main() -> None:
    """Main function to run the Reddit toxicity analysis."""
    try:
        logger.info("Starting Reddit toxicity analysis")
        
        # Verify API key is set
        if not os.getenv('TOXICITY_API_KEY'):
            raise ValueError("TOXICITY_API_KEY environment variable not set")
            
        # Initialize analyzer
        global toxicity_analyzer
        toxicity_analyzer = ToxicityAnalysisClient()
        
        # Set up database tables
        logger.info("Setting up database tables...")
        setup_database()
        
        # Run toxicity analysis
        logger.info("Starting content analysis...")
        analyze_reddit_content()
        
        logger.info("Reddit toxicity analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error in main function: {str(e)}")
        raise

if __name__ == '__main__':
    main()
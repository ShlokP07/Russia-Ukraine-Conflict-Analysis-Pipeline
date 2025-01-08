from typing import Dict, Optional
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import Json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import requests
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ContentAnalyzer:
    def __init__(self):
        self.vader = SentimentIntensityAnalyzer()
        self.toxicity_api_key = os.getenv('TOXICITY_API_KEY')
        self.database_url = os.getenv('DATABASE_URL')
        
    def analyze_content(self, post_data: Dict, comments_data: Optional[Dict] = None) -> None:
        """
        Analyze post and comments for sentiment and toxicity in real-time
        """
        try:
            with psycopg2.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    # Analyze post content
                    post_id = post_data['id']
                    title = post_data['title']
                    selftext = post_data.get('selftext', '')
                    subreddit = post_data['subreddit']
                    
                    # Calculate post sentiment
                    title_sentiment = self._analyze_sentiment(title)
                    selftext_sentiment = self._analyze_sentiment(selftext)
                    
                    # Calculate post toxicity
                    title_toxicity = self._analyze_toxicity(title)
                    selftext_toxicity = self._analyze_toxicity(selftext)
                    
                    # Process comments if available
                    comment_sentiments = []
                    comment_toxicity_scores = []
                    
                    if comments_data:
                        for comment in comments_data:
                            comment_id = comment['id']
                            body = comment['body']
                            
                            # Analyze comment sentiment
                            comment_sentiment = self._analyze_sentiment(body)
                            comment_sentiments.append(comment_sentiment)
                            
                            # Analyze comment toxicity
                            comment_toxicity = self._analyze_toxicity(body)
                            comment_toxicity_scores.append(comment_toxicity)
                            
                            # Store comment analyses
                            self._store_comment_analysis(
                                cur, comment_id, subreddit, 
                                comment_sentiment, comment_toxicity,
                                comment['created_utc'], comment['score']
                            )
                    
                    # Calculate overall scores
                    overall_sentiment = self._calculate_overall_score(
                        title_sentiment, selftext_sentiment, comment_sentiments
                    )
                    overall_toxicity = self._calculate_overall_score(
                        title_toxicity, selftext_toxicity, comment_toxicity_scores
                    )
                    
                    # Store post analyses
                    self._store_post_analysis(
                        cur, post_id, subreddit,
                        overall_sentiment, overall_toxicity,
                        post_data['created_utc'], post_data['score'],
                        len(comment_sentiments)
                    )
                    
                    conn.commit()
                    
        except Exception as e:
            logger.error(f"Error analyzing content for post {post_data.get('id', 'unknown')}: {str(e)}")
            raise

    def _analyze_sentiment(self, text: str) -> float:
        """Analyze sentiment using VADER"""
        try:
            if not text or not isinstance(text, str):
                return 0.0
            scores = self.vader.polarity_scores(text)
            return scores['compound']
        except Exception as e:
            logger.error(f"Error in sentiment analysis: {str(e)}")
            return 0.0

    def _analyze_toxicity(self, text: str) -> float:
        """Analyze toxicity using API"""
        try:
            if not text or not isinstance(text, str):
                return 0.0
                
            response = requests.post(
                "https://api.moderatehatespeech.com/api/v1/moderate/",
                json={"token": self.toxicity_api_key, "text": text},
                timeout=10
            )
            response.raise_for_status()
            result = response.json()
            
            # Convert classification to score
            confidence = float(result.get('confidence', 0))
            if confidence >= 0.85:  # confidence threshold
                if result.get('class') == 'flag':
                    return -1.0 * confidence
                elif result.get('class') == 'normal':
                    return 1.0 * confidence
            return 0.0
            
        except Exception as e:
            logger.error(f"Error in toxicity analysis: {str(e)}")
            return 0.0

    def _calculate_overall_score(self, title_score: float, selftext_score: float, 
                               comment_scores: list) -> float:
        """Calculate weighted average score"""
        if comment_scores:
            comment_avg = sum(comment_scores) / len(comment_scores)
            overall = (0.4 * title_score + 
                      0.3 * selftext_score + 
                      0.3 * comment_avg)
        else:
            overall = 0.6 * title_score + 0.4 * selftext_score
        return max(-1, min(1, overall))

    def _store_post_analysis(self, cur, post_id: str, subreddit: str,
                           sentiment: float, toxicity: float, 
                           created_utc: int, score: int, num_comments: int) -> None:
        """Store post analysis results"""
        created_dt = datetime.fromtimestamp(created_utc)
        
        # Store sentiment analysis
        cur.execute("""
            INSERT INTO reddit_sentiment_analysis
            (content_type, content_id, subreddit, sentiment_score, 
            media_metadata, created_utc, score, num_comments)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (content_type, content_id) DO UPDATE SET
            sentiment_score = EXCLUDED.sentiment_score,
            score = EXCLUDED.score,
            num_comments = EXCLUDED.num_comments
        """, ('post', post_id, subreddit, sentiment, Json({}), 
              created_dt, score, num_comments))
        
        # Store toxicity analysis
        cur.execute("""
            INSERT INTO reddit_toxicity_analysis
            (content_type, content_id, subreddit, toxicity_score,
            media_metadata, created_utc, score, num_comments)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (content_type, content_id) DO UPDATE SET
            toxicity_score = EXCLUDED.toxicity_score,
            score = EXCLUDED.score,
            num_comments = EXCLUDED.num_comments
        """, ('post', post_id, subreddit, toxicity, Json({}),
              created_dt, score, num_comments))

    def _store_comment_analysis(self, cur, comment_id: str, subreddit: str,
                              sentiment: float, toxicity: float,
                              created_utc: int, score: int) -> None:
        """Store comment analysis results"""
        created_dt = datetime.fromtimestamp(created_utc)
        
        # Store sentiment analysis
        cur.execute("""
            INSERT INTO reddit_sentiment_analysis
            (content_type, content_id, subreddit, sentiment_score,
            media_metadata, created_utc, score, num_comments)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (content_type, content_id) DO UPDATE SET
            sentiment_score = EXCLUDED.sentiment_score,
            score = EXCLUDED.score
        """, ('comment', comment_id, subreddit, sentiment,
              Json({}), created_dt, score, 0))
        
        # Store toxicity analysis
        cur.execute("""
            INSERT INTO reddit_toxicity_analysis
            (content_type, content_id, subreddit, toxicity_score,
            media_metadata, created_utc, score, num_comments)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (content_type, content_id) DO UPDATE SET
            toxicity_score = EXCLUDED.toxicity_score,
            score = EXCLUDED.score
        """, ('comment', comment_id, subreddit, toxicity,
              Json({}), created_dt, score, 0))
from typing import Dict, Optional
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import Json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import requests
import os
from dotenv import load_dotenv
import html
import re
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

load_dotenv()

class ChanContentAnalyzer:
    def __init__(self):
        self.vader = SentimentIntensityAnalyzer()
        self.toxicity_api_key = os.getenv('TOXICITY_API_KEY')
        self.database_url = os.getenv('DATABASE_URL')
        
        # Configure retry strategy for toxicity API
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session = requests.Session()
        self.session.mount('https://', HTTPAdapter(max_retries=retry_strategy))
        
        # Configure logging
        self.logger = logging.getLogger("chan_analyzer")
        self.logger.setLevel(logging.INFO)
        
    def clean_text(self, text: str) -> str:
        """Clean and prepare text for analysis"""
        if not text:
            return ""
        
        text = html.unescape(text)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        text = re.sub(r'[^\w\s,.!?]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

    def analyze_sentiment(self, text: str) -> float:
        """Calculate VADER sentiment score"""
        try:
            if not text or not isinstance(text, str):
                return 0.0
            
            cleaned_text = self.clean_text(text)
            if not cleaned_text:
                return 0.0
                
            scores = self.vader.polarity_scores(cleaned_text)
            return scores['compound']
        except Exception as e:
            self.logger.error(f"Error in sentiment analysis: {str(e)}")
            return 0.0

    def analyze_toxicity(self, text: str) -> float:
        """Get toxicity score from API"""
        try:
            if not text or not isinstance(text, str):
                return 0.0

            cleaned_text = self.clean_text(text)
            if not cleaned_text:
                return 0.0
                
            response = self.session.post(
                "https://api.moderatehatespeech.com/api/v1/moderate/",
                json={"token": self.toxicity_api_key, "text": cleaned_text},
                timeout=10
            )
            response.raise_for_status()
            result = response.json()
            
            confidence = float(result.get('confidence', 0))
            if confidence >= 0.85:  # confidence threshold
                if result.get('class') == 'flag':
                    return -1.0 * confidence
                elif result.get('class') == 'normal':
                    return 1.0 * confidence
            return 0.0
            
        except Exception as e:
            self.logger.error(f"Error in toxicity analysis: {str(e)}")
            time.sleep(1)  # Basic rate limiting
            return 0.0

    def analyze_content(self, post_data: Dict) -> None:
        """Analyze post content for sentiment and toxicity in real-time"""
        try:
            with psycopg2.connect(self.database_url) as conn:
                with conn.cursor() as cur:
                    # Extract post data
                    post_number = post_data['no']
                    thread_number = post_data.get('resto', 0)  # 0 for OP posts
                    board = post_data.get('board', '')
                    comment_text = post_data.get('com', '')
                    created_utc = datetime.fromtimestamp(post_data['time'])
                    
                    if not comment_text:
                        return
                    
                    # Calculate scores
                    sentiment_score = self.analyze_sentiment(comment_text)
                    toxicity_score = self.analyze_toxicity(comment_text)
                    
                    # Store sentiment analysis
                    cur.execute("""
                        INSERT INTO chan_sentiment_analysis 
                        (post_number, thread_number, board, sentiment_score, created_utc)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (board, thread_number, post_number) DO UPDATE
                        SET sentiment_score = EXCLUDED.sentiment_score
                    """, (post_number, thread_number, board, sentiment_score, created_utc))
                    
                    # Store toxicity analysis
                    cur.execute("""
                        INSERT INTO chan_toxicity_analysis 
                        (post_number, thread_number, board, toxicity_score, created_utc)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (board, thread_number, post_number) DO UPDATE
                        SET toxicity_score = EXCLUDED.toxicity_score
                    """, (post_number, thread_number, board, int(toxicity_score), created_utc))
                    
                    conn.commit()
                    
        except Exception as e:
            self.logger.error(f"Error analyzing content for post {post_data.get('no', 'unknown')}: {str(e)}")
            raise
import psycopg2
import pandas as pd
import datetime
from datetime import datetime
import logging
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.dates import DateFormatter
import os
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = "postgres://postgres:testpassword@localhost:5432/reddit_data"

def get_submissions_data():
    """Retrieve daily submission counts from the database"""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            query = """
                SELECT DATE(created_utc) as submission_date,
                       COUNT(*) as submission_count
                FROM reddit_posts
                WHERE subreddit = 'politics'
                AND created_utc BETWEEN 
                    '2024-11-01 00:00:00+00' AND '2024-11-14 23:59:59+00'
                GROUP BY DATE(created_utc)
                ORDER BY submission_date;
            """
            
            df = pd.read_sql_query(query, conn)
            df['submission_date'] = pd.to_datetime(df['submission_date'])
            logger.info(f"Retrieved {len(df)} days of submission data")
            return df
    except Exception as e:
        logger.error(f"Error retrieving submission data: {e}")
        raise

def get_hourly_comments():
    """Retrieve hourly comment counts from the database"""
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            query = """
                SELECT 
                    DATE_TRUNC('hour', rc.created_utc) as comment_hour,
                    COUNT(*) as comment_count
                FROM reddit_comments rc
                JOIN reddit_posts rp ON rc.post_id = rp.post_id
                WHERE rp.subreddit = 'politics'
                AND rc.created_utc BETWEEN 
                    '2024-11-01 00:00:00+00' AND '2024-11-14 23:59:59+00'
                GROUP BY DATE_TRUNC('hour', rc.created_utc)
                ORDER BY comment_hour;
            """
            
            df = pd.read_sql_query(query, conn)
            df['comment_hour'] = pd.to_datetime(df['comment_hour'])
            logger.info(f"Retrieved {len(df)} hours of comment data")
            return df
    except Exception as e:
        logger.error(f"Error retrieving hourly comment data: {e}")
        raise

def create_daily_submissions_plot(df):
    """Create and save the daily submissions visualization"""
    try:
        plt.figure(figsize=(15, 8))
        sns.set_style("whitegrid")
        
        # Create bar plot
        plt.bar(df['submission_date'], df['submission_count'],
                width=0.8,  # Wider bars for daily view
                color='#4A90E2',  # Blue color
                alpha=0.8)
        
        # Formatting
        plt.xlabel('Date', fontsize=12, color='#2C3E50')
        plt.ylabel('Number of Submissions', fontsize=12, color='#2C3E50')
        plt.title('Daily Submission Count in r/politics\nNovember 1-14, 2024',
                 fontsize=14, pad=20, color='#2C3E50')
        
        # Format x-axis
        plt.gca().xaxis.set_major_formatter(DateFormatter("%Y-%m-%d"))
        plt.xticks(rotation=45, ha='right')
        
        # Format y-axis with thousands separator
        plt.gca().yaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, p: format(int(x), ',')))
        
        # Add grid
        plt.grid(True, linestyle='--', alpha=0.4, color='#95A5A6')
        
        # Adjust layout
        plt.tight_layout()
        
        # Save plot
        plt.savefig('plot7_reddit_daily_submissions.png', dpi=300, bbox_inches='tight',
                    facecolor='white', edgecolor='none')
        logger.info("Daily submissions visualization saved as plot7_reddit_daily_submissions.png")
        
        plt.close()
        
    except Exception as e:
        logger.error(f"Error creating daily submissions visualization: {e}")
        raise

def create_hourly_comments_plot(df):
    """Create and save the hourly comments visualization"""
    try:
        plt.figure(figsize=(20, 8))
        sns.set_style("whitegrid")
        
        # Create bar plot
        plt.bar(df['comment_hour'], df['comment_count'],
                width=0.02,  # Thinner bars for hourly view
                color='#FF4B4B',  # Red color
                alpha=0.8)
        
        # Formatting
        plt.xlabel('Date and Hour', fontsize=12, color='#2C3E50')
        plt.ylabel('Number of Comments', fontsize=12, color='#2C3E50')
        plt.title('Hourly Comment Activity in r/politics\nNovember 1-14, 2024',
                 fontsize=14, pad=20, color='#2C3E50')
        
        # Format x-axis
        date_formatter = DateFormatter("%Y-%m-%d %H:%M")
        plt.gca().xaxis.set_major_formatter(date_formatter)
        plt.xticks(rotation=45, ha='right')
        plt.gca().xaxis.set_major_locator(plt.MaxNLocator(15))
        
        # Format y-axis with thousands separator
        plt.gca().yaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, p: format(int(x), ',')))
        
        # Add grid
        plt.grid(True, linestyle='--', alpha=0.4, color='#95A5A6')
        
        # Adjust layout
        plt.tight_layout()
        
        # Save plot
        plt.savefig('plot8_reddit_hourly_comments.png', dpi=300, bbox_inches='tight',
                    facecolor='white', edgecolor='none')
        logger.info("Hourly comments visualization saved as plot8_reddit_hourly_comments.png")
        
        plt.close()
        
    except Exception as e:
        logger.error(f"Error creating hourly comments visualization: {e}")
        raise

def main():
    logger.info("Starting Reddit analysis")
    
    # Get daily submissions data and create plot
    submissions_df = get_submissions_data()
    if not submissions_df.empty:
        create_daily_submissions_plot(submissions_df)
    else:
        logger.error("No submission data to visualize")
    
    # Get hourly comments data and create plot
    comments_df = get_hourly_comments()
    if not comments_df.empty:
        create_hourly_comments_plot(comments_df)
    else:
        logger.error("No comment data to visualize")
    
    logger.info("Analysis completed")

if __name__ == "__main__":
    main()
import pandas as pd
import psycopg2
from scipy import stats
import numpy as np

REDDIT_DB_URL = "postgres://postgres:testpassword@localhost:5432/reddit_data"
CHAN_DB_URL = "postgres://postgres:testpassword@localhost:5432/chan_crawler"

def create_enhanced_platform_metrics():
    """
    Creates an enhanced analysis of sentiment and toxicity scores
    with additional statistical measures and confidence intervals.
    """
    # Get Reddit data with enhanced metrics
    with psycopg2.connect(REDDIT_DB_URL) as reddit_conn:
        reddit_query = """
        WITH sentiment_stats AS (
            SELECT 
                subreddit as platform,
                AVG(sentiment_score) as avg_sentiment,
                COUNT(*) as post_count,
                STDDEV(sentiment_score) as sentiment_stddev
            FROM 
                reddit_sentiment_analysis
            WHERE 
                content_type = 'post'
            GROUP BY 
                subreddit
        ),
        toxicity_stats AS (
            SELECT 
                subreddit,
                AVG(CASE 
                    WHEN toxicity_score < 0 THEN ABS(toxicity_score)
                    ELSE 1 - toxicity_score
                END) as avg_toxicity
            FROM 
                reddit_toxicity_analysis
            WHERE 
                content_type = 'post'
            GROUP BY 
                subreddit
        )
        SELECT 
            s.platform,
            ROUND(s.avg_sentiment::numeric, 2) as avg_sentiment,
            ROUND(t.avg_toxicity::numeric, 2) as avg_toxicity,
            s.post_count,
            ROUND(s.sentiment_stddev::numeric, 2) as sentiment_std
        FROM 
            sentiment_stats s
            JOIN toxicity_stats t ON s.platform = t.subreddit
        """
        reddit_df = pd.read_sql_query(reddit_query, reddit_conn)

    # Get 4chan data with enhanced metrics
    with psycopg2.connect(CHAN_DB_URL) as chan_conn:
        chan_query = """
        WITH sentiment_stats AS (
            SELECT 
                board as platform,
                AVG(sentiment_score) as avg_sentiment,
                COUNT(*) as post_count,
                STDDEV(sentiment_score) as sentiment_stddev
            FROM 
                chan_sentiment_analysis
            GROUP BY 
                board
        ),
        toxicity_stats AS (
            SELECT 
                board,
                AVG(CASE 
                    WHEN toxicity_score = -1 THEN 1.0
                    WHEN toxicity_score = 0 THEN 0.5
                    WHEN toxicity_score = 1 THEN 0.0
                END) as avg_toxicity
            FROM 
                chan_toxicity_analysis
            GROUP BY 
                board
        )
        SELECT 
            s.platform,
            ROUND(s.avg_sentiment::numeric, 2) as avg_sentiment,
            ROUND(t.avg_toxicity::numeric, 2) as avg_toxicity,
            s.post_count,
            ROUND(s.sentiment_stddev::numeric, 2) as sentiment_std
        FROM 
            sentiment_stats s
            JOIN toxicity_stats t ON s.platform = t.board
        """
        chan_df = pd.read_sql_query(chan_query, chan_conn)

    # Add platform prefix to 4chan boards
    chan_df['platform'] = chan_df['platform'].apply(lambda x: f'/{x}/ (4chan)')
    
    # Combine the dataframes
    df = pd.concat([reddit_df, chan_df])
    
    # Calculate 95% confidence intervals
    def calculate_ci(row):
        if row['post_count'] > 1:
            ci = stats.t.interval(confidence=0.95, 
                                df=row['post_count']-1,
                                loc=row['avg_sentiment'],
                                scale=row['sentiment_std']/np.sqrt(row['post_count']))
            return ci[1] - row['avg_sentiment']
        return 0
    
    df['sentiment_ci'] = df.apply(calculate_ci, axis=1)
    
    # Print formatted table
    print("\nTable 1: Average Sentiment and Toxicity by boards/subreddit")
    print("-" * 75)
    print(f"{'boards/subreddit':<30} | {'Avg Sentiment':>20} | {'Avg Toxicity':>20}")
    print("-" * 75)
    
    # Sort by platform name and print each row
    for _, row in df.sort_values('platform').iterrows():
        print(f"{row['platform']:<30} | {row['avg_sentiment']:>20.2f} | {row['avg_toxicity']:>20.2f}")
    
    print("-" * 75)

if __name__ == "__main__":
    create_enhanced_platform_metrics()
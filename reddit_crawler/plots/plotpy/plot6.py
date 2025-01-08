import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import psycopg2
from datetime import datetime, timezone
import numpy as np

DATABASE_URL = "postgres://postgres:testpassword@localhost:5432/reddit_data"

def create_volume_sentiment_scatter(output_path='plot6_volume_sentiment_correlation.png'):
    """
    Creates and saves a scatter plot showing the relationship between
    post volume (posts/hour) and average sentiment across subreddits.
    Points are sized by total engagement.
    """
    # Create database connection
    with psycopg2.connect(DATABASE_URL) as conn:
        # Query for Reddit data
        query = """
        WITH hourly_stats AS (
            SELECT 
                subreddit,
                DATE_TRUNC('hour', created_utc) as hour,
                COUNT(*) as posts_per_hour,
                AVG(sentiment_score) as avg_sentiment,
                SUM(score + COALESCE(num_comments, 0)) as total_engagement
            FROM 
                reddit_sentiment_analysis
            WHERE 
                content_type = 'post'
            GROUP BY 
                subreddit,
                DATE_TRUNC('hour', created_utc)
        )
        SELECT 
            subreddit,
            AVG(posts_per_hour) as avg_posts_per_hour,
            AVG(avg_sentiment) as avg_sentiment,
            SUM(total_engagement) as total_engagement
        FROM 
            hourly_stats
        GROUP BY 
            subreddit
        """
        
        df = pd.read_sql_query(query, conn)
    
    # Normalize engagement scores for point sizing
    df['normalized_engagement'] = (df['total_engagement'] - df['total_engagement'].min()) / \
                                (df['total_engagement'].max() - df['total_engagement'].min())
    # Scale up the sizes for better visibility
    df['point_size'] = df['normalized_engagement'] * 1000 + 100
    
    # Create the plot
    plt.figure(figsize=(12, 8))
    
    # Set style and create scatter plot
    sns.set_style("whitegrid")
    scatter = plt.scatter(
        x=df['avg_posts_per_hour'],
        y=df['avg_sentiment'],
        s=df['point_size'],
        c=df['total_engagement'],
        cmap='viridis',
        alpha=0.6
    )
    
    # Add subreddit labels to points
    for idx, row in df.iterrows():
        plt.annotate(
            row['subreddit'],
            (row['avg_posts_per_hour'], row['avg_sentiment']),
            xytext=(5, 5), textcoords='offset points',
            fontsize=8
        )
    
    # Customize plot
    plt.title('Post Volume vs Average Sentiment by Subreddit', 
             fontsize=14, pad=20)
    plt.xlabel('Average Posts per Hour', fontsize=12)
    plt.ylabel('Average Sentiment Score', fontsize=12)
    
    # Add colorbar for engagement
    cbar = plt.colorbar(scatter)
    cbar.set_label('Total Engagement (Score + Comments)', fontsize=10)
    
    # Add size legend for visibility reference
    sizes = [df['point_size'].min(), df['point_size'].max()]
    labels = ['Low', 'High']
    legend = plt.legend(
        handles=[plt.scatter([], [], s=s, c='gray', alpha=0.3) for s in sizes],
        labels=labels,
        title="Relative Engagement",
        loc='upper left',
        bbox_to_anchor=(1.05, 1)
    )
    
    # Adjust layout and save
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

if __name__ == "__main__":
    create_volume_sentiment_scatter()
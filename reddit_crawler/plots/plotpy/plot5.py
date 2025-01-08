import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import psycopg2
import numpy as np
from datetime import datetime, timedelta

REDDIT_DATABASE_URL = "postgres://postgres:testpassword@localhost:5432/reddit_data"
CHAN_DATABASE_URL = "postgres://postgres:testpassword@localhost:5432/chan_crawler"

def get_platform_data(connection_url, query):
    """Helper function to fetch and process data for a platform."""
    try:
        conn = psycopg2.connect(connection_url)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return pd.DataFrame()

def create_hourly_sentiment_plot(output_path='plot5_hourly_sentiment_analysis.png'):
    """
    Creates and saves a line plot showing the average sentiment scores
    by hour of day for different platforms (Reddit and 4chan).
    Analyzes all available data.
    """
    # Modified queries to include all data
    reddit_query = """
    SELECT 
        EXTRACT(HOUR FROM created_utc) as hour_of_day,
        AVG(sentiment_score) as avg_sentiment,
        COUNT(*) as count
    FROM 
        reddit_sentiment_analysis
    WHERE 
        content_type = 'post'
    GROUP BY 
        EXTRACT(HOUR FROM created_utc)
    ORDER BY 
        hour_of_day
    """
    
    chan_query = """
    SELECT 
        EXTRACT(HOUR FROM created_utc) as hour_of_day,
        AVG(sentiment_score) as avg_sentiment,
        COUNT(*) as count
    FROM 
        chan_sentiment_analysis
    GROUP BY 
        EXTRACT(HOUR FROM created_utc)
    ORDER BY 
        hour_of_day
    """
    
    # Fetch data for both platforms
    reddit_df = get_platform_data(REDDIT_DATABASE_URL, reddit_query)
    reddit_df['platform'] = 'Reddit'
    
    chan_df = get_platform_data(CHAN_DATABASE_URL, chan_query)
    chan_df['platform'] = '4chan'
    
    # Check if we have data
    if reddit_df.empty and chan_df.empty:
        print("Error: No data retrieved from either platform")
        return
    
    # Combine the dataframes
    df = pd.concat([reddit_df, chan_df], ignore_index=True)
    
    # Create the plot
    plt.figure(figsize=(15, 8))
    sns.set_style("whitegrid")
    
    # Plot lines for each platform
    colors = {'Reddit': '#1f77b4', '4chan': '#2ca02c'}
    platforms = df['platform'].unique()
    
    for platform in platforms:
        platform_data = df[df['platform'] == platform]
        
        # Plot the line
        plt.plot(platform_data['hour_of_day'], 
                platform_data['avg_sentiment'], 
                label=platform,
                linewidth=2,
                alpha=0.8,
                color=colors[platform],
                marker='o')  # Added markers for hourly points
        
        # Add confidence bands
        std_err = 1.96 / np.sqrt(platform_data['count'].fillna(1))
        plt.fill_between(platform_data['hour_of_day'],
                        platform_data['avg_sentiment'] - std_err,
                        platform_data['avg_sentiment'] + std_err,
                        alpha=0.2,
                        color=colors[platform])
    
    # Customize plot
    plt.title('Average Sentiment by Hour of Day\n(All-time Aggregate)', 
             fontsize=14, pad=20)
    plt.xlabel('Hour of Day (UTC)', fontsize=12)
    plt.ylabel('Average Sentiment Score', fontsize=12)
    plt.legend(title='Platform', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Format x-axis
    plt.xticks(range(0, 24), rotation=45)
    plt.xlim(-0.5, 23.5)  # Set proper x-axis limits
    plt.grid(True, linestyle='--', alpha=0.7)
    
    # Adjust layout and save
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

if __name__ == "__main__":
    create_hourly_sentiment_plot()
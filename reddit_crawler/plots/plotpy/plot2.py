import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import psycopg2
import numpy as np

# Database connection strings
REDDIT_DATABASE_URL = 'postgres://postgres:testpassword@localhost:5432/reddit_data'
CHAN_DATABASE_URL = 'postgres://postgres:testpassword@localhost:5432/chan_crawler'

def get_platform_data(database_url, query):
    """
    Fetches data from a PostgreSQL database using the provided query.
    """
    try:
        conn = psycopg2.connect(database_url)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error fetching data: {str(e)}")
        return pd.DataFrame()

def create_sentiment_plot(output_path='plot2_sentiment_distribution.png'):
    """
    Creates visualization of sentiment scores across platforms.
    Scores range from -1 (negative sentiment) to +1 (positive sentiment)
    """
    # Set basic style settings
    sns.set_theme(style="whitegrid")
    
    # Queries for sentiment analysis
    reddit_query = """
    SELECT 
        'Reddit Posts' as platform,
        sentiment_score
    FROM 
        reddit_sentiment_analysis
    WHERE 
        content_type = 'post'
    """
    
    chan_query = """
    SELECT 
        '4chan' as platform,
        sentiment_score
    FROM 
        chan_sentiment_analysis
    """
    
    # Fetch data
    reddit_df = get_platform_data(REDDIT_DATABASE_URL, reddit_query)
    chan_df = get_platform_data(CHAN_DATABASE_URL, chan_query)
    
    # Combine dataframes
    df = pd.concat([reddit_df, chan_df], ignore_index=True)
    
    if df.empty:
        print("Error: No data retrieved from either platform")
        return
        
    # Calculate basic statistics
    stats = df.groupby('platform')['sentiment_score'].agg([
        'mean', 'median', 'std', 'count',
        'min', 'max',  # Added min and max for sentiment range
        lambda x: (x > 0).mean() * 100  # Percentage of positive sentiment
    ]).round(3)
    stats = stats.rename(columns={'<lambda>': 'positive_percentage'})
    print("\nPlatform Statistics:")
    print(stats)
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 7))
    
    # Plot 1: Violin plot with custom colors
    palette = {'Reddit Posts': '#3498db', '4chan': '#e74c3c'}
    sns.violinplot(x='platform', y='sentiment_score', data=df, ax=ax1,
                  palette=palette)
    
    # Add individual points
    sns.stripplot(x='platform', y='sentiment_score', data=df,
                 size=2, color='gray', alpha=0.3, ax=ax1, jitter=0.2)
    
    # Add reference line for neutral sentiment
    ax1.axhline(y=0, color='black', linestyle='--', alpha=0.5, label='Neutral sentiment')
    
    ax1.set_title('Distribution of Sentiment Scores', fontsize=12)
    ax1.set_xlabel('Platform', fontsize=10)
    ax1.set_ylabel('Sentiment Score\n(-1 = Most Negative, +1 = Most Positive)', fontsize=10)
    ax1.legend()
    
    # Add mean lines
    sns.pointplot(x='platform', y='sentiment_score', data=df, 
                 color='white', scale=0.5, ax=ax1)
    
    # Plot 2: Kernel Density Estimation
    for platform in ['Reddit Posts', '4chan']:
        platform_data = df[df['platform'] == platform]
        sns.kdeplot(data=platform_data, x='sentiment_score', ax=ax2, 
                   label=platform, fill=True, alpha=0.3)
    
    ax2.axvline(x=0, color='black', linestyle='--', alpha=0.5, label='Neutral sentiment')
    ax2.set_title('Sentiment Score Density Distribution', fontsize=12)
    ax2.set_xlabel('Sentiment Score', fontsize=10)
    ax2.set_ylabel('Density', fontsize=10)
    ax2.legend()
    
    plt.suptitle('Sentiment Analysis: Reddit Posts vs 4chan\n(-1 = Most Negative, +1 = Most Positive)', 
                fontsize=14, y=1.02)
    plt.tight_layout()
    
    # Save plot
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"\nPlot saved as: {output_path}")

if __name__ == "__main__":
    create_sentiment_plot()
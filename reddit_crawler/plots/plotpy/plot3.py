import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import psycopg2
import numpy as np

DATABASE_URL = "postgres://postgres:testpassword@localhost:5432/reddit_data"

def create_toxicity_scatter(output_path='plot3_toxicity_engagement_correlation.png'):
    """
    Creates and saves a scatter plot showing the relationship between
    toxicity scores and normalized engagement across different platforms.
    """
    # Create database connection
    with psycopg2.connect(DATABASE_URL) as conn:
        query = """
        SELECT 
            subreddit,
            toxicity_score,
            score + COALESCE(num_comments, 0) as total_engagement
        FROM 
            reddit_toxicity_analysis
        WHERE 
            content_type = 'post'
        """
        df = pd.read_sql_query(query, conn)
    
    # Normalize engagement scores using min-max scaling
    df['normalized_engagement'] = (df['total_engagement'] - df['total_engagement'].min()) / \
                                (df['total_engagement'].max() - df['total_engagement'].min())
    
    # Create the plot
    plt.figure(figsize=(12, 8))
    
    # Set style and create scatter plot
    sns.set_style("whitegrid")
    sns.scatterplot(
        data=df,
        x='toxicity_score',
        y='normalized_engagement',
        hue='subreddit',
        alpha=0.5
    )
    
    # Customize plot
    plt.title('Correlation between Post Toxicity and Engagement by Subreddit', 
             fontsize=14, pad=20)
    plt.xlabel('Toxicity Score', fontsize=12)
    plt.ylabel('Normalized Engagement (Score + Comments)', fontsize=12)
    plt.legend(title='Subreddit', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Adjust layout and save
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

if __name__ == "__main__":
    create_toxicity_scatter()
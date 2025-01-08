import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import psycopg2
import numpy as np

DATABASE_URL = "postgres://postgres:testpassword@localhost:5432/reddit_data"

def create_alternative_media_impact_plot(output_path='plot4_media_impact_heatmap.png'):
    """
    Creates and saves a heatmap visualization with corrected media type classification
    """
    with psycopg2.connect(DATABASE_URL) as conn:
        query = """
        WITH media_types AS (
            SELECT 
                CASE 
                    WHEN media_metadata->>'text' = 'true' AND 
                         media_metadata->>'image' = 'false' AND 
                         media_metadata->>'video' = 'false'
                    THEN 'Text Only'
                    WHEN media_metadata->>'image' = 'true' AND 
                         media_metadata->>'video' = 'false'
                    THEN 'Image'
                    WHEN media_metadata->>'video' = 'true'
                    THEN 'Video'
                END AS media_type,
                sentiment_score,
                score + COALESCE(num_comments, 0) as total_engagement
            FROM 
                reddit_sentiment_analysis
            WHERE 
                content_type = 'post'
        )
        SELECT 
            media_type,
            AVG(sentiment_score) as avg_sentiment,
            AVG(total_engagement) as avg_engagement,
            COUNT(*) as count
        FROM 
            media_types
        WHERE 
            media_type IS NOT NULL
        GROUP BY 
            media_type
        ORDER BY 
            avg_engagement DESC
        """
        df = pd.read_sql_query(query, conn)
    
    # Create separate DataFrames for raw and normalized values
    raw_data = pd.DataFrame({
        'Engagement': df['avg_engagement'].values,
        'Sentiment': df['avg_sentiment'].values
    }, index=df['media_type'])
    
    # Normalize each column independently
    normalized_data = pd.DataFrame()
    for column in ['Engagement', 'Sentiment']:
        min_val = raw_data[column].min()
        max_val = raw_data[column].max()
        normalized_data[column] = (raw_data[column] - min_val) / (max_val - min_val)
    
    # Create figure and axis
    plt.figure(figsize=(12, 8))
    
    # Create heatmap with normalized data
    sns.heatmap(normalized_data, 
                annot=raw_data,  # Show raw values
                fmt='.2f', 
                cmap='RdYlBu',
                center=0.5,
                cbar_kws={'label': 'Normalized Score'})
    
    # Add sample size annotations
    for i, count in enumerate(df['count']):
        plt.text(-0.2, i + 0.5, f'n={int(count)}', ha='right', va='center')
    
    # Customize the plot
    plt.title('Media Type Impact on Engagement and Sentiment\n(Raw Values with Normalized Colors)', pad=20)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

if __name__ == "__main__":
    create_alternative_media_impact_plot()
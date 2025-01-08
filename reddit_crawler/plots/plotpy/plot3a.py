import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import psycopg2
import numpy as np
from scipy import stats

# Configuration
DATABASE_URL = "postgres://postgres:testpassword@localhost:5432/reddit_data"

def plot_toxicity_engagement_correlation(output_path='plot3a_toxicity_engagement_correlation.png'):
    """
    Creates and saves a scatter plot with regression lines showing the correlation between
    toxicity scores and normalized engagement across different platforms.
    """
    # Create database connection
    with psycopg2.connect(DATABASE_URL) as conn:
        # Query to get posts data and calculate engagement
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
        
        # Read data into DataFrame
        df = pd.read_sql_query(query, conn)
    
    # Normalize engagement scores using min-max scaling
    df['normalized_engagement'] = (df['total_engagement'] - df['total_engagement'].min()) / \
                                (df['total_engagement'].max() - df['total_engagement'].min())
    
    # Create the plot
    plt.figure(figsize=(12, 8))
    
    # Create scatter plot with regression lines for each subreddit
    sns.set_style("whitegrid")
    
    # Create scatter plot
    sns.scatterplot(
        data=df,
        x='toxicity_score',
        y='normalized_engagement',
        hue='subreddit',
        alpha=0.5
    )
    
    # Add regression line for each subreddit
    subreddits = df['subreddit'].unique()
    for subreddit in subreddits:
        subreddit_data = df[df['subreddit'] == subreddit]
        
        # Calculate regression line
        slope, intercept, r_value, p_value, std_err = stats.linregress(
            subreddit_data['toxicity_score'],
            subreddit_data['normalized_engagement']
        )
        
        # Create regression line points
        line_x = np.array([subreddit_data['toxicity_score'].min(), 
                          subreddit_data['toxicity_score'].max()])
        line_y = slope * line_x + intercept
        
        # Plot regression line
        plt.plot(line_x, line_y, '--', 
                label=f'{subreddit} (R² = {r_value**2:.3f})')
    
    # Customize plot
    plt.title('Correlation between Post Toxicity and Engagement by Subreddit', 
             fontsize=14, pad=20)
    plt.xlabel('Toxicity Score', fontsize=12)
    plt.ylabel('Normalized Engagement (Score + Comments)', fontsize=12)
    plt.legend(title='Subreddit', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Adjust layout to prevent legend cutoff
    plt.tight_layout()
    
    # Save plot
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    # Print statistics
    print("\nStatistics by Subreddit:")
    stats_df = df.groupby('subreddit').agg({
        'toxicity_score': ['mean', 'std'],
        'normalized_engagement': ['mean', 'std'],
    }).round(3)
    
    print(f"\nTotal posts analyzed: {len(df)}")
    print("\nDetailed statistics:")
    print(stats_df)
    
    return stats_df

if __name__ == "__main__":
    print("Starting toxicity-engagement analysis...")
    
    # Create visualization and get statistics
    stats = plot_toxicity_engagement_correlation()
    
    print("\nAnalysis complete!")
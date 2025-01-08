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

def create_improved_toxicity_plot(output_path='plot1_improved_toxicity_distribution.png'):
    """
    Creates a visualization of toxicity scores across platforms.
    For Reddit, only posts (not comments) are considered.
    Interpretation:
    - Negative scores (closer to -1): More toxic content
    - Positive scores (closer to +1): Non-toxic/"normal" content
    - Scores around 0: Neutral content or low confidence
    """
    # Set basic style settings
    sns.set_theme(style="whitegrid")
    
    # Modified Reddit query to explicitly filter for posts only
    reddit_query = """
    SELECT 
        'Reddit Posts' as platform,  -- Changed label to be more specific
        toxicity_score
    FROM 
        reddit_toxicity_analysis
    WHERE 
        content_type = 'post'  -- This ensures we only get posts
    """
    
    chan_query = """
    SELECT 
        '4chan' as platform,
        toxicity_score
    FROM 
        chan_toxicity_analysis
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
    stats = df.groupby('platform')['toxicity_score'].agg([
        'mean', 'median', 'std', 'count'
    ]).round(3)
    print("\nPlatform Statistics:")
    print(stats)
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 7))
    
    # Plot 1: Violin plot with custom colors
    palette = {'Reddit Posts': '#3498db', '4chan': '#e74c3c'}  # Updated key to match new label
    sns.violinplot(x='platform', y='toxicity_score', data=df, ax=ax1,
                  palette=palette)
    
    # Add individual points
    sns.stripplot(x='platform', y='toxicity_score', data=df,
                 size=2, color='gray', alpha=0.3, ax=ax1, jitter=0.2)
    
    # Add horizontal lines for toxicity zones
    ax1.axhline(y=0.3, color='green', linestyle='--', alpha=0.5, label='Non-toxic threshold')
    ax1.axhline(y=-0.3, color='red', linestyle='--', alpha=0.5, label='Toxic threshold')
    
    ax1.set_title('Distribution of Toxicity Scores', fontsize=12)
    ax1.set_xlabel('Platform', fontsize=10)
    ax1.set_ylabel('Toxicity Score\n(Negative = Toxic, Positive = Non-toxic)', fontsize=10)
    ax1.legend()
    
    # Add mean lines
    sns.pointplot(x='platform', y='toxicity_score', data=df, 
                 color='white', scale=0.5, ax=ax1)
    
    # Plot 2: Stacked bar chart with three categories
    platforms = ['Reddit Posts', '4chan']  # Updated platform name
    colors = ['#e74c3c', '#95a5a6', '#2ecc71']  # red, gray, green
    
    for i, platform in enumerate(platforms):
        platform_data = df[df['platform'] == platform]
        
        # Calculate proportions using the correct interpretation
        toxic = (platform_data['toxicity_score'] < -0.3).mean() * 100
        neutral = ((platform_data['toxicity_score'] >= -0.3) & 
                  (platform_data['toxicity_score'] <= 0.3)).mean() * 100
        non_toxic = (platform_data['toxicity_score'] > 0.3).mean() * 100
        
        # Plot stacked bars
        bottom = 0
        for score, color, label in zip([toxic, neutral, non_toxic], 
                                     colors, 
                                     ['Toxic', 'Neutral', 'Non-toxic']):
            ax2.bar(platform, score, bottom=bottom, color=color, 
                   label=label if i == 0 else "")
            # Add percentage labels
            if score >= 5:  # Only show label if segment is large enough
                ax2.text(i, bottom + score/2, f'{score:.1f}%', 
                        ha='center', va='center')
            bottom += score
    
    ax2.set_title('Content Distribution by Toxicity Level', fontsize=12)
    ax2.set_xlabel('Platform', fontsize=10)
    ax2.set_ylabel('Percentage', fontsize=10)
    ax2.legend()
    
    plt.suptitle('Toxicity Analysis: Reddit Posts vs 4chan\n(Negative scores = Toxic, Positive scores = Non-toxic)', 
                fontsize=14, y=1.02)
    plt.tight_layout()
    
    # Save plot
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"\nPlot saved as: {output_path}")

if __name__ == "__main__":
    create_improved_toxicity_plot()
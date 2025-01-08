import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
import seaborn as sns
from datetime import datetime, timezone
import matplotlib.dates as mdates

# Configuration
DATABASE_URL = "postgresql://postgres:testpassword@localhost:5432/chan_crawler"
BOARD = 'pol'
START_DATE = '2024-11-01'
END_DATE = '2024-11-15'  # End date is exclusive

def get_data():
    """Fetch and aggregate hourly post data"""
    with psycopg2.connect(DATABASE_URL) as conn:
        query = """
            SELECT 
                date_trunc('hour', to_timestamp((data->>'time')::bigint)) AS hour_bucket,
                COUNT(*) AS post_count
            FROM posts
            WHERE board = %s
            AND to_timestamp((data->>'time')::bigint) >= %s::timestamp with time zone
            AND to_timestamp((data->>'time')::bigint) < %s::timestamp with time zone
            GROUP BY hour_bucket
            ORDER BY hour_bucket
        """
        
        df = pd.read_sql_query(query, conn, params=(BOARD, START_DATE, END_DATE))
        return df

def create_line_plot(df):
    """Create line plot of hourly post counts"""
    # Convert to local timezone
    df['hour_bucket'] = pd.to_datetime(df['hour_bucket']).dt.tz_convert('America/New_York')
    
    # Create the plot
    plt.figure(figsize=(20, 10))
    sns.set_style("whitegrid")
    
    # Plot hourly data points
    plt.plot(df['hour_bucket'], df['post_count'], 
            color='#1f77b4', 
            linewidth=1.5,
            marker='o',
            markersize=3,
            label='Hourly Comments')
    
    # Customize plot
    plt.title('Hourly Comment Count on /pol/\nNovember 1-14, 2024',
             fontsize=16, 
             pad=20)
    plt.xlabel('Date (EST)', fontsize=14)
    plt.ylabel('Number of Comments', fontsize=14)
    
    # Format x-axis
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_minor_locator(mdates.HourLocator(interval=6))
    ax.xaxis.set_minor_formatter(mdates.DateFormatter('%H:00'))
    
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
    plt.setp(ax.get_xminorticklabels(), rotation=45, ha='right', fontsize=8)
    
    plt.grid(True, which='major', linestyle='-', alpha=0.7)
    plt.grid(True, which='minor', linestyle=':', alpha=0.4)
    
    plt.legend(loc='upper right')
    plt.tight_layout()
    
    plt.savefig('plot9_pol_hourly_comments_line_nov1_14.png', dpi=300, bbox_inches='tight')
    print("\nLine plot saved as plot9_pol_hourly_comments_line_nov1_14.png")

def create_bar_plot(df):
    """Create bar plot of hourly post counts"""
    # Convert to local timezone
    df['hour_bucket'] = pd.to_datetime(df['hour_bucket']).dt.tz_convert('America/New_York')
    
    # Create the plot
    plt.figure(figsize=(20, 10))
    sns.set_style("whitegrid")
    
    # Create bar plot
    plt.bar(df['hour_bucket'], df['post_count'], 
           width=1/24,  # Width set to roughly one hour
           color='#1f77b4',
           alpha=0.7,
           label='Hourly Comments')
    
    # Add average line
    avg = df['post_count'].mean()
    plt.axhline(y=avg, color='r', linestyle=':', 
               label=f'Average ({avg:.0f} comments/hour)')
    
    # Customize plot
    plt.title('Hourly Comment Count on /pol/\nNovember 1-14, 2024',
             fontsize=16, 
             pad=20)
    plt.xlabel('Date (EST)', fontsize=14)
    plt.ylabel('Number of Comments', fontsize=14)
    
    # Format x-axis
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_minor_locator(mdates.HourLocator(interval=6))
    ax.xaxis.set_minor_formatter(mdates.DateFormatter('%H:00'))
    
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
    plt.setp(ax.get_xminorticklabels(), rotation=45, ha='right', fontsize=8)
    
    plt.grid(True, which='major', linestyle='-', alpha=0.7)
    plt.grid(True, which='minor', linestyle=':', alpha=0.4)
    
    plt.legend(loc='upper right')
    plt.tight_layout()
    
    plt.savefig('plot9_pol_hourly_comments_bar_nov1_14.png', dpi=300, bbox_inches='tight')
    print("Bar plot saved as plot9_pol_hourly_comments_bar_nov1_14.png")

def print_statistics(df):
    """Print summary statistics"""
    print(f"\nStatistics for /pol/ (Nov 1-14, 2024):")
    print(f"Total comments: {df['post_count'].sum():,}")
    print(f"Average comments per hour: {df['post_count'].mean():.1f}")
    print(f"Maximum comments in one hour: {df['post_count'].max():,}")
    print(f"Minimum comments in one hour: {df['post_count'].min():,}")
    print(f"Total hours analyzed: {len(df)}")

if __name__ == "__main__":
    print("Starting analysis...")
    
    # Get data
    df = get_data()
    
    if df.empty:
        print("No data found!")
    else:
        # Create visualizations
        create_line_plot(df)
        create_bar_plot(df)
        print_statistics(df)
    
    print("\nAnalysis complete!")
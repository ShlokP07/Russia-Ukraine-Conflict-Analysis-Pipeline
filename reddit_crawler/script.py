import json
import csv
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from psycopg2.extensions import register_adapter
import os
from dotenv import load_dotenv

# Register the Json adapter for PostgreSQL
register_adapter(dict, Json)

# Load environment variables
load_dotenv()

# Get database connection string from environment variable
DATABASE_URL = os.getenv('DATABASE_URL')

def process_comment(comment_data, parent_id=None):
    """
    Recursively process comments and their replies to extract relevant information.
    Returns a list of dictionaries containing comment information.
    """
    comments_list = []

    # Extract basic comment information
    comment_info = {
        'comment_id': comment_data.get('id'),
        'parent_id': parent_id,
        'author': comment_data.get('author'),
        'body': comment_data.get('body'),
        'score': comment_data.get('score'),
        'created_utc': datetime.fromtimestamp(comment_data.get('created_utc')).strftime('%Y-%m-%d %H:%M:%S') if comment_data.get('created_utc') else None,
        'subreddit': comment_data.get('subreddit', '')
    }

    comments_list.append(comment_info)

    # Process replies if they exist
    replies = comment_data.get('replies', '')
    if replies and isinstance(replies, dict):
        reply_data = replies.get('data', {}).get('children', [])
        for reply in reply_data:
            reply_comments = process_comment(
                reply['data'],
                parent_id=comment_data['id']
            )
            comments_list.extend(reply_comments)

    return comments_list

def extract_and_process_data(batch_size=1000, output_csv='processed_reddit_comments.csv'):
    """
    Extract data from PostgreSQL database and process it in batches
    """
    try:
        with psycopg2.connect(dsn=DATABASE_URL) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get total count of records
                cur.execute("SELECT COUNT(*) FROM reddit_comments")
                total_records = cur.fetchone()['count']
                print(f"Total records to process: {total_records}")

                # Initialize variables
                offset = 0
                all_processed_comments = []
                
                while True:
                    # Fetch batch of records
                    cur.execute(
                        "SELECT data FROM reddit_comments ORDER BY id LIMIT %s OFFSET %s",
                        (batch_size, offset)
                    )
                    records = cur.fetchall()
                    
                    if not records:
                        break
                        
                    print(f"Processing batch starting at offset {offset}")
                    
                    # Process each record in the batch
                    for record in records:
                        try:
                            json_data = record['data']
                            processed_comments = process_comment(json_data)
                            all_processed_comments.extend(processed_comments)
                        except Exception as e:
                            print(f"Error processing record: {str(e)}")
                            continue
                    
                    offset += batch_size
                    print(f"Processed {len(all_processed_comments)} comments so far...")
                    
                    # Optional: Save intermediate results
                    if len(all_processed_comments) % 10000 == 0:
                        temp_df = pd.DataFrame(all_processed_comments)
                        temp_df.to_csv(f'intermediate_results_{offset}.csv', index=False)
                
                # Convert all processed comments to DataFrame
                df_output = pd.DataFrame(all_processed_comments)
                
                if len(df_output) > 0:
                    # Reorder columns
                    columns_order = [
                        'comment_id',
                        'parent_id',
                        'author',
                        'body',
                        'score',
                        'created_utc',
                        'subreddit'
                    ]
                    
                    df_output = df_output[columns_order]
                    
                    # Save to CSV
                    df_output.to_csv(output_csv, index=False, encoding='utf-8', quoting=csv.QUOTE_ALL)
                    
                    print(f"\nSuccessfully processed {len(df_output)} comments")
                    print(f"Data saved to {output_csv}")
                    print("\nFirst few rows of the processed data:")
                    print(df_output.head())
                else:
                    print("No data found in the database")
                
                return df_output
                
    except psycopg2.Error as e:
        print(f"[ERROR] Database error: {e}")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")

if __name__ == "__main__":
    if not DATABASE_URL:
        print("[ERROR] DATABASE_URL environment variable not set")
        exit(1)
        
    print("[START] Starting to process Reddit comments")
    
    try:
        # Test database connection
        with psycopg2.connect(dsn=DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                print("[INFO] Database connection successful")
    except psycopg2.Error as e:
        print(f"[ERROR] Database connection failed: {e}")
        exit(1)
    
    # Process the data with a batch size of 1000 records
    df = extract_and_process_data(batch_size=1000, output_csv='processed_reddit_comments.csv')
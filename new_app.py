from flask import Flask, jsonify, render_template
import psycopg2
import pandas as pd
from flask_cors import CORS
from urllib.parse import unquote

app = Flask(__name__)
CORS(app)

DATABASE_URL = "postgres://postgres:testpassword@localhost:5432/reddit_data"

@app.route('/')
def index():
    return render_template('index_new.html')

@app.route('/api/subreddits')
def get_subreddits():
    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            query = """
            SELECT DISTINCT subreddit 
            FROM reddit_sentiment_analysis 
            WHERE content_type = 'post'
            ORDER BY subreddit
            """
            df = pd.read_sql_query(query, conn)
            return jsonify(df['subreddit'].tolist())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/media-metrics/<path:subreddit>')
def get_media_metrics(subreddit):
    try:
        subreddit = unquote(subreddit)
        
        with psycopg2.connect(DATABASE_URL) as conn:
            query = """
            WITH combined_metrics AS (
                SELECT 
                    s.content_id,
                    CASE 
                        WHEN s.media_metadata->>'text' = 'true' AND 
                             s.media_metadata->>'image' = 'false' AND 
                             s.media_metadata->>'video' = 'false'
                        THEN 'Text Only'
                        WHEN s.media_metadata->>'image' = 'true' AND 
                             s.media_metadata->>'video' = 'false' AND
                             s.media_metadata->>'text' = 'false'
                        THEN 'Image Only'
                        WHEN s.media_metadata->>'video' = 'true' AND
                             s.media_metadata->>'text' = 'false'
                        THEN 'Video Only'
                        WHEN s.media_metadata->>'image' = 'true' AND 
                             s.media_metadata->>'text' = 'true'
                        THEN 'Text + Image'
                        WHEN s.media_metadata->>'video' = 'true' AND
                             s.media_metadata->>'text' = 'true'
                        THEN 'Text + Video'
                        WHEN s.media_metadata->>'link' = 'true'
                        THEN 'Link'
                    END AS derived_media_type,
                    s.sentiment_score,
                    t.toxicity_score
                FROM reddit_sentiment_analysis s
                JOIN reddit_toxicity_analysis t 
                    ON s.content_id = t.content_id 
                    AND s.content_type = t.content_type
                WHERE s.content_type = 'post'
                    AND s.subreddit = %s
            )
            SELECT 
                derived_media_type,
                AVG(sentiment_score) as avg_sentiment,
                AVG(toxicity_score) as avg_toxicity
            FROM combined_metrics
            WHERE derived_media_type IS NOT NULL
            GROUP BY derived_media_type
            ORDER BY derived_media_type
            """
            
            df = pd.read_sql_query(query, conn, params=(subreddit,))
            
            if df.empty:
                return jsonify({'error': f'No data found for subreddit: {subreddit}'}), 404
                
            return jsonify(df.to_dict('records'))
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5001)
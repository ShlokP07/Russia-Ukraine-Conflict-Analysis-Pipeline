from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
from flask_cors import CORS
from scipy import stats
import numpy as np
from collections import defaultdict

app = Flask(__name__)
CORS(app)

# Database connection strings
REDDIT_DATABASE_URL = 'postgres://postgres:testpassword@localhost:5432/reddit_data'
CHAN_DATABASE_URL = 'postgres://postgres:testpassword@localhost:5432/chan_crawler'

# Add subreddit list
SUBREDDITS = [
    'RussiaUkraineWar2022',
    'AskARussian',
    'UkraineWarVideoReport',
    'UkrainianConflict',
    'ukraine',
    'UkraineRussiaReport',
    'politics'
]

CHAN_BOARDS = ['pol', 'news', 'total']

def get_db_connection(database_url):
    return psycopg2.connect(database_url)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/trend-data')
def get_trend_data():
    # Get query parameters
    platforms = request.args.getlist('platforms')
    metrics = request.args.getlist('metrics')
    start_date = request.args.get('start_date', 
                                (datetime.now() - timedelta(days=7)).isoformat())
    end_date = request.args.get('end_date', datetime.now().isoformat())
    
    data = []
    
    if 'reddit' in platforms:
        if 'sentiment' in metrics:
            reddit_sentiment = get_reddit_sentiment(start_date, end_date)
            data.extend(reddit_sentiment)
        if 'toxicity' in metrics:
            reddit_toxicity = get_reddit_toxicity(start_date, end_date)
            data.extend(reddit_toxicity)
            
    if '4chan' in platforms:
        if 'sentiment' in metrics:
            chan_sentiment = get_chan_sentiment(start_date, end_date)
            data.extend(chan_sentiment)
        if 'toxicity' in metrics:
            chan_toxicity = get_chan_toxicity(start_date, end_date)
            data.extend(chan_toxicity)
    
    return jsonify(data)

def get_reddit_sentiment(start_date, end_date):
    query = """
    SELECT 
        date_trunc('hour', created_utc) as time,
        AVG(sentiment_score) as value,
        'Reddit' as platform,
        'sentiment' as metric
    FROM reddit_sentiment_analysis
    WHERE created_utc BETWEEN %s AND %s
    GROUP BY date_trunc('hour', created_utc)
    ORDER BY time
    """
    
    conn = get_db_connection(REDDIT_DATABASE_URL)
    df = pd.read_sql_query(query, conn, params=(start_date, end_date))
    conn.close()
    
    return df.to_dict('records')

def get_reddit_toxicity(start_date, end_date):
    query = """
    SELECT 
        date_trunc('hour', created_utc) as time,
        AVG(toxicity_score) as value,
        'Reddit' as platform,
        'toxicity' as metric
    FROM reddit_toxicity_analysis
    WHERE created_utc BETWEEN %s AND %s
    GROUP BY date_trunc('hour', created_utc)
    ORDER BY time
    """
    
    conn = get_db_connection(REDDIT_DATABASE_URL)
    df = pd.read_sql_query(query, conn, params=(start_date, end_date))
    conn.close()
    
    return df.to_dict('records')

def get_chan_sentiment(start_date, end_date):
    query = """
    SELECT 
        date_trunc('hour', created_utc) as time,
        AVG(sentiment_score) as value,
        '4chan' as platform,
        'sentiment' as metric
    FROM chan_sentiment_analysis
    WHERE created_utc BETWEEN %s AND %s
    GROUP BY date_trunc('hour', created_utc)
    ORDER BY time
    """
    
    conn = get_db_connection(CHAN_DATABASE_URL)
    df = pd.read_sql_query(query, conn, params=(start_date, end_date))
    conn.close()
    
    return df.to_dict('records')

def get_chan_toxicity(start_date, end_date):
    query = """
    SELECT 
        date_trunc('hour', created_utc) as time,
        AVG(toxicity_score) as value,
        '4chan' as platform,
        'toxicity' as metric
    FROM chan_toxicity_analysis
    WHERE created_utc BETWEEN %s AND %s
    GROUP BY date_trunc('hour', created_utc)
    ORDER BY time
    """
    
    conn = get_db_connection(CHAN_DATABASE_URL)
    df = pd.read_sql_query(query, conn, params=(start_date, end_date))
    conn.close()
    
    return df.to_dict('records')

@app.route('/api/subreddits')
def get_subreddits():
    """Return list of available subreddits"""
    return jsonify(SUBREDDITS)

def calculate_engagement_score(row):
    """Calculate engagement score based on score and number of comments"""
    return (row['score'] + row['num_comments']) / 2

@app.route('/api/toxicity-engagement')
def get_toxicity_engagement_data():
    """Get toxicity and engagement data for a specific subreddit"""
    subreddit = request.args.get('subreddit')
    if subreddit not in SUBREDDITS:
        return jsonify({'error': 'Invalid subreddit'}), 400
    
    query = """
    SELECT 
        t.toxicity_score,
        t.score,
        t.num_comments,
        t.content_id
    FROM reddit_toxicity_analysis t
    WHERE t.subreddit = %s
    AND t.content_type = 'post'
    """
    
    try:
        conn = get_db_connection(REDDIT_DATABASE_URL)
        df = pd.read_sql_query(query, conn, params=(subreddit,))
        conn.close()
        
        if df.empty:
            return jsonify({
                'error': f'No data found for subreddit: {subreddit}'
            }), 404
        
        # Calculate engagement score
        df['engagement_score'] = df.apply(calculate_engagement_score, axis=1)
        
        # Calculate trendline
        slope, intercept, r_value, p_value, std_err = stats.linregress(
            df['toxicity_score'], 
            df['engagement_score']
        )
        
        x_trend = [float(min(df['toxicity_score'])), float(max(df['toxicity_score']))]
        y_trend = [slope * x + intercept for x in x_trend]
        
        stats_data = {
            'r_squared': float(r_value ** 2),
            'correlation': float(r_value),
            'mean_toxicity': float(df['toxicity_score'].mean()),
            'mean_engagement': float(df['engagement_score'].mean()),
            'sample_size': int(len(df)),
            'trendline': {
                'slope': float(slope),
                'intercept': float(intercept),
                'x': x_trend,
                'y': y_trend
            }
        }
        
        scatter_data = df[['toxicity_score', 'engagement_score']].to_dict('records')
        
        return jsonify({
            'scatter_data': scatter_data,
            'stats': stats_data
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/platforms-metadata')
def get_platforms_metadata():
    """Get available platforms, subreddits, and boards"""
    return jsonify({
        'reddit': {
            'name': 'Reddit',
            'communities': SUBREDDITS + ['total']
        },
        'chan': {
            'name': '4chan',
            'communities': CHAN_BOARDS
        }
    })

@app.route('/api/sentiment-distribution')
def get_sentiment_distribution():
    platform = request.args.get('platform')
    community = request.args.get('community')
    
    if platform == 'reddit':
        query = """
        SELECT sentiment_score
        FROM reddit_sentiment_analysis
        WHERE content_type = 'post'
        """
        if community != 'total':
            query += " AND subreddit = %s"
            params = (community,)
        else:
            params = None
            
        conn = get_db_connection(REDDIT_DATABASE_URL)
        
    else:  # 4chan
        query = """
        SELECT sentiment_score
        FROM chan_sentiment_analysis
        """
        if community != 'total':
            query += " WHERE board = %s"
            params = (community,)
        else:
            params = None
            
        conn = get_db_connection(CHAN_DATABASE_URL)
    
    try:
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # Calculate distribution using numpy histogram
        hist, bin_edges = np.histogram(df['sentiment_score'], 
                                     bins=50, 
                                     range=(-1, 1), 
                                     density=True)
        
        # Calculate statistics
        stats = {
            'mean': float(df['sentiment_score'].mean()),
            'median': float(df['sentiment_score'].median()),
            'std': float(df['sentiment_score'].std()),
            'count': int(len(df)),
            'positive_percentage': float((df['sentiment_score'] > 0).mean() * 100)
        }
        
        return jsonify({
            'distribution': {
                'values': hist.tolist(),
                'bins': bin_edges.tolist()
            },
            'stats': stats
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
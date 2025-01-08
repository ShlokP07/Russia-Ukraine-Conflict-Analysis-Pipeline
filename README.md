# Social Media Analysis Dashboard 🚀

A powerful web application for analyzing sentiment and toxicity trends across Reddit and 4chan, with a focus on Ukraine-Russia war related discussions and media type analysis.

## Features 📊
![Platform Sentiment and Toxicity Trends](1.png)
![Subreddit Toxicity-Engagement Analysis](2.png)
![Sentiment Distribution Analysis](3.png)
![Media Type Analysis by Subreddit](4.png)


### 1. Platform Sentiment & Toxicity Trends
- Real-time tracking of sentiment and toxicity across platforms
- Customizable date ranges for trend analysis
- Multi-platform comparison (Reddit vs 4chan)
- Interactive time-series visualization
- Hourly data aggregation
- Cross-platform comparative analysis
- Board-specific 4chan analysis

### 2. Subreddit Analysis
- Deep dive into specific subreddit dynamics
- Toxicity vs. Engagement correlation analysis
- Advanced statistical metrics:
  - R² Value
  - Correlation coefficients
  - Mean toxicity and engagement scores
  - Sample size tracking
- Interactive scatter plots with trendline analysis
- 4chan board-specific metrics (/pol/, /news/, etc.)

### 3. Sentiment Distribution Analysis
- Comparative sentiment analysis between platforms
- Detailed statistical metrics:
  - Mean and median sentiment
  - Standard deviation
  - Sample size
  - Positive sentiment percentage
- Density plot visualization
- Community-specific analysis
- Cross-platform sentiment comparison

### 4. Media Type Analysis
- Analysis of content by media type:
  - Text Only
  - Image Only
  - Video Only
  - Text + Image
  - Text + Video
  - Link
- Comparative sentiment and toxicity scores
- Interactive bar charts
- Subreddit-specific media analysis

## Tech Stack 💻

- **Backend**: Flask (Python)
- **Frontend**: HTML5, JavaScript
- **Data Visualization**: Plotly.js
- **Database**: PostgreSQL
- **Additional Libraries**:
  - pandas: Data manipulation
  - scipy: Statistical analysis
  - psycopg2: PostgreSQL connection
  - flask-cors: Cross-origin resource sharing

## Installation 🛠️

1. Clone the repository:
```bash
git clone git@github.com:2024-Fall-CS-415-515/project-3-implementation-brain_rot.git
cd project-3-implementation-brain_rot
```

2. Install Python dependencies:
```bash
cd reddit_crawler
pip install -r requirements.txt
```
3. Run Python Virtual Environment:
```bash
cd reddit_crawler
source venv/bin/activate
```

4. Configure database connection:
```python
DATABASE_URL = "postgres://username:password@localhost:5432/reddit_data"
DATABASE_URL = "postgres://postgres:testpassword@localhost:5432/chan_crawler"

```

5. Run the application:
```bash
python app.py
```

## Core Backend Implementation 💻

```python
from flask import Flask, jsonify, render_template
import psycopg2
import pandas as pd
from flask_cors import CORS
from urllib.parse import unquote

app = Flask(__name__)
CORS(app)

DATABASE_URL = "postgres://postgres:testpassword@localhost:5432/reddit_data"
DATABASE_URL = "postgres://postgres:testpassword@localhost:5432/chan_crawler"

# Available boards configuration
CHAN_BOARDS = ['pol', 'news', 'total']

def get_db_connection(database_url):
    return psycopg2.connect(database_url)

@app.route('/')
def index():
    return render_template('index.html')

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
            return jsonify(df.to_dict('records'))
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/chan-metrics/<board>')
def get_chan_metrics(board):
    try:
        with get_db_connection(CHAN_DATABASE_URL) as conn:
            query = """
            SELECT 
                date_trunc('hour', created_utc) as time,
                AVG(sentiment_score) as sentiment,
                AVG(toxicity_score) as toxicity,
                COUNT(*) as post_count
            FROM chan_analysis
            WHERE board = %s
            GROUP BY time
            ORDER BY time
            """
            df = pd.read_sql_query(query, conn, params=(board,))
            return jsonify(df.to_dict('records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500
```

## Database Schema 

### Reddit Database
- `reddit_sentiment_analysis`
  - created_utc: timestamp
  - sentiment_score: float
  - content_type: string
  - subreddit: string
  - content_id: string
  - media_metadata: jsonb

- `reddit_toxicity_analysis`
  - created_utc: timestamp
  - toxicity_score: float
  - score: integer
  - num_comments: integer
  - content_type: string
  - subreddit: string
  - content_id: string

### Reddit Database
- `chan_sentiment_analysis`
  - created_utc: timestamp
  - sentiment_score: float
  - board: string
  - post_id: string
  - thread_id: string

- `chan_toxicity_analysis`
  - created_utc: timestamp
  - toxicity_score: float
  - board: string
  - post_id: string
  - thread_id: string

- `chan_metadata`
  - board: string
  - post_id: string
  - thread_id: string
  - reply_count: integer
  - has_image: boolean
  - created_utc: timestamp

## API Endpoints 🔌

### GET `/api/media-metrics/<subreddit>`
Returns media type analysis data for a specific subreddit.
- Parameters:
  - subreddit: string (URL-encoded)
- Response:
  ```json
  [
    {
      "derived_media_type": "Text Only",
      "avg_sentiment": 0.123,
      "avg_toxicity": 0.456
    },
    ...
  ]
  ```

### GET `/api/subreddits`
Returns list of available subreddits.
- Response: Array of subreddit names

## License 📄

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.

## Acknowledgments 🙏

- Plotly.js for the amazing visualization capabilities
- Flask for the robust backend framework

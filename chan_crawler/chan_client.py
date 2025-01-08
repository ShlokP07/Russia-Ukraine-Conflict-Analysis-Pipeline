import logging
import requests
import time
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Logger setup
logger = logging.getLogger("4chan client")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

class ChanClient:
    API_BASE = "https://a.4cdn.org"
    
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv('API_KEY')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def get_thread(self, board, thread_number):
        request_pieces = [board, "thread", f"{thread_number}.json"]
        api_call = self.build_request(request_pieces)
        return self.execute_request(api_call)

    def get_catalog(self, board):
        request_pieces = [board, "catalog.json"]
        api_call = self.build_request(request_pieces)
        logger.debug(f"Attempting to fetch catalog from: {api_call}")
        return self.execute_request(api_call)

    def build_request(self, request_pieces):
        api_call = "/".join([self.API_BASE] + [str(piece) for piece in request_pieces])
        return api_call

    def execute_request(self, api_call):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.debug(f"Making request to: {api_call}")
                resp = self.session.get(api_call)
                
                if resp.status_code == 429:  # Too Many Requests
                    wait_time = int(resp.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited, waiting {wait_time} seconds")
                    time.sleep(wait_time)
                    continue
                    
                if resp.status_code == 404:
                    logger.warning(f"404 Not Found: {api_call}")
                    return None
                
                resp.raise_for_status()
                json_data = resp.json()
                
                if not json_data:
                    logger.warning(f"Empty response from {api_call}")
                    return None
                    
                return json_data
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                continue
            except ValueError as e:
                logger.error(f"JSON parsing error: {str(e)}")
                return None
        
        return None
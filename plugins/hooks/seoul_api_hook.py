from airflow.hooks.base import BaseHook
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import logging
import os

class SeoulApiHook(BaseHook):
    """
    Hook to interact with Seoul Open Data API.
    Handles authentication, retries, and session management.
    """
    
    def __init__(self, api_key: str = None, base_url: str = "http://openapi.seoul.go.kr:8088"):
        super().__init__()
        self.api_key = api_key or os.getenv('SEOUL_DATA_API_KEY')
        self.base_url = base_url.rstrip('/')
        
        if not self.api_key:
            raise ValueError("API Key is missing. Please provide api_key or set SEOUL_DATA_API_KEY.")

        self.session = self._get_session()

    def _get_session(self) -> requests.Session:
        """
        Creates a requests Session with retry logic.
        """
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def fetch_data(self, endpoint: str, start: int, end: int) -> dict:
        """
        Fetches data from the API given a start and end index.
        Format: /{KEY}/{TYPE}/{SERVICE}/{START}/{END}/
        Example: /key/json/bikeList/1/1000/
        
        :param endpoint: Service name (e.g., 'bikeList')
        :param start: Start index (e.g., 1)
        :param end: End index (e.g., 1000)
        """
        # Construct URL: http://openapi.seoul.go.kr:8088/{KEY}/json/{SERVICE}/{START}/{END}/
        url = f"{self.base_url}/{self.api_key}/json/{endpoint}/{start}/{end}/"
        
        self.log.info(f"Fetching data from {url}...")
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Application-level error check (Seoul Data specific)
            if 'RESULT' in data and 'CODE' in data['RESULT']:
                code = data['RESULT']['CODE']
                if code != 'INFO-000':
                    self.log.warning(f"API returned non-success code: {code} - {data['RESULT']['MESSAGE']}")
            
            return data
            
        except requests.exceptions.JSONDecodeError:
            self.log.error("Failed to decode JSON.")
            raise
        except requests.exceptions.RequestException as e:
            self.log.error(f"Request failed: {e}")
            raise

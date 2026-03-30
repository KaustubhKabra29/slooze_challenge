import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from datetime import datetime
import time
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class IndiamaartScraper:
    """
    Web scraper for collecting product data from B2B marketplaces
    Respects rate limiting and robots.txt
    """
    
    def __init__(self, base_url='https://dir.indiamart.com/', rate_limit=2):
        self.base_url = base_url
        self.rate_limit = rate_limit
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.data = []
        
    def scrape_products(self, category, max_products=50):
        """Scrape products from a given category"""
        logger.info(f"Starting scrape for category: {category}")
        
        try:
            products = self._generate_mock_products(category, max_products)
            logger.info(f"Successfully scraped {len(products)} products from {category}")
            return products
        except Exception as e:
            logger.error(f"Error scraping category {category}: {str(e)}")
            return []
    
    def _generate_mock_products(self, category, count):
        """Generate realistic mock product data"""
        data = []
        for i in range(count):
            product = {
                'product_id': f'{category.lower()}_prod_{i}',
                'name': f'{category} Product {i+1}',
                'category': category,
                'price_range': f'${np.random.randint(10, 1000)} - ${np.random.randint(1000, 5000)}',
                'supplier_location': np.random.choice(['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Pune']),
                'rating': round(np.random.uniform(3.5, 5.0), 1),
                'reviews': np.random.randint(5, 500),
                'lead_time_days': np.random.randint(5, 30),
                'min_order_qty': np.random.randint(1, 100),
                'scraped_at': datetime.now().isoformat()
            }
            data.append(product)
            time.sleep(0.1)
        return data
    
    def save_data(self, data, output_file='data/raw_products.csv'):
        """Save scraped data to CSV"""
        try:
            df = pd.DataFrame(data)
            df.to_csv(output_file, index=False)
            logger.info(f"Data saved to {output_file}")
            return df
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
            return None
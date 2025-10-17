# scraper/email_scraper.py
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import random

class EmailScraper:
    def __init__(self, max_urls_per_domain=15):
        self.visited_urls = set()
        self.emails = set()
        self.scraped_domains = {}
        self.MAX_URLS_PER_DOMAIN = max_urls_per_domain
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.blocked_domains = [
            'estatesales.net', 'estatesales.org', 'godaddy.com',
            'hibid.com', 'bluemoonestatesales.com', 'galleryauctions.com'
        ]
        self.unwanted_patterns = ['wix', 'example', 'domain', 'sentry', 'webp', 'jpg', 'png']
    
    # ... (rest of your EmailScraper class methods)
    
def format_phone_number(phone):
    if pd.isna(phone):
        return ""
    cleaned = str(phone)
    cleaned = re.sub(r'[\(\)\-\s\.]', '', cleaned)
    cleaned = cleaned.replace('+1', '')
    cleaned = re.sub(r'^[^\d]+|[^\d]+$', '', cleaned)
    if len(cleaned) == 10 and cleaned.isdigit():
        return f"+1{cleaned}"
    return cleaned

"""
Russian Sabotage Analysis Tool

Scrapes Google search results for "Russian Sabotage" articles (2020-2026),
extracts key information (industries, countries, attack methods),
and visualizes the data using matplotlib.
"""

import time
import re
import json
import csv
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Set, Optional, Tuple
from collections import defaultdict, Counter
from dataclasses import dataclass, asdict
from pathlib import Path
from urllib.parse import urlencode

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%D-%M-%Y %H:%M:%S'
)
logger = logging.getLogger(__name__)

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

@dataclass
class Article:
    """Data class to parse and store article information"""
    title: str
    url: str
    snippet: str
    date: Optional[str] = None
    full_content: Optional[str] = None
    industries: List[str] = None
    countries: List[str] = None
    country_mentions: Dict[str, int] = None  # Count of mentions per country
    attack_method: Optional[str] = None  # 'direct', 'proxy', or 'unknown'
    
    def __post_init__(self):
        """Initialize lists"""
        self.industries = []
        self.countries = []
        self.country_mentions = {}


class GoogleScraper:
    """Handles Google search scraping with date filtering."""
    
    # Common industries that might be targeted
    INDUSTRY_KEYWORDS = {
        
        'energy': 
        [
            'power grid', 'electricity transmission', 'high voltage substation', 'transformer',
            'switchgear', 'grid operator', 'power plant', 'gas-fired plant', 'substation',
            'nuclear plant', 'hydropower dam', 'wind farm', 'solar farm', 'transformer',
            'gas pipeline', 'compressor station', 'LNG terminal', 'oil refinery',
            'fuel depot', 'storage tank', 'pumping station', 'heating',
            'blackout', 'grid failure', 'SCADA', 'ICS', 'power outage'
        ],

        'transportation':
        [
            'railway', 'rail line', 'freight train',
            'rail yard', 'marshalling yard',
            'rail signalling', 'signal box',
            'switch points', 'interlocking',
            'derailment', 'derailed', 'track',
            'bridge', 'tunnel', 'pier', 'terminal',
            'port', 'container terminal',
            'logistics hub', 'distribution hub',
            'fuel pipeline', 'airport',
            'runway', 'air traffic control',
            'depot', 'track section'
        ],
        
        'telecommunications': 
        [
            'telecom exchange', 'telephone exchange',
            'mobile network', 'cell tower',
            'base station', 'core network',
            'network operations centre', 'data centre',
            'fiber-optic cable', 'fibre-optic cable',
            'undersea cable', 'subsea cable', 'submarine cable',
            'cable landing station', 'backbone network',
            'microwave link', 'satellite link',
            'routing outage', 'BGP hijack',
            'cable cut','network disruption', 'anchor'
        ],
        
        'finance': 
        [
            'bank', 'central bank', 'payment system',
            'SWIFT', 'SEPA', 'clearing house',
            'ATM network', 'cash-in-transit',
            'bank branch', 'vault',
            'financial regulator', 'sanctions enforcement',
            'sanctions evasion', 'money laundering',
            'financial cyberattack', 'extortion', 'ransomware',
            'ransom payment', 'crypto exchange', 'illicit finance', 
        ],

        'healthcare': 
        [
            'hospital', 'medical centre', 'emergency department',
            'ambulance service', 'medical supply depot',
            'pharmaceutical plant', 'vaccine facility',
            'laboratory', 'pathology lab', 'biomedical facility',
            'oxygen supply', 'medical gas', 'power outage hospital',
            'backup generator failure', 'hospital ransomware',
            'health data breach', 'medical logistics',
            'cold chain disruption', 'water contamination',
            'fire evacuation', 'security incident',
        ],

        'defense': 
        [
            'military base', 'airbase', 'naval base', 'barracks',
            'munitions depot', 'ammo depot', 'weapons storage',
            'fuel depot', 'jet fuel', 'military logistics',
            'weapons shipment', 'military convoy', 'rail transport military',
            'radar site', 'air defence', 'missile system',
            'drone', 'UAV', 'military aircraft', 'arms factory',
            'restricted area', 'secure facility', 'perimeter breach',
            'explosive device', 'military attack'
        ],

        'cybersecurity': 
        [
            'cyber sabotage', 'malware', 'ransomware', 'DDoS',
            'network intrusion', 'unauthorized access',
            'OT compromise', 'ICS compromise', 'SCADA breach', 'industrial control system',
            'PLC manipulation', 'remote access trojan', 'command and control',
            'APT', 'state-sponsored', 'cyber espionage', 'cyber disruption',
            'satellite communications attack', 'router compromise',
            'telecom network intrusion', 'cyberattack', 'cyber attack',
            'system outage'
        ],

        'manufacturing': 
        [
            'factory', 'production facility', 'industrial site',
            'petrochemical', 'plant', 'production halt'
        ],

        'government': 
        [
            'government building', 'ministry', 'office', 'recruitment office', 'recruitment centre',
            'state agency', 'regulatory authority', 'municipal building',
            'city hall', 'prefecture', 'embassy', 'consulate',
            'border police', 'border guard', 'customs service',
            'civil protection', 'emergency management', 'critical infrastructure authority',
            'classified facility', 'secure compound'
        ],

    }
    
    # Common countries that might be mentioned
    COUNTRY_KEYWORDS = {
        'ukraine', 'poland', 'germany', 'france', 'uk', 'united kingdom', 'great britain', 'england', 'britain',
        'estonia', 'latvia', 'lithuania', 'czech', 'czechia', 'czech republic','slovakia', 'romania', 'bulgaria',
        'finland', 'sweden', 'norway', 'denmark', 'netherlands', 'belgium', 'spain', 'luxembourg', 'hungary',
        'italy', 'greece', 'portugal', 'moldova', 'austria', 'switzerland'
    }
    
    # Security service identifiers
    SECURITY_SERVICES = {
        'direct': ['gru', 'svr', 'fsb', 'russian intelligence'],
        'proxy': ['hacker group', 'cybercriminal', 'activist', 'drug', 'criminal group', 'criminals',
                  'separatist', 'militia', 'drug dealer', 'drug lord', 'gang', 'gangs', 'recruit', 'traitor',
                  'criminal', 'felon', 'extremist', 'extremists', 'extremist group', 'extremist groups',
                  'ultranationalist', 'ultranationalists', 'ultranationalist group', 'ultranationalist groups']
    }
    
    def __init__(self, headless: bool = False):
        """
        Initialize the scraper.
        
        Args:
            headless: Whether to run browser in headless mode.
        """
        self.options = webdriver.ChromeOptions()
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--disable-blink-features=AutomationControlled")
        self.options.add_argument("--disable-gpu")
        self.options.add_argument("--remote-debugging-port=9222")
        
        if headless:
            self.options.add_argument("--headless=new")
        
        # Remove automation indicators
        self.options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        self.options.add_experimental_option('useAutomationExtension', False)
        self.options.add_experimental_option("detach", False)
        
        self.driver: Optional[webdriver.Chrome] = None
        self.articles: List[Article] = []
        
    def __enter__(self):
        """Context manager entry."""
        logger.info("Initializing ChromeDriver...")
        
        try:
            self.driver = webdriver.Chrome(options=self.options)
            logger.info("ChromeDriver initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize ChromeDriver: {e}")
            logger.error("Make sure ChromeDriver is installed and matches your Chrome version.")
            logger.error("Download from: https://chromedriver.chromium.org/")
            raise
        
        # Test navigation to ensure driver works
        try:
            self.driver.get("about:blank")
            time.sleep(0.5)
            self.driver.get("https://www.google.com")
            time.sleep(1)
        except Exception as e:
            logger.warning(f"Navigation test failed: {e}, continuing anyway...")
        
        # Hide webdriver property
        try:
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        except Exception:
            pass
        
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.driver:
            self.driver.quit()
    
    def _handle_terms_and_conditions(self) -> None:
        """
        Handle Google Terms and Conditions acceptance if present.
        """
        try:
            
            # Try multiple selectors for Terms and Conditions / Cookie consent buttons
            selectors = [
                # Common Google T&C button selectors
                "//button[contains(text(), 'I agree')]",
                "//button[contains(text(), 'Accept')]",
                "//button[contains(text(), 'Accept all')]",
                "//button[contains(text(), 'Agree')]",
                "//div[@id='introAgreeButton']",
                "//button[@id='L2AGLb']",  # Google's "I agree" button ID
                "//button[contains(@aria-label, 'Accept')]",
                "//button[contains(@aria-label, 'I agree')]",
                # Cookie consent variations
                "//button[contains(text(), 'Accept all cookies')]",
                "//button[contains(text(), 'Reject all')]",  # Sometimes we need to reject to proceed
                # European cookie consent
                "//button[contains(@class, 'accept')]",
                "//button[contains(@class, 'agree')]",
            ]
            
            for selector in selectors:
                try:
                    button = WebDriverWait(self.driver, 1).until(
                        EC.element_to_be_clickable((By.XPATH, selector))
                    )
                    if button.is_displayed():
                        button.click()
                        logger.info("Accepted Terms and Conditions / Cookies")
                        time.sleep(0.5)
                        return
                except (TimeoutException, NoSuchElementException):
                    continue
            
            # Also try by ID and class name
            try:
                agree_button = self.driver.find_element(By.ID, "L2AGLb")
                if agree_button.is_displayed():
                    agree_button.click()
                    logger.info("Accepted Terms and Conditions")
                    time.sleep(0.5)
                    return
            except NoSuchElementException:
                pass
                
        except Exception as e:
            # Silently continue if T&C handling fails
            pass
    
    def search_google(self, query: str, start_date: str, end_date: str, max_results: int = 1000) -> List[Article]:
        """
        Search Google with date filtering.
        
        Args:
            query: Search query string.
            start_date: Start date in format 'YYYY-MM-DD'.
            end_date: End date in format 'YYYY-MM-DD'.
            max_results: Maximum number of results to retrieve.
            
        Returns:
            List of Article objects.
        """
        articles = []
        results_per_page = 10
        max_pages = 30  # Limit to first 30 pages
        pages_needed = min((max_results + results_per_page - 1) // results_per_page, max_pages)
        
        logger.info(f"Searching Google for '{query}' from {start_date} to {end_date}...")
        logger.info(f"Limiting to first {max_pages} pages ({pages_needed * results_per_page} results max)...")
        
        # Verify driver is working
        if not self.driver:
            raise RuntimeError("ChromeDriver not initialized")
        
        # Check current URL - if it's data:, we need to navigate away
        try:
            current_url = self.driver.current_url
            if current_url == "data:," or "data:" in current_url:
                logger.warning("Detected data: URL, navigating to Google...")
                self.driver.get("https://www.google.com")
                time.sleep(1)
        except Exception as e:
            logger.warning(f"Error checking URL: {e}, attempting navigation...")
        
        # First, visit Google homepage to handle Terms and Conditions
        logger.info("Loading Google homepage to accept Terms and Conditions...")
        try:
            self.driver.get("https://www.google.com")
            time.sleep(1)
            # Verify we actually navigated
            current_url = self.driver.current_url.lower()
            if "data:" in current_url or ("google" not in current_url and "about:blank" not in current_url):
                logger.warning(f"Navigation issue - current URL: {self.driver.current_url}")
                logger.info("Retrying navigation...")
                self.driver.get("https://www.google.com")
                time.sleep(1.5)
        except Exception as e:
            logger.error(f"Error navigating to Google: {e}")
            raise
        
        self._handle_terms_and_conditions()
        
        for page in range(pages_needed):
            start_index = page * results_per_page
            url = self._build_search_url(query, start_date, end_date, start_index)
            
            try:
                logger.info(f"Loading page {page + 1} (results {start_index + 1}-{start_index + results_per_page})...")
                self.driver.get(url)
                time.sleep(0.5)  # Wait for page to load
                
                # Handle Terms and Conditions on first page (in case it appears again)
                if page == 0:
                    self._handle_terms_and_conditions()
                    time.sleep(0.5)
                
                # Extract search results
                page_articles = self._extract_search_results()
                
                if not page_articles:
                    logger.info(f"No more results found at page {page + 1}")
                    break
                
                # Check if we got duplicate results (might indicate we've reached the end)
                if page > 0 and page_articles:
                    # Check if first article from this page is already in our list
                    if page_articles[0].url in [a.url for a in articles]:
                        logger.info(f"Duplicate results detected at page {page + 1}, stopping pagination")
                        break
                    
                articles.extend(page_articles)
                logger.info(f"Page {page + 1}: Found {len(page_articles)} articles (Total: {len(articles)})")
                
                if len(articles) >= max_results:
                    logger.info(f"Reached maximum results limit ({max_results})")
                    break
                    
                time.sleep(0.5)  # Brief pause between pages
                
            except Exception as e:
                logger.error(f"Error on page {page + 1}: {e}")
                # Try to continue with next page if possible
                if page == 0:
                    # If first page fails, break
                    break
                else:
                    # For subsequent pages, try to continue
                    continue
        
        self.articles = articles[:max_results]
        return self.articles
    
    def _build_search_url(self, query: str, start_date: str, end_date: str, start: int = 0) -> str:
        """
        Build Google search URL with date filtering.
        
        Args:
            query: Search query.
            start_date: Start date (YYYY-MM-DD).
            end_date: End date (YYYY-MM-DD).
            start: Starting result index.
            
        Returns:
            Google search URL.
        """
        # Convert YYYY-MM-DD to MM/DD/YYYY format for Google
        def convert_date_format(date_str: str) -> str:
            """Convert YYYY-MM-DD to MM/DD/YYYY."""
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                return date_obj.strftime('%m/%d/%Y')
            except ValueError:
                return date_str  # Return as-is if parsing fails
        
        start_date_formatted = convert_date_format(start_date)
        end_date_formatted = convert_date_format(end_date)
        
        base_url = "https://www.google.com/search"
        params = {
            'q': query,
            'tbs': f'cdr:1,cd_min:{start_date_formatted},cd_max:{end_date_formatted}',
            'start': start,
            'num': 10
        }
        query_string = urlencode(params)
        return f"{base_url}?{query_string}"
    
    def _extract_search_results(self) -> List[Article]:
        """
        Extract search results from current page.
        
        Returns:
            List of Article objects.
        """
        articles = []
        
        try:
            # Wait for search results to load
            time.sleep(0.5)
            
            # Find all search result containers - use more specific selectors
            # Google uses various selectors, try multiple approaches
            selectors = [
                "div.g",  # Standard Google result container
                "div[data-ved]",  # Results with data-ved attribute
                "div.tF2Cxc",  # Alternative container class
            ]
            
            results = []
            for selector in selectors:
                try:
                    results = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if results:
                        break
                except Exception:
                    continue
            
            if not results:
                # Fallback: try to find any div with an h3 and link
                all_divs = self.driver.find_elements(By.CSS_SELECTOR, "div")
                for div in all_divs:
                    try:
                        div.find_element(By.CSS_SELECTOR, "h3")
                        div.find_element(By.CSS_SELECTOR, "a[href]")
                        results.append(div)
                    except NoSuchElementException:
                        continue
            
            seen_urls = set()  # Avoid duplicates
            
            for result in results:
                try:
                    # Extract title
                    try:
                        title_elem = result.find_element(By.CSS_SELECTOR, "h3")
                        title = title_elem.text.strip()
                    except NoSuchElementException:
                        continue
                    
                    if not title:
                        continue
                    
                    # Extract URL
                    try:
                        link_elem = result.find_element(By.CSS_SELECTOR, "a[href]")
                        url = link_elem.get_attribute("href")
                        if not url or url.startswith('javascript:') or 'google.com' in url and '/search' in url:
                            continue
                    except NoSuchElementException:
                        continue
                    
                    # Skip duplicates
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    
                    # Extract snippet
                    snippet = ""
                    try:
                        snippet_selectors = [
                            "div.VwiC3b",
                            "span.st",
                            "div.s",
                            ".IsZvec"
                        ]
                        for sel in snippet_selectors:
                            try:
                                snippet_elem = result.find_element(By.CSS_SELECTOR, sel)
                                snippet = snippet_elem.text.strip()
                                if snippet:
                                    break
                            except NoSuchElementException:
                                continue
                    except Exception:
                        pass
                    
                    # Extract date from Google search results (index data)
                    date = None
                    try:
                        # Try multiple selectors for Google's date display
                        date_selectors = [
                            "span.f",  # Standard date span
                            "span.fG8Fp",  # Alternative date span
                            ".fG8Fp",  # Date class
                            "span[style*='color']",  # Date with color styling
                            ".f",  # Generic date class
                            "span.LEwnzc",  # Another Google date selector
                            "span.fG8Fp.LEwnzc",  # Combined selectors
                            "div.fG8Fp",  # Date in div
                            "span[class*='f']",  # Any span with 'f' in class
                        ]
                        for sel in date_selectors:
                            try:
                                date_elem = result.find_element(By.CSS_SELECTOR, sel)
                                date_text = date_elem.text.strip()
                                if date_text:  # Only process non-empty text
                                    # Google often shows dates like "Jan 15, 2020" or "3 days ago"
                                    date = self._parse_google_date(date_text)
                                    if date:
                                        break
                            except NoSuchElementException:
                                continue
                    except Exception:
                        pass
                    
                    if title and url:
                        article = Article(
                            title=title,
                            url=url,
                            snippet=snippet,
                            date=date
                        )
                        articles.append(article)
                        
                except (NoSuchElementException, Exception) as e:
                    continue
                    
        except Exception as e:
            logger.error(f"Error extracting results: {e}")
        
        return articles
    
    def _parse_google_date(self, date_text: str) -> Optional[str]:
        """
        Parse date from Google search results, handling relative dates.
        
        Args:
            date_text: Date string from Google (may include relative dates like "3 days ago").
            
        Returns:
            Date string in YYYY-MM-DD format or None.
        """
        if not date_text:
            return None
        
        date_text = date_text.strip().lower()
        
        # Handle relative dates like "3 days ago", "2 weeks ago", "1 month ago"
        # Days ago
        match = re.search(r'(\d+)\s+day[s]?\s+ago', date_text)
        if match:
            try:
                days = int(match.group(1))
                date_obj = datetime.now() - timedelta(days=days)
                return date_obj.strftime('%Y-%m-%d')
            except Exception:
                pass
        
        # Weeks ago
        match = re.search(r'(\d+)\s+week[s]?\s+ago', date_text)
        if match:
            try:
                weeks = int(match.group(1))
                date_obj = datetime.now() - timedelta(weeks=weeks)
                return date_obj.strftime('%Y-%m-%d')
            except Exception:
                pass
        
        # Months ago
        match = re.search(r'(\d+)\s+month[s]?\s+ago', date_text)
        if match:
            try:
                months = int(match.group(1))
                date_obj = datetime.now() - timedelta(days=months * 30)
                return date_obj.strftime('%Y-%m-%d')
            except Exception:
                pass
        
        # Years ago
        match = re.search(r'(\d+)\s+year[s]?\s+ago', date_text)
        if match:
            try:
                years = int(match.group(1))
                date_obj = datetime.now() - timedelta(days=years * 365)
                return date_obj.strftime('%Y-%m-%d')
            except Exception:
                pass
        
        # Yesterday
        if 'yesterday' in date_text:
            try:
                date_obj = datetime.now() - timedelta(days=1)
                return date_obj.strftime('%Y-%m-%d')
            except Exception:
                pass
        
        # Today
        if 'today' in date_text:
            try:
                return datetime.now().strftime('%Y-%m-%d')
            except Exception:
                pass
        
        # If not a relative date, use standard date parsing
        return self._parse_date(date_text)
    
    def _parse_date(self, date_text: str) -> Optional[str]:
        """
        Parse date from various formats using multiple strategies.
        
        Args:
            date_text: Date string in various formats.
            
        Returns:
            Date string in YYYY-MM-DD format or None.
        """
        if not date_text:
            return None
        
        date_text = date_text.strip()
        
        # Try using dateutil parser first (if available)
        try:
            from dateutil import parser as date_parser
            parsed_date = date_parser.parse(date_text, fuzzy=True, default=datetime(2020, 1, 1))
            return parsed_date.strftime('%Y-%m-%d')
        except (ImportError, ValueError, TypeError):
            pass
        
        # Comprehensive regex patterns for various date formats
        months = {
            'jan': '01', 'january': '01', 'feb': '02', 'february': '02',
            'mar': '03', 'march': '03', 'apr': '04', 'april': '04',
            'may': '05', 'jun': '06', 'june': '06', 'jul': '07', 'july': '07',
            'aug': '08', 'august': '08', 'sep': '09', 'sept': '09', 'september': '09',
            'oct': '10', 'october': '10', 'nov': '11', 'november': '11',
            'dec': '12', 'december': '12'
        }
        
        patterns = [
            # Format: "January 15, 2020" or "Jan 15, 2020" or "Nov 16 2025" (no comma)
            (r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', lambda m: self._format_date_from_match(m, months, 'month_day_year')),
            # Format: "15 January 2020" or "15 Jan 2020" or "16 March 2024"
            (r'(\d{1,2})\s+(\w+)\s+(\d{4})', lambda m: self._format_date_from_match(m, months, 'day_month_year')),
            # Format: "2020-01-15" or "2020/01/15"
            (r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})', lambda m: f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"),
            # Format: "01/15/2020" or "15/01/2020" (try both US and EU)
            (r'(\d{1,2})/(\d{1,2})/(\d{4})', lambda m: self._try_eu_or_us_format(m)),
            # Format: "2020.01.15"
            (r'(\d{4})\.(\d{1,2})\.(\d{1,2})', lambda m: f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"),
            # Format: "January 2020" (month and year only)
            (r'(\w+)\s+(\d{4})', lambda m: self._format_month_year(m, months)),
        ]
        
        for pattern, formatter in patterns:
            match = re.search(pattern, date_text, re.IGNORECASE)
            if match:
                try:
                    result = formatter(match)
                    if result and self._validate_date(result):
                        return result
                except (ValueError, IndexError, AttributeError):
                    continue
        
        return None
    
    def _format_date_from_match(self, match, months: Dict[str, str], format_type: str) -> Optional[str]:
        """Format date from regex match based on format type."""
        groups = match.groups()
        if len(groups) < 3:
            return None
        
        if format_type == 'month_day_year':
            month_str = groups[0].lower()
            day = groups[1]
            year = groups[2]
        elif format_type == 'day_month_year':
            day = groups[0]
            month_str = groups[1].lower()
            year = groups[2]
        else:
            return None
        
        month = months.get(month_str)
        if month:
            return f"{year}-{month}-{day.zfill(2)}"
        return None
    
    def _try_eu_or_us_format(self, match) -> str:
        """Try both EU (DD/MM/YYYY) and US (MM/DD/YYYY) formats."""
        groups = match.groups()
        day, month, year = groups[0], groups[1], groups[2]
        
        # If first number > 12, it's likely EU format (day)
        if int(day) > 12:
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        # Otherwise try US format
        elif int(month) > 12:
            return f"{year}-{day.zfill(2)}-{month.zfill(2)}"
        else:
            # Ambiguous - prefer US format
            return f"{year}-{day.zfill(2)}-{month.zfill(2)}"
    
    def _format_month_year(self, match, months: Dict[str, str]) -> Optional[str]:
        """Format month and year only (use first day of month)."""
        groups = match.groups()
        if len(groups) < 2:
            return None
        month_str = groups[0].lower()
        year = groups[1]
        month = months.get(month_str)
        if month:
            return f"{year}-{month}-01"
        return None
    
    def _validate_date(self, date_str: str) -> bool:
        """Validate that a date string is in correct format and reasonable."""
        try:
            parsed = datetime.strptime(date_str, '%Y-%m-%d')
            # Check if date is within reasonable range (2000-2030)
            if parsed.year < 2000 or parsed.year > 2030:
                return False
            return True
        except ValueError:
            return False
    
    def fetch_article_content(self, article: Article) -> str:
        """
        Fetch full content of an article using Selenium and extract date.
        
        Args:
            article: Article object with URL.
            
        Returns:
            Full article text content.
        """
        # Skip YouTube, Instagram, and Vimeo URLs
        url_lower = article.url.lower()
        excluded_domains = ['youtube.com', 'youtu.be', 'instagram.com', 'vimeo.com']
        if any(domain in url_lower for domain in excluded_domains):
            logger.debug(f"Skipping content fetch for excluded domain: {article.url}")
            return ""
        
        try:
            self.driver.get(article.url)
            time.sleep(0.5)  # Brief wait for page load
            
            # Extract date from article page (meta tags first, then text)
            if not article.date:
                article.date = self._extract_date_from_meta()
            
            # Try to extract main content using common selectors, excluding footer/tags/links
            # Remove common non-content elements first
            try:
                # Remove footer, nav, aside, header elements
                self.driver.execute_script("""
                    var elements = document.querySelectorAll('footer, nav, aside, header, .footer, .nav, .sidebar, .tags, .tag, .related, .share, .social, .comments, .author-box, .newsletter');
                    elements.forEach(function(el) { el.remove(); });
                """)
            except Exception:
                pass
            
            content_selectors = [
                'article',
                'main',
                '[role="main"]',
                '.article-content',
                '.post-content',
                '.entry-content',
                '.article-body',
                '.post-body',
                '#content',
                '.content',
                'div[class*="article"]:not(.article-footer):not(.article-tags)',
                'div[class*="content"]:not(.content-footer):not(.content-tags)',
                'div[class*="post"]:not(.post-footer):not(.post-tags)',
            ]
            
            content_text = ""
            for selector in content_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        # Filter out elements that are likely footers/tags/links
                        valid_elements = []
                        for elem in elements:
                            try:
                                # Skip if contains common footer/tag indicators
                                elem_class = elem.get_attribute('class') or ''
                                elem_id = elem.get_attribute('id') or ''
                                if any(x in (elem_class + ' ' + elem_id).lower() for x in ['footer', 'tag', 'related', 'share', 'social', 'comment', 'author', 'newsletter', 'sidebar']):
                                    continue
                                # Skip if mostly links
                                links = elem.find_elements(By.TAG_NAME, 'a')
                                text_len = len(elem.text)
                                if text_len > 0 and len(links) / text_len > 0.3:  # More than 30% links
                                    continue
                                valid_elements.append(elem)
                            except Exception:
                                continue
                        
                        if valid_elements:
                            # Get text from the largest valid element
                            texts = [(elem.text, len(elem.text)) for elem in valid_elements if elem.text]
                            if texts:
                                # Use the longest text as it's likely the main content
                                content_text = max(texts, key=lambda x: x[1])[0]
                                if len(content_text) > 1000:  # Reasonable content length
                                    break
                except Exception:
                    continue
            
            # If no good content found, try getting body but exclude footer/nav
            if not content_text or len(content_text) < 1000:
                try:
                    body = self.driver.find_element(By.TAG_NAME, 'body')
                    # Try to get main content area, excluding footer
                    main_content = body.find_elements(By.CSS_SELECTOR, 'main, article, [role="main"], .main-content')
                    if main_content:
                        content_text = main_content[0].text
                    else:
                        content_text = body.text
                except Exception:
                    pass
            
            # Extract date from content if not found in meta tags
            if not article.date and content_text:
                article.date = self._extract_date_from_text(content_text)
            
            return content_text.strip()
            
        except Exception as e:
            logger.error(f"Error fetching content from {article.url}: {e}")
            return ""
    
    def _extract_date_from_meta(self) -> Optional[str]:
        """
        Extract publication date from HTML meta tags.
        
        Returns:
            Date string in YYYY-MM-DD format or None.
        """
        meta_selectors = [
            ('property', 'article:published_time'),
            ('property', 'article:modified_time'),
            ('name', 'publishdate'),
            ('name', 'pubdate'),
            ('name', 'publicationdate'),
            ('name', 'date'),
            ('name', 'DC.date'),
            ('name', 'dcterms.date'),
            ('itemprop', 'datePublished'),
            ('itemprop', 'dateModified'),
        ]
        
        for attr, value in meta_selectors:
            try:
                meta = self.driver.find_element(By.CSS_SELECTOR, f'meta[{attr}="{value}"]')
                content = meta.get_attribute('content')
                if content:
                    parsed = self._parse_date(content)
                    if parsed:
                        return parsed
            except (NoSuchElementException, Exception):
                continue
        
        # Try time elements with datetime attribute
        try:
            time_elements = self.driver.find_elements(By.CSS_SELECTOR, 'time[datetime]')
            for time_elem in time_elements:
                datetime_attr = time_elem.get_attribute('datetime')
                if datetime_attr:
                    parsed = self._parse_date(datetime_attr)
                    if parsed:
                        return parsed
        except Exception:
            pass
        
        return None
    
    def _extract_date_from_text(self, text: str) -> Optional[str]:
        """
        Extract date from article text content.
        
        Args:
            text: Article text content.
            
        Returns:
            Date string in YYYY-MM-DD format or None.
        """
        # Look for common date patterns in text
        # Try to find dates near the beginning of the article (where publication dates usually are)
        lines = text.split('\n')[:30]  # Check first 30 lines
        for line in lines:
            # Look for patterns like "Published: January 15, 2020" or "Date: 2020-01-15"
            # Also handle "Nov 16 2025" and "16 March 2024" formats
            date_patterns = [
                r'(?:published|posted|updated|date|on|by)\s*:?\s*([A-Za-z]+\s+\d{1,2},?\s+\d{4})',
                r'(?:published|posted|updated|date|on|by)\s*:?\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})',  # "16 March 2024"
                r'(?:published|posted|updated|date|on|by)\s*:?\s*(\d{4}[-/]\d{1,2}[-/]\d{1,2})',
                r'(?:published|posted|updated|date|on|by)\s*:?\s*(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
                # Direct date patterns without prefix
                r'\b([A-Za-z]+\s+\d{1,2},?\s+\d{4})\b',  # "Nov 16 2025" or "November 16, 2025"
                r'\b(\d{1,2}\s+[A-Za-z]+\s+\d{4})\b',  # "16 March 2024"
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    parsed = self._parse_date(match.group(1))
                    if parsed:
                        return parsed
        
        # If no date found in structured format, try parsing any date in first 1000 chars
        # Look for date patterns in the beginning of the article
        first_part = text[:1000]
        # Try to find dates that look like publication dates (not random dates in content)
        date_candidates = re.findall(r'\b([A-Za-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4}[-/]\d{1,2}[-/]\d{1,2})\b', first_part, re.IGNORECASE)
        for candidate in date_candidates[:5]:  # Check first 5 candidates
            parsed = self._parse_date(candidate)
            if parsed:
                # Validate it's a reasonable date (not too far in future/past)
                try:
                    date_obj = datetime.strptime(parsed, '%Y-%m-%d')
                    if 2020 <= date_obj.year <= 2026:
                        return parsed
                except ValueError:
                    continue
        
        return None
    
    def fetch_all_article_contents(self) -> None:
        """Fetch full content for all articles."""
        logger.info(f"Fetching full content for {len(self.articles)} articles...")
        
        for i, article in enumerate(self.articles, 1):
            try:
                if i % 10 == 0:  # Log every 10th article
                    logger.info(f"Fetching article {i}/{len(self.articles)}...")
                article.full_content = self.fetch_article_content(article)
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"Error fetching content for article {i}: {e}")
                article.full_content = ""  # Set empty string on error
                continue
        
        logger.info("Finished fetching article contents")
    
    def analyze_articles(self) -> None:
        """Analyze articles to extract industries, countries, and attack methods."""
        logger.info("Analyzing articles for industries, countries, and attack methods...")
        
        for i, article in enumerate(self.articles, 1):
            try:
                # Use full content if available, but focus on main body only
                # Extract main body content (exclude footer/tags/links sections)
                if article.full_content:
                    # Split content and take first 80% (likely main body, excluding footer)
                    content_lines = article.full_content.split('\n')
                    # Estimate main body - usually first 70-80% of content
                    main_body_lines = content_lines[:int(len(content_lines) * 0.75)]
                    main_body = '\n'.join(main_body_lines)
                    # Remove lines that are mostly links or very short (likely tags/footers)
                    filtered_lines = []
                    for line in main_body.split('\n'):
                        line = line.strip()
                        if len(line) < 3:  # Skip very short lines
                            continue
                        # Skip lines that are mostly links (more than 30% of line is link-like)
                        if 'http' in line.lower() or line.count('/') > 3:
                            continue
                        filtered_lines.append(line)
                    main_body = ' '.join(filtered_lines)
                    text = f"{article.title} {main_body}".lower()
                else:
                    text = f"{article.title} {article.snippet}".lower()
                
                # Extract industries
                article.industries = self._extract_industries(text)
                
                # Extract countries and count mentions
                article.countries, article.country_mentions = self._extract_countries_with_counts(text)
                
                # Determine attack method
                article.attack_method = self._determine_attack_method(text)
            except Exception as e:
                logger.warning(f"Error analyzing article {i} ({article.title[:50]}...): {e}")
                # Ensure defaults are set
                if article.industries is None:
                    article.industries = []
                if article.countries is None:
                    article.countries = []
                if article.country_mentions is None:
                    article.country_mentions = {}
                continue
        
        logger.info(f"Analysis complete for {len(self.articles)} articles.")
    
    def _extract_industries(self, text: str) -> List[str]:
        """
        Extract affected industries from text.
        
        Args:
            text: Text to analyze.
            
        Returns:
            List of industry names found.
        """
        found_industries = []
        
        for industry, keywords in self.INDUSTRY_KEYWORDS.items():
            for keyword in keywords:
                if keyword.lower() in text:
                    found_industries.append(industry)
                    break
        
        return found_industries
    
    def _extract_countries(self, text: str) -> List[str]:
        """
        Extract mentioned countries from text using word boundaries.
        
        Args:
            text: Text to analyze.
            
        Returns:
            List of country names found.
        """
        found_countries = []
        text_lower = text.lower()
        
        # Sort countries by length (longest first) to avoid partial matches
        sorted_countries = sorted(self.COUNTRY_KEYWORDS, key=len, reverse=True)
        
        for country in sorted_countries:
            country_lower = country.lower()
            # Use word boundaries to avoid matching "russia" in "russian"
            # Match whole words or at word boundaries
            pattern = r'\b' + re.escape(country_lower) + r'\b'
            if re.search(pattern, text_lower):
                # Normalize country names
                normalized = self._normalize_country_name(country)
                if normalized not in found_countries:
                    found_countries.append(normalized)
        
        return found_countries
    
    def _extract_countries_with_counts(self, text: str) -> Tuple[List[str], Dict[str, int]]:
        """
        Extract mentioned countries from text and count all instances.
        
        Args:
            text: Text to analyze.
            
        Returns:
            Tuple of (list of unique country names, dict of country mention counts).
        """
        found_countries = []
        country_mentions = {}
        text_lower = text.lower()
        
        # Sort countries by length (longest first) to avoid partial matches
        sorted_countries = sorted(self.COUNTRY_KEYWORDS, key=len, reverse=True)
        
        for country in sorted_countries:
            country_lower = country.lower()
            # Use word boundaries to avoid matching "russia" in "russian"
            pattern = r'\b' + re.escape(country_lower) + r'\b'
            matches = re.findall(pattern, text_lower)
            
            if matches:
                # Normalize country names
                normalized = self._normalize_country_name(country)
                # Count all mentions
                count = len(matches)
                country_mentions[normalized] = country_mentions.get(normalized, 0) + count
                
                if normalized not in found_countries:
                    found_countries.append(normalized)
        
        return found_countries, country_mentions
    
    def _normalize_country_name(self, country: str) -> str:
        """
        Normalize country names to standard format.
        
        Args:
            country: Country name to normalize.
            
        Returns:
            Normalized country name.
        """
        mapping = {
            'uk': 'United Kingdom',
            'britain': 'United Kingdom',
            'gb': 'United Kingdom',
            'great britain': 'United Kingdom',
            'england': 'United Kingdom',
            'britain': 'United Kingdom',
            'ukraine': 'Ukraine',
            'poland': 'Poland',
            'germany': 'Germany',
            'france': 'France',
            'estonia': 'Estonia',
            'latvia': 'Latvia',
            'lithuania': 'Lithuania',
            'czech': 'Czech Republic',
            'czechia': 'Czech Republic',
            'czech republic': 'Czech Republic',
            'slovakia': 'Slovakia',
            'romania': 'Romania',
            'bulgaria': 'Bulgaria',
            'finland': 'Finland',
            'sweden': 'Sweden',
            'norway': 'Norway',
            'denmark': 'Denmark',
            'netherlands': 'Netherlands',
            'belgium': 'Belgium',
            'spain': 'Spain',
            'italy': 'Italy',
            'greece': 'Greece',
            'portugal': 'Portugal',
            'moldova': 'Moldova',
            'luxembourg': 'Luxembourg',
            'hungary': 'Hungary'
        }
        
        country_lower = country.lower()
        for key, value in mapping.items():
            if key == country_lower or key in country_lower:
                return value
        
        return country.title()
    
    def _determine_attack_method(self, text: str) -> str:
        """
        Determine if attack was direct or by proxy.
        
        Args:
            text: Text to analyze.
            
        Returns:
            'direct', 'proxy', or 'unknown'.
        """
        text_lower = text.lower()
        
        direct_score = sum(1 for keyword in self.SECURITY_SERVICES['direct'] if keyword in text_lower)
        proxy_score = sum(1 for keyword in self.SECURITY_SERVICES['proxy'] if keyword in text_lower)
        
        if direct_score > proxy_score:
            return 'direct'
        elif proxy_score > direct_score:
            return 'proxy'
        else:
            return 'unknown'
    
    def save_results(self, filename: str = "results.json") -> None:
        """
        Save results to JSON file.
        
        Args:
            filename: Output filename.
        """
        data = {
            'articles': [asdict(article) for article in self.articles],
            'total_count': len(self.articles),
            'timestamp': datetime.now().isoformat()
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Results saved to {filename}")
    
    def load_results(self, filename: str = "results.json") -> None:
        """
        Load results from JSON file.
        
        Args:
            filename: Input filename.
        """
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        self.articles = [Article(**article) for article in data['articles']]
        logger.info(f"Loaded {len(self.articles)} articles from {filename}")
    
    def export_to_csv(self, filename: str = "results.csv") -> None:
        """
        Export results to CSV file with all analysis data and article links.
        
        Args:
            filename: Output CSV filename.
        """
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow([
                'Title',
                'URL',
                'Date',
                'Snippet',
                'Industries',
                'Countries',
                'Country Mentions (Counts)',
                'Attack Method'
            ])
            
            # Write data rows
            for article in self.articles:
                # Format country mentions as "Country: Count, Country: Count"
                country_mentions_str = ', '.join([
                    f"{country}: {count}" 
                    for country, count in article.country_mentions.items()
                ]) if article.country_mentions else ''
                
                writer.writerow([
                    article.title,
                    article.url,
                    article.date or '',
                    article.snippet,
                    ', '.join(article.industries) if article.industries else '',
                    ', '.join(article.countries) if article.countries else '',
                    country_mentions_str,
                    article.attack_method or 'unknown'
                ])
        
        logger.info(f"Results exported to {filename}")
    
    def export_unknown_articles(self, filename: str = "unknown_articles.txt") -> None:
        """
        Export URLs of articles with unknown/other attack methods (excluding direct and proxy).
        
        Args:
            filename: Output text filename.
        """
        unknown_articles = []
        
        for article in self.articles:
            method = (article.attack_method or 'unknown').lower()
            # Include only articles that are not 'direct' or 'proxy'
            if method not in ['direct', 'proxy']:
                unknown_articles.append(article.url)
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("Articles with Unknown/Other Attack Methods\n")
            f.write("=" * 60 + "\n")
            f.write(f"Total: {len(unknown_articles)} articles\n")
            f.write("=" * 60 + "\n\n")
            
            for i, url in enumerate(unknown_articles, 1):
                f.write(f"{i}. {url}\n")
        
        logger.info(f"Exported {len(unknown_articles)} unknown/other articles to {filename}")


class DataVisualizer:
    """Handles data visualization using matplotlib."""
    
    def __init__(self, articles: List[Article]):
        """
        Initialize visualizer with articles.
        
        Args:
            articles: List of Article objects to visualize.
        """
        self.articles = articles
    
    def create_all_visualizations(self, output_dir: str = "visualizations") -> None:
        """
        Create all visualizations and save them.
        
        Args:
            output_dir: Directory to save visualization files.
        """
        Path(output_dir).mkdir(exist_ok=True)
        
        logger.info("Creating visualizations...")
        
        self.plot_timeline_by_month(output_dir)
        self.plot_timeline_by_year(output_dir)
        self.plot_industries_affected(output_dir)
        self.plot_industry_keywords_incidence(output_dir)
        self.plot_countries_affected(output_dir)
        self.plot_attack_methods(output_dir)
        self.plot_combined_analysis(output_dir)
        
        logger.info(f"All visualizations saved to {output_dir}/")
    
    def plot_timeline_by_month(self, output_dir: str) -> None:
        """
        Plot article count by month.
        
        Args:
            output_dir: Output directory.
        """
        # Group articles by month
        monthly_counts = defaultdict(int)
        
        for article in self.articles:
            if article.date:
                try:
                    date_obj = datetime.strptime(article.date, '%Y-%m-%d')
                    month_key = date_obj.strftime('%Y-%m')
                    monthly_counts[month_key] += 1
                except ValueError:
                    continue
        
        if not monthly_counts:
            logger.warning("No date information available for timeline visualization.")
            return
        
        # Sort by date
        sorted_months = sorted(monthly_counts.items())
        months = [datetime.strptime(m, '%Y-%m') for m, _ in sorted_months]
        counts = [c for _, c in sorted_months]
        
        fig, ax = plt.subplots(figsize=(14, 6))
        
        if months and counts:
            ax.plot(months, counts, marker='o', linewidth=2, markersize=6)
            ax.fill_between(months, counts, alpha=0.3)
            
            # Add date labels on data points
            for i, (month, count) in enumerate(zip(months, counts)):
                if count > 0:  # Only label non-zero points
                    ax.annotate(f'{count}', (month, count), 
                               textcoords="offset points", xytext=(0,10), 
                               ha='center', fontsize=8, alpha=0.7)
        
        # Set X-axis limits to 2020-2026
        ax.set_xlim([datetime(2020, 1, 1), datetime(2026, 12, 31)])
        
        ax.set_xlabel('Month (Date from Google Index)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Number of Articles', fontsize=12, fontweight='bold')
        ax.set_title('Russian Sabotage Articles by Month (2020-2026)\nDates from Google Search Results', 
                    fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3)
        
        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        plt.xticks(rotation=45)
        
        # Add date range info
        if months:
            date_range = f"Date Range: {months[0].strftime('%Y-%m')} to {months[-1].strftime('%Y-%m')}"
            ax.text(0.02, 0.98, date_range, transform=ax.transAxes, 
                   fontsize=9, verticalalignment='top', 
                   bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        else:
            # Show message if no data
            ax.text(0.5, 0.5, 'No date data available', 
                   transform=ax.transAxes, ha='center', va='center',
                   fontsize=12, style='italic')
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/timeline_by_month.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info("Timeline by month saved")
    
    def plot_timeline_by_year(self, output_dir: str) -> None:
        """
        Plot article count by year.
        
        Args:
            output_dir: Output directory.
        """
        yearly_counts = defaultdict(int)
        
        for article in self.articles:
            if article.date:
                try:
                    date_obj = datetime.strptime(article.date, '%Y-%m-%d')
                    year = date_obj.year
                    yearly_counts[year] += 1
                except ValueError:
                    continue
        
        # Ensure we have data for all years 2020-2026, even if count is 0
        all_years = list(range(2020, 2027))
        counts = [yearly_counts.get(year, 0) for year in all_years]
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        if any(counts):  # Only plot if there's data
            bars = ax.bar(all_years, counts, color='steelblue', alpha=0.7, edgecolor='black', linewidth=1.5)
            
            # Add value labels on bars with dates
            for bar, year, count in zip(bars, all_years, counts):
                if count > 0:  # Only label non-zero bars
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height,
                           f'{int(height)}',
                           ha='center', va='bottom', fontweight='bold')
        else:
            # Show message if no data
            ax.text(0.5, 0.5, 'No date data available', 
                   transform=ax.transAxes, ha='center', va='center',
                   fontsize=12, style='italic')
        
        # Set X-axis limits and ticks to 2020-2026
        ax.set_xlim([2019.5, 2026.5])
        ax.set_xticks(all_years)
        ax.set_xlabel('Year (Date from Google Index)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Number of Articles', fontsize=12, fontweight='bold')
        ax.set_title('Russian Sabotage Articles by Year (2020-2026)\nDates from Google Search Results', 
                    fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add date range info
        date_range = f"Date Range: 2020 to 2026"
        ax.text(0.02, 0.98, date_range, transform=ax.transAxes, 
               fontsize=9, verticalalignment='top', 
               bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/timeline_by_year.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info("Timeline by year saved")
    
    def plot_industries_affected(self, output_dir: str) -> None:
        """
        Plot industries affected by attacks.
        
        Args:
            output_dir: Output directory.
        """
        industry_counts = Counter()
        
        for article in self.articles:
            for industry in article.industries:
                industry_counts[industry] += 1
        
        if not industry_counts:
            logger.warning("No industry data available")
            return
        
        industries = list(industry_counts.keys())
        counts = list(industry_counts.values())
        
        # Sort by count
        sorted_data = sorted(zip(industries, counts), key=lambda x: x[1], reverse=True)
        industries, counts = zip(*sorted_data)
        
        fig, ax = plt.subplots(figsize=(12, 8))
        bars = ax.barh(industries, counts, color='crimson', alpha=0.7, edgecolor='black', linewidth=1.5)
        
        # Add value labels
        for i, (bar, count) in enumerate(zip(bars, counts)):
            ax.text(count, bar.get_y() + bar.get_height()/2.,
                   f' {count}',
                   ha='left', va='center', fontweight='bold')
        
        ax.set_xlabel('Number of Articles', fontsize=12, fontweight='bold')
        ax.set_ylabel('Industry', fontsize=12, fontweight='bold')
        ax.set_title('Industries Affected by Russian Sabotage', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='x')
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/industries_affected.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info("Industries affected saved")
    
    def plot_industry_keywords_incidence(self, output_dir: str) -> None:
        """
        Plot incidence of individual keywords from INDUSTRY_KEYWORDS list.
        
        Args:
            output_dir: Output directory.
        """
        # Define industry keywords (same as in GoogleScraper)
        INDUSTRY_KEYWORDS = {
            'energy': [
                'power grid', 'electricity transmission', 'high voltage substation', 'transformer',
                'switchgear', 'grid operator', 'power plant', 'gas-fired plant', 'substation',
                'nuclear plant', 'hydropower dam', 'wind farm', 'solar farm',
                'gas pipeline', 'compressor station', 'LNG terminal', 'oil refinery',
                'fuel depot', 'storage tank', 'pumping station', 'heating',
                'blackout', 'grid failure', 'SCADA', 'ICS', 'power outage'
            ],
            'transportation': [
                'railway', 'rail line', 'freight train',
                'rail yard', 'marshalling yard',
                'rail signalling', 'signal box',
                'switch points', 'interlocking',
                'derailment', 'track sabotage',
                'bridge', 'tunnel', 'port', 'pier', 'terminal',
                'container terminal',
                'logistics hub', 'distribution hub',
                'fuel pipeline', 'airport',
                'runway', 'air traffic control',
                'fire at depot', 'explosion',
            ],
            'telecommunications': [
                'telecom exchange', 'telephone exchange',
                'mobile network', 'cell tower',
                'base station', 'core network',
                'network operations centre', 'data centre',
                'fiber-optic cable', 'fibre-optic cable',
                'undersea cable', 'subsea cable',
                'cable landing station', 'backbone network',
                'microwave link', 'satellite link',
                'routing outage', 'BGP hijack',
                'cable cut', 'network disruption'
            ],
            'finance': [
                'bank', 'central bank', 'payment system',
                'SWIFT', 'SEPA', 'clearing house',
                'ATM network', 'cash-in-transit',
                'bank branch', 'vault',
                'financial regulator', 'sanctions enforcement',
                'sanctions evasion', 'money laundering',
                'financial cyberattack', 'DDoS extortion',
                'data breach', 'ransom payment',
                'crypto exchange', 'illicit finance',
                'state-sponsored attack', 'insider threat'
            ],
            'healthcare': [
                'hospital', 'medical centre', 'emergency department',
                'ambulance service', 'medical supply depot',
                'pharmaceutical plant', 'vaccine facility',
                'laboratory', 'pathology lab', 'biomedical facility',
                'oxygen supply', 'medical gas', 'power outage hospital',
                'backup generator failure', 'hospital ransomware',
                'health data breach', 'medical logistics',
                'cold chain disruption', 'water contamination',
                'fire evacuation', 'security incident'
            ],
            'defense': [
                'military base', 'airbase', 'naval base', 'barracks',
                'munitions depot', 'ammo depot', 'weapons storage',
                'fuel depot', 'jet fuel', 'military logistics',
                'weapons shipment', 'military convoy', 'rail transport military',
                'radar site', 'air defence', 'missile system',
                'drone', 'UAV', 'military aircraft', 'arms factory',
                'restricted area', 'secure facility', 'perimeter breach',
                'explosive device', 'military attack',
                'covert reconnaissance', 'military sabotage'
            ],
            'cybersecurity': [
                'cyber sabotage', 'wiper malware', 'ransomware', 'DDoS',
                'network intrusion', 'unauthorized access', 'supply chain attack',
                'OT compromise', 'ICS compromise', 'SCADA breach', 'industrial control system',
                'PLC manipulation', 'remote access trojan', 'command and control',
                'APT', 'state-sponsored', 'cyber espionage', 'cyber disruption',
                'satellite communications attack', 'router compromise',
                'telecom network intrusion', 'energy sector cyberattack',
                'transport sector cyberattack', 'data wiper', 'system outage',
                'incident response', 'attribution'
            ],
            'manufacturing': [
                'industrial plant', 'factory', 'production facility', 'industrial site',
                'chemical plant', 'petrochemical', 'fertilizer plant', 'explosives plant',
                'ammunition factory', 'drone factory', 'aerospace plant',
                'shipyard', 'railcar factory', 'machinery plant', 'transformer factory',
                'cable factory', 'electronics plant', 'warehouse', 'logistics depot',
                'distribution centre', 'industrial fire',
                'production halt', 'supply chain disruption'
            ],
            'government': [
                'government building', 'ministry', 'defence ministry', 'interior ministry',
                'foreign ministry', 'state agency', 'regulatory authority', 'municipal building',
                'city hall', 'prefecture', 'embassy', 'consulate', 'diplomatic mission',
                'border police', 'border guard', 'customs service', 'national police',
                'gendarmerie', 'security service', 'intelligence service', 'counterintelligence',
                'civil protection', 'emergency management', 'critical infrastructure authority',
                'classified facility', 'secure compound', 'perimeter breach'
            ],
        }
        
        # Count keyword occurrences across all articles
        keyword_counts = Counter()
        
        for article in self.articles:
            # Combine title, snippet, and full content for keyword search
            text = ""
            if article.full_content:
                # Use main body only (first 75% to exclude footer/tags)
                content_lines = article.full_content.split('\n')
                main_body_lines = content_lines[:int(len(content_lines) * 0.75)]
                main_body = '\n'.join(main_body_lines)
                text = f"{article.title} {article.snippet} {main_body}".lower()
            else:
                text = f"{article.title} {article.snippet}".lower()
            
            # Count occurrences of each keyword
            for industry, keywords in INDUSTRY_KEYWORDS.items():
                for keyword in keywords:
                    # Count occurrences using word boundaries for better accuracy
                    pattern = r'\b' + re.escape(keyword.lower()) + r'\b'
                    matches = len(re.findall(pattern, text))
                    if matches > 0:
                        keyword_counts[keyword] += matches
        
        if not keyword_counts:
            logger.warning("No industry keyword data available")
            return
        
        # Get top 30 keywords by incidence
        top_keywords = keyword_counts.most_common(30)
        keywords, counts = zip(*top_keywords)
        
        fig, ax = plt.subplots(figsize=(14, 10))
        bars = ax.barh(keywords, counts, color='darkorange', alpha=0.7, edgecolor='black', linewidth=1.5)
        
        # Add value labels
        for bar, count in zip(bars, counts):
            ax.text(count, bar.get_y() + bar.get_height()/2.,
                   f' {count}',
                   ha='left', va='center', fontweight='bold')
        
        ax.set_xlabel('Number of Occurrences', fontsize=12, fontweight='bold')
        ax.set_ylabel('Industry Keyword', fontsize=12, fontweight='bold')
        ax.set_title('Incidence of Industry Keywords in Articles (Top 30)', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='x')
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/industry_keywords_incidence.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info("Industry keywords incidence saved")
    
    def plot_countries_affected(self, output_dir: str) -> None:
        """
        Plot countries mentioned in articles using mention counts.
        
        Args:
            output_dir: Output directory.
        """
        country_mention_counts = defaultdict(int)
        
        for article in self.articles:
            for country, count in article.country_mentions.items():
                country_mention_counts[country] += count
        
        if not country_mention_counts:
            logger.warning("No country data available")
            return
        
        # Get top 15 countries by mention count
        top_countries = sorted(country_mention_counts.items(), key=lambda x: x[1], reverse=True)[:15]
        countries, counts = zip(*top_countries)
        
        fig, ax = plt.subplots(figsize=(12, 8))
        bars = ax.barh(countries, counts, color='darkgreen', alpha=0.7, edgecolor='black', linewidth=1.5)
        
        # Add value labels
        for bar, count in zip(bars, counts):
            ax.text(count, bar.get_y() + bar.get_height()/2.,
                   f' {count}',
                   ha='left', va='center', fontweight='bold')
        
        ax.set_xlabel('Number of Articles', fontsize=12, fontweight='bold')
        ax.set_ylabel('Country', fontsize=12, fontweight='bold')
        ax.set_title('Countries Mentioned in Russian Sabotage Articles (Top 15)', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='x')
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/countries_affected.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info("Countries affected saved")
    
    def plot_attack_methods(self, output_dir: str) -> None:
        """
        Plot distribution of attack methods (direct vs proxy).
        
        Args:
            output_dir: Output directory.
        """
        method_counts = Counter()
        
        for article in self.articles:
            method_counts[article.attack_method] += 1
        
        methods = list(method_counts.keys())
        counts = list(method_counts.values())
        
        colors = {'direct': 'red', 'proxy': 'orange', 'unknown': 'gray'}
        method_colors = [colors.get(m, 'blue') for m in methods]
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Pie chart
        ax1.pie(counts, labels=methods, autopct='%1.1f%%', startangle=90, colors=method_colors)
        ax1.set_title('Attack Method Distribution', fontsize=12, fontweight='bold')
        
        # Bar chart
        bars = ax2.bar(methods, counts, color=method_colors, alpha=0.7, edgecolor='black', linewidth=1.5)
        for bar, count in zip(bars, counts):
            ax2.text(bar.get_x() + bar.get_width()/2., count,
                    f'{count}',
                    ha='center', va='bottom', fontweight='bold')
        
        ax2.set_xlabel('Attack Method', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Number of Articles', fontsize=12, fontweight='bold')
        ax2.set_title('Attack Method Counts', fontsize=12, fontweight='bold')
        ax2.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/attack_methods.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info("Attack methods saved")
    
    def plot_combined_analysis(self, output_dir: str) -> None:
        """
        Create a comprehensive combined visualization.
        
        Args:
            output_dir: Output directory.
        """
        fig = plt.figure(figsize=(16, 10))
        gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)
        
        # 1. Timeline by year
        ax1 = fig.add_subplot(gs[0, :])
        yearly_counts = defaultdict(int)
        for article in self.articles:
            if article.date:
                try:
                    year = datetime.strptime(article.date, '%Y-%m-%d').year
                    yearly_counts[year] += 1
                except ValueError:
                    continue
        
        # Ensure we have data for all years 2020-2026
        all_years = list(range(2020, 2027))
        counts = [yearly_counts.get(year, 0) for year in all_years]
        
        if any(counts):  # Only plot if there's data
            bars = ax1.bar(all_years, counts, color='steelblue', alpha=0.7, edgecolor='black')
            # Add value labels
            for bar, year, count in zip(bars, all_years, counts):
                if count > 0:
                    ax1.text(bar.get_x() + bar.get_width()/2., count,
                            f'{count}', ha='center', va='bottom', fontsize=8)
        else:
            ax1.text(0.5, 0.5, 'No date data available', 
                    transform=ax1.transAxes, ha='center', va='center',
                    fontsize=10, style='italic')
        
        ax1.set_xlim([2019.5, 2026.5])
        ax1.set_xticks(all_years)
        ax1.set_xlabel('Year (Date from Google Index)', fontweight='bold')
        ax1.set_ylabel('Articles', fontweight='bold')
        ax1.set_title('Articles by Year (Dates from Google Search Results)', fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='y')
        # Add date range
        ax1.text(0.02, 0.98, f"Range: 2020-2026", 
                transform=ax1.transAxes, fontsize=8, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        # 2. Top industries
        ax2 = fig.add_subplot(gs[1, 0])
        industry_counts = Counter()
        for article in self.articles:
            for industry in article.industries:
                industry_counts[industry] += 1
        
        if industry_counts:
            top_industries = industry_counts.most_common(8)
            industries, counts = zip(*top_industries)
            ax2.barh(industries, counts, color='crimson', alpha=0.7)
            ax2.set_xlabel('Articles', fontweight='bold')
            ax2.set_title('Top Industries', fontweight='bold')
            ax2.grid(True, alpha=0.3, axis='x')
        
        # 3. Top countries
        ax3 = fig.add_subplot(gs[1, 1])
        country_counts = Counter()
        for article in self.articles:
            for country in article.countries:
                country_counts[country] += 1
        
        if country_counts:
            top_countries = country_counts.most_common(8)
            countries, counts = zip(*top_countries)
            ax3.barh(countries, counts, color='darkgreen', alpha=0.7)
            ax3.set_xlabel('Articles', fontweight='bold')
            ax3.set_title('Top Countries', fontweight='bold')
            ax3.grid(True, alpha=0.3, axis='x')
        
        # 4. Attack methods
        ax4 = fig.add_subplot(gs[2, 0])
        method_counts = Counter(article.attack_method for article in self.articles)
        methods = list(method_counts.keys())
        counts = list(method_counts.values())
        colors = {'direct': 'red', 'proxy': 'orange', 'unknown': 'gray'}
        method_colors = [colors.get(m, 'blue') for m in methods]
        ax4.pie(counts, labels=methods, autopct='%1.1f%%', colors=method_colors, startangle=90)
        ax4.set_title('Attack Methods', fontweight='bold')
        
        # 5. Summary statistics
        ax5 = fig.add_subplot(gs[2, 1])
        ax5.axis('off')
        stats_text = f"""
        Summary Statistics
        
        Total Articles: {len(self.articles)}
        Articles with Dates: {sum(1 for a in self.articles if a.date)}
        Unique Industries: {len(set(i for a in self.articles for i in a.industries))}
        Unique Countries: {len(set(c for a in self.articles for c in a.countries))}
        
        Attack Methods:
        Direct: {method_counts.get('direct', 0)}
        Proxy: {method_counts.get('proxy', 0)}
        Unknown: {method_counts.get('unknown', 0)}
        """
        ax5.text(0.1, 0.5, stats_text, fontsize=11, verticalalignment='center',
                family='monospace', fontweight='bold')
        
        fig.suptitle('Russian Sabotage Analysis Dashboard (2020-2026)', 
                    fontsize=16, fontweight='bold', y=0.98)
        
        plt.savefig(f"{output_dir}/combined_analysis.png", dpi=300, bbox_inches='tight')
        plt.close()
        logger.info("Combined analysis saved")
    
    def export_visualization_data_to_csv(self, filename: str = "visualization_data.csv") -> None:
        """
        Export visualization data to CSV with article links for each category.
        
        Args:
            filename: Output CSV filename.
        """
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow([
                'Category',
                'Value',
                'Count',
                'Article Titles',
                'Article URLs'
            ])
            
            # Export industries data - use dict with URLs as keys to ensure each article is only added once
            industry_articles = defaultdict(dict)  # dict[industry][url] = article
            for article in self.articles:
                for industry in article.industries:
                    industry_articles[industry][article.url] = article
            
            for industry, articles_dict in sorted(industry_articles.items(), key=lambda x: len(x[1]), reverse=True):
                articles_list = list(articles_dict.values())
                titles = ' | '.join([a.title for a in articles_list])
                urls = ' | '.join([a.url for a in articles_list])
                writer.writerow([
                    'Industry',
                    industry,
                    len(articles_list),
                    titles,
                    urls
                ])
            
            # Export countries data with mention counts
            country_articles = defaultdict(dict)
            country_total_mentions = defaultdict(int)
            
            for article in self.articles:
                for country in article.countries:
                    country_articles[country][article.url] = article
                    if country in article.country_mentions:
                        country_total_mentions[country] += article.country_mentions[country]
            
            for country, articles_dict in sorted(country_articles.items(), 
                                                  key=lambda x: country_total_mentions.get(x[0], 0), 
                                                  reverse=True):
                articles_list = list(articles_dict.values())
                titles = ' | '.join([a.title for a in articles_list])
                urls = ' | '.join([a.url for a in articles_list])
                total_mentions = country_total_mentions.get(country, 0)
                writer.writerow([
                    'Country',
                    country,
                    f"{total_mentions} mentions in {len(articles_list)} articles",
                    titles,
                    urls
                ])
            
            # Export attack methods data - use dict with URLs to ensure each article is only added once
            method_articles = defaultdict(dict)  # dict[method][url] = article
            for article in self.articles:
                method = article.attack_method or 'unknown'
                method_articles[method][article.url] = article
            
            for method, articles_dict in sorted(method_articles.items(), key=lambda x: len(x[1]), reverse=True):
                articles_list = list(articles_dict.values())
                titles = ' | '.join([a.title for a in articles_list])
                urls = ' | '.join([a.url for a in articles_list])
                writer.writerow([
                    'Attack Method',
                    method,
                    len(articles_list),
                    titles,
                    urls
                ])
            
            # Export timeline data (by year) - use dict with URLs to ensure each article is only added once
            yearly_articles = defaultdict(dict)  # dict[year][url] = article
            for article in self.articles:
                if article.date:
                    try:
                        year = datetime.strptime(article.date, '%Y-%m-%d').year
                        yearly_articles[year][article.url] = article
                    except ValueError:
                        continue
            
            for year, articles_dict in sorted(yearly_articles.items()):
                articles_list = list(articles_dict.values())
                titles = ' | '.join([a.title for a in articles_list])
                urls = ' | '.join([a.url for a in articles_list])
                writer.writerow([
                    'Year',
                    str(year),
                    len(articles_list),
                    titles,
                    urls
                ])
            
            # Export timeline data (by month) - use dict with URLs to ensure each article is only added once
            monthly_articles = defaultdict(dict)  # dict[month][url] = article
            for article in self.articles:
                if article.date:
                    try:
                        date_obj = datetime.strptime(article.date, '%Y-%m-%d')
                        month_key = date_obj.strftime('%Y-%m')
                        monthly_articles[month_key][article.url] = article
                    except ValueError:
                        continue
            
            for month, articles_dict in sorted(monthly_articles.items()):
                articles_list = list(articles_dict.values())
                titles = ' | '.join([a.title for a in articles_list])
                urls = ' | '.join([a.url for a in articles_list])
                writer.writerow([
                    'Month',
                    month,
                    len(articles_list),
                    titles,
                    urls
                ])
        
        logger.info(f"Visualization data exported to {filename}")


def main():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("Russian Sabotage Analysis Tool")
    logger.info("=" * 60)
    
    # Configuration
    query = "Russian Sabotage -YouTube -Vimeo -Instagram -TikTok"
    start_date = "2020-01-01"
    end_date = "2026-12-31"
    max_results = 1000
    headless = False  # Set to True to run without browser window
    
    # Scrape Google results
    with GoogleScraper(headless=headless) as scraper:
        articles = scraper.search_google(query, start_date, end_date, max_results)
        
        if not articles:
            logger.warning("No articles found. Exiting.")
            return
        
        # Fetch full content for all articles
        scraper.fetch_all_article_contents()
        
        # Analyze articles (now using full content)
        scraper.analyze_articles()
        
        # Save results
        scraper.save_results("results.json")
        
        # Export to CSV
        scraper.export_to_csv("results.csv")
        
        # Export unknown/other articles to text file
        scraper.export_unknown_articles("unknown_articles.txt")
        
        # Create visualizations
        visualizer = DataVisualizer(scraper.articles)
        visualizer.create_all_visualizations()
        
        # Export visualization data to CSV
        visualizer.export_visualization_data_to_csv("visualization_data.csv")
    
    logger.info("=" * 60)
    logger.info("Analysis complete!")
    logger.info("=" * 60)
    logger.info(f"Total articles analyzed: {len(articles)}")
    logger.info("Check 'results.json' for detailed data")
    logger.info("Check 'results.csv' for article data with links")
    logger.info("Check 'unknown_articles.txt' for unknown/other attack method articles")
    logger.info("Check 'visualization_data.csv' for visualization data with article links")
    logger.info("Check 'visualizations/' folder for charts")


if __name__ == "__main__":
    main()

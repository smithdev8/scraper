"""
🕷️ UNIVERSAL WEB SCRAPER
Профессиональный инструмент для сбора данных с любых сайтов
"""

import asyncio
import aiohttp
from aiohttp import ClientSession, ClientTimeout
import json
import csv
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
import hashlib
from pathlib import Path
import re
from urllib.parse import urljoin, urlparse
import time
import random

# Парсинг
from bs4 import BeautifulSoup
from lxml import html, etree
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# API и веб-сервер
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Depends
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, HttpUrl
import uvicorn

# Утилиты
import pandas as pd
import numpy as np
from fake_useragent import UserAgent
import cloudscraper  # Для обхода Cloudflare
from playwright.async_api import async_playwright  # Альтернатива Selenium
import httpx
from rich.console import Console
from rich.table import Table
from rich.progress import track
import schedule
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Email уведомления
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Telegram уведомления
from aiogram import Bot
from aiogram.types import ParseMode

# Логирование
import logging
from loguru import logger

# Конфигурация
from dataclasses import dataclass
from dotenv import load_dotenv
import os

load_dotenv()

# ==================== КОНФИГУРАЦИЯ ====================
@dataclass
class Config:
    """Централизованная конфигурация"""
    
    # API настройки
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_KEY: str = os.getenv("API_KEY", "your-secret-api-key")
    
    # База данных
    DB_PATH: str = "scraper_data.db"
    
    # Proxy настройки (опционально)
    PROXY_LIST: List[str] = None
    USE_PROXY: bool = False
    
    # Браузер
    HEADLESS: bool = True
    CHROME_DRIVER_PATH: str = None  # Путь к chromedriver
    
    # Уведомления
    TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID")
    EMAIL_HOST: str = os.getenv("EMAIL_HOST", "smtp.gmail.com")
    EMAIL_PORT: int = 587
    EMAIL_USER: str = os.getenv("EMAIL_USER")
    EMAIL_PASSWORD: str = os.getenv("EMAIL_PASSWORD")
    
    # Лимиты
    MAX_CONCURRENT_REQUESTS: int = 10
    REQUEST_TIMEOUT: int = 30
    RETRY_ATTEMPTS: int = 3
    RATE_LIMIT_DELAY: float = 1.0  # Секунды между запросами
    
    # Пути
    DATA_DIR: Path = Path("scraped_data")
    CACHE_DIR: Path = Path("cache")
    EXPORTS_DIR: Path = Path("exports")
    
    def __post_init__(self):
        """Создаем необходимые директории"""
        for dir_path in [self.DATA_DIR, self.CACHE_DIR, self.EXPORTS_DIR]:
            dir_path.mkdir(exist_ok=True)

config = Config()

# ==================== БАЗА ДАННЫХ ====================
class Database:
    """Менеджер базы данных для хранения результатов"""
    
    def __init__(self, db_path: str = config.DB_PATH):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.create_tables()
    
    def create_tables(self):
        """Создание таблиц"""
        cursor = self.conn.cursor()
        
        # Таблица задач
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT UNIQUE,
                url TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                error TEXT,
                data_count INTEGER DEFAULT 0
            )
        ''')
        
        # Таблица результатов
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraped_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                url TEXT,
                data TEXT,
                data_hash TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (job_id) REFERENCES scraping_jobs(job_id)
            )
        ''')
        
        # Таблица мониторинга цен
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_monitoring (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_url TEXT,
                product_name TEXT,
                price REAL,
                currency TEXT,
                availability TEXT,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Таблица изменений
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS change_detection (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT,
                content_hash TEXT,
                changed BOOLEAN,
                diff TEXT,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.conn.commit()
    
    def save_job(self, job_id: str, url: str, status: str = "pending"):
        """Сохранить задачу"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO scraping_jobs (job_id, url, status)
            VALUES (?, ?, ?)
        ''', (job_id, url, status))
        self.conn.commit()
    
    def save_data(self, job_id: str, url: str, data: dict):
        """Сохранить результаты"""
        data_json = json.dumps(data, ensure_ascii=False)
        data_hash = hashlib.md5(data_json.encode()).hexdigest()
        
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO scraped_data (job_id, url, data, data_hash)
                VALUES (?, ?, ?, ?)
            ''', (job_id, url, data_json, data_hash))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Дубликат данных
            return False
    
    def get_job_status(self, job_id: str) -> dict:
        """Получить статус задачи"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT status, created_at, completed_at, error, data_count
            FROM scraping_jobs WHERE job_id = ?
        ''', (job_id,))
        
        result = cursor.fetchone()
        if result:
            return {
                "status": result[0],
                "created_at": result[1],
                "completed_at": result[2],
                "error": result[3],
                "data_count": result[4]
            }
        return None
    
    def get_scraped_data(self, job_id: str) -> list:
        """Получить данные по задаче"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT data, created_at FROM scraped_data 
            WHERE job_id = ? ORDER BY created_at DESC
        ''', (job_id,))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                "data": json.loads(row[0]),
                "scraped_at": row[1]
            })
        return results

# ==================== SCRAPER ENGINES ====================
class ScraperEngine:
    """Базовый класс для различных методов скрапинга"""
    
    def __init__(self):
        self.ua = UserAgent()
        self.session = None
        
    def get_headers(self) -> dict:
        """Генерация случайных заголовков"""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }

class RequestsScraper(ScraperEngine):
    """Простой скрапер на requests + BeautifulSoup"""
    
    async def scrape(self, url: str, selectors: dict = None) -> dict:
        """Скрапинг с помощью requests"""
        headers = self.get_headers()
        
        try:
            response = requests.get(url, headers=headers, timeout=config.REQUEST_TIMEOUT)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Если указаны селекторы, извлекаем по ним
            if selectors:
                data = {}
                for key, selector in selectors.items():
                    if selector.startswith('//'):  # XPath
                        tree = html.fromstring(response.content)
                        elements = tree.xpath(selector)
                        data[key] = [elem.text_content().strip() for elem in elements]
                    else:  # CSS selector
                        elements = soup.select(selector)
                        data[key] = [elem.get_text(strip=True) for elem in elements]
                return data
            
            # Иначе возвращаем весь HTML
            return {
                "url": url,
                "title": soup.title.string if soup.title else None,
                "html": str(soup)
            }
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            raise

class SeleniumScraper(ScraperEngine):
    """Скрапер с JavaScript рендерингом через Selenium"""
    
    def __init__(self):
        super().__init__()
        self.driver = None
    
    def setup_driver(self):
        """Настройка Chrome драйвера"""
        options = Options()
        
        if config.HEADLESS:
            options.add_argument('--headless')
        
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument(f'user-agent={self.ua.random}')
        
        # Блокировка изображений для ускорения
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(config.REQUEST_TIMEOUT)
    
    async def scrape(self, url: str, wait_for: str = None, actions: list = None) -> dict:
        """Скрапинг динамических сайтов"""
        if not self.driver:
            self.setup_driver()
        
        try:
            self.driver.get(url)
            
            # Ждем загрузки элемента
            if wait_for:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_for))
                )
            
            # Выполняем действия (клики, скролл и т.д.)
            if actions:
                for action in actions:
                    if action['type'] == 'click':
                        elem = self.driver.find_element(By.CSS_SELECTOR, action['selector'])
                        elem.click()
                    elif action['type'] == 'scroll':
                        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    elif action['type'] == 'wait':
                        time.sleep(action['seconds'])
            
            # Получаем данные
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            return {
                "url": url,
                "title": self.driver.title,
                "html": str(soup),
                "cookies": self.driver.get_cookies()
            }
            
        except Exception as e:
            logger.error(f"Selenium error for {url}: {e}")
            raise
        
    def close(self):
        """Закрыть браузер"""
        if self.driver:
            self.driver.quit()

class CloudflareScraper(ScraperEngine):
    """Скрапер для сайтов с Cloudflare защитой"""
    
    async def scrape(self, url: str) -> dict:
        """Обход Cloudflare"""
        scraper = cloudscraper.create_scraper()
        
        try:
            response = scraper.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            return {
                "url": url,
                "title": soup.title.string if soup.title else None,
                "html": str(soup),
                "cloudflare_bypassed": True
            }
            
        except Exception as e:
            logger.error(f"Cloudflare bypass failed for {url}: {e}")
            raise

class PlaywrightScraper(ScraperEngine):
    """Современный асинхронный скрапер на Playwright"""
    
    async def scrape(self, url: str, screenshot: bool = False) -> dict:
        """Скрапинг с Playwright"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=config.HEADLESS)
            page = await browser.new_page()
            
            # Установка user-agent
            await page.set_extra_http_headers(self.get_headers())
            
            try:
                await page.goto(url, wait_until='networkidle')
                
                # Скриншот если нужен
                screenshot_path = None
                if screenshot:
                    screenshot_path = config.DATA_DIR / f"{hashlib.md5(url.encode()).hexdigest()}.png"
                    await page.screenshot(path=str(screenshot_path))
                
                # Получаем контент
                content = await page.content()
                title = await page.title()
                
                # Можем выполнить JavaScript
                dimensions = await page.evaluate('''() => {
                    return {
                        width: document.documentElement.clientWidth,
                        height: document.documentElement.clientHeight,
                        deviceScaleFactor: window.devicePixelRatio,
                    }
                }''')
                
                await browser.close()
                
                return {
                    "url": url,
                    "title": title,
                    "html": content,
                    "screenshot": str(screenshot_path) if screenshot_path else None,
                    "viewport": dimensions
                }
                
            except Exception as e:
                await browser.close()
                logger.error(f"Playwright error for {url}: {e}")
                raise

# ==================== СПЕЦИАЛИЗИРОВАННЫЕ СКРАПЕРЫ ====================
class EcommerceScraper:
    """Специализированный скрапер для e-commerce"""
    
    def __init__(self):
        self.patterns = {
            'amazon': {
                'title': 'span#productTitle',
                'price': 'span.a-price-whole',
                'rating': 'span.a-icon-alt',
                'availability': 'div#availability span'
            },
            'ebay': {
                'title': 'h1.it-ttl',
                'price': 'span.notranslate',
                'bids': 'span.vi-acc-num-bid',
                'time_left': 'span.vi-tm-left'
            },
            'aliexpress': {
                'title': 'h1.product-title-text',
                'price': 'span.product-price-value',
                'orders': 'span.product-reviewer-sold',
                'rating': 'span.overview-rating-average'
            }
        }
    
    def detect_platform(self, url: str) -> str:
        """Определить платформу по URL"""
        domain = urlparse(url).netloc.lower()
        
        if 'amazon' in domain:
            return 'amazon'
        elif 'ebay' in domain:
            return 'ebay'
        elif 'aliexpress' in domain:
            return 'aliexpress'
        else:
            return 'unknown'
    
    async def scrape_product(self, url: str) -> dict:
        """Скрапинг товара"""
        platform = self.detect_platform(url)
        
        if platform == 'unknown':
            raise ValueError(f"Unknown e-commerce platform for URL: {url}")
        
        selectors = self.patterns[platform]
        scraper = RequestsScraper()
        
        try:
            data = await scraper.scrape(url, selectors)
            
            # Обработка данных
            product_data = {
                'platform': platform,
                'url': url,
                'scraped_at': datetime.now().isoformat()
            }
            
            for key, values in data.items():
                if values:
                    product_data[key] = values[0] if len(values) == 1 else values
            
            # Очистка и форматирование цены
            if 'price' in product_data:
                price_str = product_data['price']
                # Извлекаем числа из строки цены
                price_numbers = re.findall(r'[\d,]+\.?\d*', str(price_str))
                if price_numbers:
                    product_data['price_numeric'] = float(price_numbers[0].replace(',', ''))
            
            return product_data
            
        except Exception as e:
            logger.error(f"Error scraping product from {platform}: {e}")
            raise

class NewsScraper:
    """Скрапер новостных сайтов"""
    
    def __init__(self):
        self.patterns = {
            'general': {
                'title': ['h1', 'article h1', '.article-title'],
                'content': ['article', '.article-content', '.post-content'],
                'author': ['.author', '.by-author', '.article-author'],
                'date': ['time', '.publish-date', '.article-date'],
                'category': ['.category', '.article-category', '.post-category']
            }
        }
    
    async def scrape_article(self, url: str) -> dict:
        """Скрапинг новостной статьи"""
        scraper = RequestsScraper()
        
        try:
            response = requests.get(url, headers=scraper.get_headers())
            soup = BeautifulSoup(response.content, 'html.parser')
            
            article_data = {
                'url': url,
                'scraped_at': datetime.now().isoformat()
            }
            
            # Извлекаем мета-данные
            meta_tags = soup.find_all('meta')
            for tag in meta_tags:
                if tag.get('property') == 'og:title':
                    article_data['og_title'] = tag.get('content')
                elif tag.get('property') == 'og:description':
                    article_data['og_description'] = tag.get('content')
                elif tag.get('property') == 'article:author':
                    article_data['meta_author'] = tag.get('content')
                elif tag.get('property') == 'article:published_time':
                    article_data['published_time'] = tag.get('content')
            
            # Пытаемся найти основные элементы
            for field, selectors in self.patterns['general'].items():
                for selector in selectors:
                    element = soup.select_one(selector)
                    if element:
                        article_data[field] = element.get_text(strip=True)
                        break
            
            # Извлекаем все параграфы для полного контента
            paragraphs = soup.find_all('p')
            article_data['full_text'] = '\n\n'.join([p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 50])
            
            # Извлекаем изображения
            images = soup.find_all('img')
            article_data['images'] = [img.get('src') for img in images if img.get('src')]
            
            return article_data
            
        except Exception as e:
            logger.error(f"Error scraping news article: {e}")
            raise

# ==================== МОНИТОРИНГ И АЛЕРТЫ ====================
class PriceMonitor:
    """Мониторинг цен на товары"""
    
    def __init__(self, db: Database):
        self.db = db
        self.ecommerce_scraper = EcommerceScraper()
    
    async def check_price(self, url: str, target_price: float = None) -> dict:
        """Проверить цену товара"""
        product_data = await self.ecommerce_scraper.scrape_product(url)
        
        # Сохраняем в БД
        cursor = self.db.conn.cursor()
        cursor.execute('''
            INSERT INTO price_monitoring (product_url, product_name, price, currency, availability)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            url,
            product_data.get('title', 'Unknown'),
            product_data.get('price_numeric', 0),
            'USD',  # Можно улучшить детекцию валюты
            product_data.get('availability', 'Unknown')
        ))
        self.db.conn.commit()
        
        # Проверяем целевую цену
        alert = None
        if target_price and 'price_numeric' in product_data:
            current_price = product_data['price_numeric']
            if current_price <= target_price:
                alert = f"🎯 Price Alert! {product_data.get('title')} is now ${current_price} (target: ${target_price})"
        
        return {
            'product': product_data,
            'alert': alert
        }
    
    async def get_price_history(self, url: str, days: int = 30) -> list:
        """Получить историю цен"""
        cursor = self.db.conn.cursor()
        cursor.execute('''
            SELECT price, checked_at FROM price_monitoring
            WHERE product_url = ? 
            AND checked_at > datetime('now', '-{} days')
            ORDER BY checked_at DESC
        '''.format(days), (url,))
        
        return [{'price': row[0], 'date': row[1]} for row in cursor.fetchall()]

class ChangeDetector:
    """Отслеживание изменений на веб-страницах"""
    
    def __init__(self, db: Database):
        self.db = db
    
    async def check_changes(self, url: str, selector: str = None) -> dict:
        """Проверить изменения на странице"""
        scraper = RequestsScraper()
        
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Если указан селектор, проверяем только его
            if selector:
                element = soup.select_one(selector)
                content = str(element) if element else ""
            else:
                content = str(soup)
            
            # Хешируем контент
            content_hash = hashlib.md5(content.encode()).hexdigest()
            
            # Проверяем предыдущий хеш
            cursor = self.db.conn.cursor()
            cursor.execute('''
                SELECT content_hash FROM change_detection
                WHERE url = ?
                ORDER BY checked_at DESC
                LIMIT 1
            ''', (url,))
            
            previous = cursor.fetchone()
            changed = False
            diff = None
            
            if previous and previous[0] != content_hash:
                changed = True
                diff = f"Content changed. New hash: {content_hash}"
            
            # Сохраняем результат
            cursor.execute('''
                INSERT INTO change_detection (url, content_hash, changed, diff)
                VALUES (?, ?, ?, ?)
            ''', (url, content_hash, changed, diff))
            self.db.conn.commit()
            
            return {
                'url': url,
                'changed': changed,
                'content_hash': content_hash,
                'diff': diff,
                'checked_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error checking changes for {url}: {e}")
            raise

# ==================== NOTIFICATION SYSTEM ====================
class NotificationManager:
    """Менеджер уведомлений"""
    
    def __init__(self):
        self.telegram_bot = None
        if config.TELEGRAM_BOT_TOKEN:
            self.telegram_bot = Bot(token=config.TELEGRAM_BOT_TOKEN)
    
    async def send_telegram(self, message: str):
        """Отправить уведомление в Telegram"""
        if self.telegram_bot and config.TELEGRAM_CHAT_ID:
            try:
                await self.telegram_bot.send_message(
                    chat_id=config.TELEGRAM_CHAT_ID,
                    text=message,
                    parse_mode=ParseMode.HTML
                )
                logger.info(f"Telegram notification sent: {message[:50]}...")
            except Exception as e:
                logger.error(f"Failed to send Telegram notification: {e}")
    
    def send_email(self, subject: str, body: str, to_email: str):
        """Отправить email уведомление"""
        if not all([config.EMAIL_USER, config.EMAIL_PASSWORD]):
            logger.warning("Email credentials not configured")
            return
        
        try:
            msg = MIMEMultipart()
            msg['From'] = config.EMAIL_USER
            msg['To'] = to_email
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(config.EMAIL_HOST, config.EMAIL_PORT)
            server.starttls()
            server.login(config.EMAIL_USER, config.EMAIL_PASSWORD)
            
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email sent to {to_email}: {subject}")
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}")

# ==================== EXPORT MANAGER ====================
class ExportManager:
    """Менеджер экспорта данных"""
    
    def __init__(self, db: Database):
        self.db = db
    
    def export_to_csv(self, job_id: str) -> str:
        """Экспорт в CSV"""
        data = self.db.get_scraped_data(job_id)
        
        if not data:
            raise ValueError(f"No data found for job {job_id}")
        
        # Преобразуем в плоскую структуру
        rows = []
        for item in data:
            flat_data = self._flatten_dict(item['data'])
            flat_data['scraped_at'] = item['scraped_at']
            rows.append(flat_data)
        
        # Создаем DataFrame
        df = pd.DataFrame(rows)
        
        # Сохраняем в CSV
        filename = f"export_{job_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = config.EXPORTS_DIR / filename
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        
        logger.info(f"Data exported to CSV: {filepath}")
        return str(filepath)
    
    def export_to_json(self, job_id: str) -> str:
        """Экспорт в JSON"""
        data = self.db.get_scraped_data(job_id)
        
        filename = f"export_{job_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = config.EXPORTS_DIR / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Data exported to JSON: {filepath}")
        return str(filepath)
    
    def export_to_excel(self, job_id: str) -> str:
        """Экспорт в Excel с форматированием"""
        data = self.db.get_scraped_data(job_id)
        
        if not data:
            raise ValueError(f"No data found for job {job_id}")
        
        # Преобразуем в DataFrame
        rows = []
        for item in data:
            flat_data = self._flatten_dict(item['data'])
            flat_data['scraped_at'] = item['scraped_at']
            rows.append(flat_data)
        
        df = pd.DataFrame(rows)
        
        # Сохраняем в Excel с форматированием
        filename = f"export_{job_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        filepath = config.EXPORTS_DIR / filename
        
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Scraped Data', index=False)
            
            # Автоматическая ширина колонок
            worksheet = writer.sheets['Scraped Data']
            for column in df:
                column_width = max(df[column].astype(str).map(len).max(), len(column))
                col_idx = df.columns.get_loc(column)
                worksheet.column_dimensions[chr(65 + col_idx)].width = min(column_width + 2, 50)
        
        logger.info(f"Data exported to Excel: {filepath}")
        return str(filepath)
    
    def _flatten_dict(self, d: dict, parent_key: str = '', sep: str = '_') -> dict:
        """Преобразование вложенного словаря в плоский"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, ', '.join(map(str, v))))
            else:
                items.append((new_key, v))
        return dict(items)

# ==================== ORCHESTRATOR ====================
class ScraperOrchestrator:
    """Главный оркестратор всех скраперов"""
    
    def __init__(self):
        self.db = Database()
        self.export_manager = ExportManager(self.db)
        self.notification_manager = NotificationManager()
        self.price_monitor = PriceMonitor(self.db)
        self.change_detector = ChangeDetector(self.db)
        self.scheduler = AsyncIOScheduler()
        self.scheduler.start()
    
    async def scrape(self, url: str, method: str = 'auto', **kwargs) -> str:
        """Универсальный метод скрапинга"""
        job_id = hashlib.md5(f"{url}{datetime.now()}".encode()).hexdigest()[:10]
        
        # Сохраняем задачу
        self.db.save_job(job_id, url, 'processing')
        
        try:
            # Выбираем метод скрапинга
            if method == 'auto':
                method = self._detect_best_method(url)
            
            scraper = self._get_scraper(method)
            
            # Скрапим данные
            data = await scraper.scrape(url, **kwargs)
            
            # Сохраняем результаты
            self.db.save_data(job_id, url, data)
            
            # Обновляем статус
            cursor = self.db.conn.cursor()
            cursor.execute('''
                UPDATE scraping_jobs 
                SET status = 'completed', completed_at = CURRENT_TIMESTAMP,
                    data_count = (SELECT COUNT(*) FROM scraped_data WHERE job_id = ?)
                WHERE job_id = ?
            ''', (job_id, job_id))
            self.db.conn.commit()
            
            # Отправляем уведомление
            await self.notification_manager.send_telegram(
                f"✅ Scraping completed!\nJob ID: {job_id}\nURL: {url}"
            )
            
            return job_id
            
        except Exception as e:
            # Сохраняем ошибку
            cursor = self.db.conn.cursor()
            cursor.execute('''
                UPDATE scraping_jobs 
                SET status = 'failed', error = ?, completed_at = CURRENT_TIMESTAMP
                WHERE job_id = ?
            ''', (str(e), job_id))
            self.db.conn.commit()
            
            logger.error(f"Scraping failed for job {job_id}: {e}")
            raise
    
    def _detect_best_method(self, url: str) -> str:
        """Автоматическое определение лучшего метода"""
        domain = urlparse(url).netloc.lower()
        
        # Сайты с JavaScript
        js_sites = ['instagram', 'facebook', 'twitter', 'linkedin', 'youtube']
        if any(site in domain for site in js_sites):
            return 'selenium'
        
        # E-commerce
        ecommerce = ['amazon', 'ebay', 'aliexpress', 'shopify']
        if any(site in domain for site in ecommerce):
            return 'ecommerce'
        
        # Новостные сайты
        news = ['news', 'times', 'post', 'journal', 'daily']
        if any(site in domain for site in news):
            return 'news'
        
        # По умолчанию
        return 'requests'
    
    def _get_scraper(self, method: str):
        """Получить нужный скрапер"""
        scrapers = {
            'requests': RequestsScraper(),
            'selenium': SeleniumScraper(),
            'cloudflare': CloudflareScraper(),
            'playwright': PlaywrightScraper(),
            'ecommerce': EcommerceScraper(),
            'news': NewsScraper()
        }
        
        return scrapers.get(method, RequestsScraper())
    
    def schedule_scraping(self, url: str, interval_minutes: int, method: str = 'auto'):
        """Запланировать регулярный скрапинг"""
        job = self.scheduler.add_job(
            self.scrape,
            trigger=IntervalTrigger(minutes=interval_minutes),
            args=[url, method],
            id=f"scheduled_{hashlib.md5(url.encode()).hexdigest()[:10]}",
            replace_existing=True
        )
        
        logger.info(f"Scheduled scraping for {url} every {interval_minutes} minutes")
        return job.id

# ==================== FAST API ====================
app = FastAPI(title="Universal Web Scraper API", version="2.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Инициализация
orchestrator = ScraperOrchestrator()

# ==================== API MODELS ====================
class ScrapeRequest(BaseModel):
    url: HttpUrl
    method: str = Field(default="auto", description="Scraping method: auto, requests, selenium, etc.")
    selectors: Dict[str, str] = Field(default=None, description="CSS/XPath selectors")
    actions: List[dict] = Field(default=None, description="Actions for dynamic sites")
    screenshot: bool = Field(default=False, description="Take screenshot (playwright only)")

class ScheduleRequest(BaseModel):
    url: HttpUrl
    interval_minutes: int = Field(default=60, description="Check interval in minutes")
    method: str = Field(default="auto")

class MonitorRequest(BaseModel):
    url: HttpUrl
    target_price: float = Field(default=None, description="Target price for alerts")
    selector: str = Field(default=None, description="Element to monitor for changes")

class ExportRequest(BaseModel):
    job_id: str
    format: str = Field(default="json", description="Export format: json, csv, excel")

# ==================== API ENDPOINTS ====================
@app.get("/")
async def root():
    """API информация"""
    return {
        "name": "Universal Web Scraper API",
        "version": "2.0",
        "endpoints": {
            "POST /scrape": "Scrape a website",
            "GET /status/{job_id}": "Check job status",
            "GET /data/{job_id}": "Get scraped data",
            "POST /schedule": "Schedule regular scraping",
            "POST /monitor/price": "Monitor product price",
            "POST /monitor/changes": "Monitor page changes",
            "POST /export": "Export data",
            "GET /jobs": "List all jobs"
        }
    }

@app.post("/scrape")
async def scrape_endpoint(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """Запустить скрапинг"""
    try:
        # Запускаем в фоне
        job_id = await orchestrator.scrape(
            str(request.url),
            request.method,
            selectors=request.selectors,
            actions=request.actions,
            screenshot=request.screenshot
        )
        
        return {
            "status": "success",
            "job_id": job_id,
            "message": "Scraping started",
            "check_status": f"/status/{job_id}"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/status/{job_id}")
async def get_status(job_id: str):
    """Получить статус задачи"""
    status = orchestrator.db.get_job_status(job_id)
    
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return status

@app.get("/data/{job_id}")
async def get_data(job_id: str, limit: int = Query(default=100)):
    """Получить данные по задаче"""
    data = orchestrator.db.get_scraped_data(job_id)
    
    if not data:
        raise HTTPException(status_code=404, detail="No data found")
    
    return {
        "job_id": job_id,
        "count": len(data),
        "data": data[:limit]
    }

@app.post("/schedule")
async def schedule_scraping(request: ScheduleRequest):
    """Запланировать регулярный скрапинг"""
    job_id = orchestrator.schedule_scraping(
        str(request.url),
        request.interval_minutes,
        request.method
    )
    
    return {
        "status": "success",
        "scheduled_job_id": job_id,
        "interval_minutes": request.interval_minutes
    }

@app.post("/monitor/price")
async def monitor_price(request: MonitorRequest):
    """Мониторинг цены товара"""
    result = await orchestrator.price_monitor.check_price(
        str(request.url),
        request.target_price
    )
    
    return result

@app.post("/monitor/changes")
async def monitor_changes(request: MonitorRequest):
    """Мониторинг изменений на странице"""
    result = await orchestrator.change_detector.check_changes(
        str(request.url),
        request.selector
    )
    
    return result

@app.post("/export")
async def export_data(request: ExportRequest):
    """Экспорт данных"""
    try:
        if request.format == "csv":
            filepath = orchestrator.export_manager.export_to_csv(request.job_id)
        elif request.format == "excel":
            filepath = orchestrator.export_manager.export_to_excel(request.job_id)
        else:
            filepath = orchestrator.export_manager.export_to_json(request.job_id)
        
        return FileResponse(
            filepath,
            media_type='application/octet-stream',
            filename=Path(filepath).name
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs")
async def list_jobs(limit: int = Query(default=50)):
    """Список всех задач"""
    cursor = orchestrator.db.conn.cursor()
    cursor.execute('''
        SELECT job_id, url, status, created_at, data_count
        FROM scraping_jobs
        ORDER BY created_at DESC
        LIMIT ?
    ''', (limit,))
    
    jobs = []
    for row in cursor.fetchall():
        jobs.append({
            "job_id": row[0],
            "url": row[1],
            "status": row[2],
            "created_at": row[3],
            "data_count": row[4]
        })
    
    return jobs

@app.get("/dashboard")
async def dashboard():
    """Веб-интерфейс дашборда"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Web Scraper Dashboard</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { 
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                padding: 2rem;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                border-radius: 1rem;
                padding: 2rem;
                box-shadow: 0 20px 25px -5px rgba(0,0,0,0.1);
            }
            h1 {
                color: #1f2937;
                margin-bottom: 2rem;
                text-align: center;
            }
            .stats {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 1rem;
                margin-bottom: 2rem;
            }
            .stat-card {
                background: #f9fafb;
                padding: 1.5rem;
                border-radius: 0.5rem;
                text-align: center;
            }
            .stat-value {
                font-size: 2rem;
                font-weight: bold;
                color: #667eea;
            }
            .stat-label {
                color: #6b7280;
                margin-top: 0.5rem;
            }
            .form-section {
                background: #f9fafb;
                padding: 1.5rem;
                border-radius: 0.5rem;
                margin-bottom: 2rem;
            }
            input, select, button {
                width: 100%;
                padding: 0.75rem;
                margin: 0.5rem 0;
                border: 1px solid #d1d5db;
                border-radius: 0.5rem;
            }
            button {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                border: none;
                cursor: pointer;
                font-weight: bold;
            }
            button:hover {
                opacity: 0.9;
            }
            #results {
                background: #f9fafb;
                padding: 1rem;
                border-radius: 0.5rem;
                margin-top: 1rem;
                max-height: 400px;
                overflow-y: auto;
            }
            .job-item {
                background: white;
                padding: 1rem;
                margin: 0.5rem 0;
                border-radius: 0.5rem;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            .status {
                padding: 0.25rem 0.75rem;
                border-radius: 9999px;
                font-size: 0.875rem;
                font-weight: bold;
            }
            .status.completed { background: #10b981; color: white; }
            .status.processing { background: #f59e0b; color: white; }
            .status.failed { background: #ef4444; color: white; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🕷️ Universal Web Scraper Dashboard</h1>
            
            <div class="stats" id="stats">
                <div class="stat-card">
                    <div class="stat-value">0</div>
                    <div class="stat-label">Total Jobs</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">0</div>
                    <div class="stat-label">Completed</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">0</div>
                    <div class="stat-label">Processing</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value">0</div>
                    <div class="stat-label">Failed</div>
                </div>
            </div>
            
            <div class="form-section">
                <h2>Start New Scraping Job</h2>
                <input type="url" id="url" placeholder="Enter URL to scrape">
                <select id="method">
                    <option value="auto">Auto Detect</option>
                    <option value="requests">Simple (Requests)</option>
                    <option value="selenium">Dynamic (Selenium)</option>
                    <option value="cloudflare">Cloudflare Bypass</option>
                    <option value="playwright">Modern (Playwright)</option>
                    <option value="ecommerce">E-commerce</option>
                    <option value="news">News Article</option>
                </select>
                <button onclick="startScraping()">Start Scraping</button>
            </div>
            
            <div class="form-section">
                <h2>Recent Jobs</h2>
                <div id="results"></div>
            </div>
        </div>
        
        <script>
            async function loadJobs() {
                const response = await fetch('/jobs');
                const jobs = await response.json();
                
                const resultsDiv = document.getElementById('results');
                resultsDiv.innerHTML = jobs.map(job => `
                    <div class="job-item">
                        <div>
                            <strong>${job.job_id}</strong><br>
                            <small>${job.url}</small>
                        </div>
                        <span class="status ${job.status}">${job.status}</span>
                    </div>
                `).join('');
                
                // Update stats
                const stats = {
                    total: jobs.length,
                    completed: jobs.filter(j => j.status === 'completed').length,
                    processing: jobs.filter(j => j.status === 'processing').length,
                    failed: jobs.filter(j => j.status === 'failed').length
                };
                
                document.querySelectorAll('.stat-value')[0].textContent = stats.total;
                document.querySelectorAll('.stat-value')[1].textContent = stats.completed;
                document.querySelectorAll('.stat-value')[2].textContent = stats.processing;
                document.querySelectorAll('.stat-value')[3].textContent = stats.failed;
            }
            
            async function startScraping() {
                const url = document.getElementById('url').value;
                const method = document.getElementById('method').value;
                
                if (!url) {
                    alert('Please enter a URL');
                    return;
                }
                
                const response = await fetch('/scrape', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({url, method})
                });
                
                const result = await response.json();
                alert(`Scraping started! Job ID: ${result.job_id}`);
                loadJobs();
            }
            
            // Load jobs on page load
            loadJobs();
            
            // Refresh every 5 seconds
            setInterval(loadJobs, 5000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# ==================== MAIN ====================
if __name__ == "__main__":
    console = Console()
    
    # Красивый баннер
    console.print("""
    [bold magenta]
    ╔══════════════════════════════════════════╗
    ║     🕷️  UNIVERSAL WEB SCRAPER v2.0  🕷️     ║
    ║         Professional Data Extraction      ║
    ╚══════════════════════════════════════════╝
    [/bold magenta]
    """)
    
    # Таблица с информацией
    table = Table(title="Scraper Features")
    table.add_column("Feature", style="cyan")
    table.add_column("Status", style="green")
    
    features = [
        ("Simple Scraping (BeautifulSoup)", "✅ Ready"),
        ("Dynamic Sites (Selenium)", "✅ Ready"),
        ("Cloudflare Bypass", "✅ Ready"),
        ("Modern Async (Playwright)", "✅ Ready"),
        ("E-commerce Specialized", "✅ Ready"),
        ("News Extraction", "✅ Ready"),
        ("Price Monitoring", "✅ Ready"),
        ("Change Detection", "✅ Ready"),
        ("API Server", "✅ Ready"),
        ("Web Dashboard", "✅ Ready"),
        ("Export (JSON/CSV/Excel)", "✅ Ready"),
        ("Telegram Notifications", "✅ Ready" if config.TELEGRAM_BOT_TOKEN else "⚠️  Not configured"),
        ("Email Alerts", "✅ Ready" if config.EMAIL_USER else "⚠️  Not configured")
    ]
    
    for feature, status in features:
        table.add_row(feature, status)
    
    console.print(table)
    
    # Запуск сервера
    console.print(f"\n[bold green]🚀 Starting API server on http://localhost:{config.API_PORT}[/bold green]")
    console.print(f"[bold blue]📊 Dashboard: http://localhost:{config.API_PORT}/dashboard[/bold blue]")
    console.print(f"[bold yellow]📚 API Docs: http://localhost:{config.API_PORT}/docs[/bold yellow]\n")
    
    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT)

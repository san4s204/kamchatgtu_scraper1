import asyncio
import sys

if sys.platform.startswith('win'):
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import aiohttp
import logging
import os
import uuid
import json
import time
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://kamchatgtu.ru/"
SITEMAP_URL = urljoin(BASE_URL, "sitemap.xml")

# Теги, из которых извлекаем текст внутри div#content
TARGET_TAGS = ['p', 'h1', 'h2', 'h3', 'div', 'li']

def extract_keywords(text, num=5):
    words = text.split()
    frequency = {}
    for word in words:
        w = word.lower().strip('.,!?()[]{}"\'')
        if w and len(w) > 3:
            frequency[w] = frequency.get(w, 0) + 1
    sorted_keywords = sorted(frequency.items(), key=lambda item: item[1], reverse=True)
    return [w for w, freq in sorted_keywords[:num]]

async def fetch(session, url, timeout=10):
    try:
        async with session.get(url, timeout=timeout) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        logger.error(f"Ошибка при загрузке {url}: {e}")
        return None

def get_full_url(relative_url):
    return urljoin(BASE_URL, relative_url)

processed_entries = set()

async def parse_page(session, url, results):
    logger.info(f"Обработка страницы: {url}")
    html = await fetch(session, url)
    if not html:
        return

    soup = BeautifulSoup(html, 'lxml')
    
    # Извлекаем основной контент из div#content
    content_div = soup.find('div', id='content')
    if not content_div:
        # Если контентного блока нет, выходим
        return

    entry = {
        "class": "general_motors", 
        "id": str(uuid.uuid4()), 
        "props": [],
        "url": url
    }

    content_blocks = content_div.find_all(TARGET_TAGS)
    for block in content_blocks:
        text = block.get_text(strip=True)
        if text:
            # Проверяем уникальность (url, text)
            unique_key = (url, text)
            if unique_key in processed_entries:
                continue
            processed_entries.add(unique_key)

            prop = {
                "title": block.name.upper(),
                "content": text,
                "keywords": extract_keywords(text)
            }
            entry["props"].append(prop)

    # Добавляем entry в results только если были добавлены пропсы
    if entry["props"]:
        results.append(entry)

async def get_all_urls_from_sitemap(session, sitemap_url):
    """Извлекает все конечные URL из sitemap или sitemap index."""
    logger.info(f"Загрузка карты сайта: {sitemap_url}")
    xml_data = await fetch(session, sitemap_url)
    if not xml_data:
        return []

    soup = BeautifulSoup(xml_data, 'xml')

    sitemap_index = soup.find('sitemapindex')
    urlset = soup.find('urlset')

    if sitemap_index:
        # Это индексная карта сайта - рекурсивно обходим все вложенные карты
        sitemaps = sitemap_index.find_all('sitemap')
        all_urls = []
        for sm in sitemaps:
            loc = sm.find('loc')
            if loc:
                sub_sitemap_url = loc.get_text(strip=True)
                sub_urls = await get_all_urls_from_sitemap(session, sub_sitemap_url)
                all_urls.extend(sub_urls)
        return all_urls

    elif urlset:
        # Обычная карта сайта
        urls = []
        for url_tag in urlset.find_all('url'):
            loc = url_tag.find('loc')
            if loc:
                url_text = loc.get_text(strip=True)
                urls.append(url_text)
        logger.info(f"Найдено {len(urls)} конечных URL в карте {sitemap_url}")
        return urls
    else:
        # Неизвестный формат
        logger.warning(f"Неизвестный формат sitemap: {sitemap_url}")
        return []

async def main():
    async with aiohttp.ClientSession() as session:
        # Получаем все конечные URL из sitemap (включая индексные)
        urls = await get_all_urls_from_sitemap(session, SITEMAP_URL)

        # Фильтрация URL по домену
        urls = [u for u in urls if urlparse(u).netloc == urlparse(BASE_URL).netloc]

        logger.info(f"Всего конечных URL для парсинга: {len(urls)}")
        results = []

        # Ограничиваем количество одновременно обрабатываемых запросов
        sem = asyncio.Semaphore(40)

        async def bounded_parse(u):
            async with sem:
                await parse_page(session, u, results)

        tasks = [bounded_parse(u) for u in urls]
        await asyncio.gather(*tasks)

        # Сохраняем результат сразу в parsed_data_cleaned.json
        file_name = 'parsed_data_cleaned_v2.json'
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
        logger.info(f"Парсинг завершен. Данные сохранены в {file_name}")

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    logger.info(f"Общее время выполнения: {time.time() - start_time:.2f} сек")

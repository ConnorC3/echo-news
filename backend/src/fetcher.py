from dataclasses import dataclass, field
from typing import List
from datetime import datetime
import logging
import time
import asyncio
import httpx
import random
import json
import re

from urllib.parse import urlparse

import feedparser
from feedparser import FeedParserDict

logging.basicConfig(filename='fetch.log', level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class Article:
    title: str
    url: str
    source: str = ""
    published_at: str | None = None
    content: str | None = None
    fetched_at: datetime = field(default_factory=lambda: datetime.now())
    author: str | None = None
    tags: List[str] = field(default_factory=list)

def _strip_html(text: str) -> str:
    if not text:
        return ''
    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean

def _parse_entry(fetched: FeedParserDict, entry: FeedParserDict) -> Article | None:
    title = entry.get('title')
    url = entry.get('link')

    if not url:
        return None
    
    if not title:
        title = "Untitled"
    
    source = fetched.feed.get('title')

    published_at = (
        entry.get('published') or 
        entry.get('updated')
    )
    
    content_list = entry.get('content', [])

    content = (
        (content_list[0].get('value', '') if content_list else None) or
        entry.get('description') or 
        entry.get('summary') or
        ''
    )

    content = _strip_html(content)

    if len(content) < 50:
        content = None
    
    author = entry.get('author', None)
    tags = [t.get('term', '') for t in entry.get('tags', [])]

    # print(f"Title: {title}, URL: {url}")

    return Article(
        title=title,
        url=url,
        source=source,
        published_at=published_at,
        content=content,
        author=author,
        tags=tags
    ) 

class FeedFetcher:
    def __init__(self, max_concurrent: int = 10):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.domain_locks: dict[str, asyncio.Lock] = {}
    
    def _get_domain_lock(self, url: str) -> asyncio.Lock:
        domain = urlparse(url).netloc
        if domain not in self.domain_locks:
            self.domain_locks[domain] = asyncio.Lock()
        return self.domain_locks[domain]

    def _get_delay(self, response: httpx.Response, att: int) -> float:
        retry_after = response.headers.get('Retry-After')
        delay = 0.0
        if retry_after:
            try:
                delay = float(retry_after)
            except ValueError:
                delay = 2**att + random.random()
        else:
            delay = 2**att + random.random()
        
        return delay
    
    async def fetch_once(self, url: str, client: httpx.AsyncClient, max_attempts: int = 3) -> List[Article]:
        logger.info(f"Starting fetch: {url}")
        t_start = time.perf_counter()
        
        response = None
        
        domain_lock = self._get_domain_lock(url)

        for att in range(max_attempts):
            try:
                async with domain_lock:
                    async with self.semaphore:
                        response = await client.get(url)

                status = response.status_code

                if status == 200:
                    break
                elif status == 429 or status >= 500:
                    delay = self._get_delay(response, att)
                    logger.warning(f"Attempt {att+1} for {url} failed with {status}, retrying in {delay:.1f}s")
                    await asyncio.sleep(delay)
                else:
                    response.raise_for_status()
                
            except (httpx.HTTPStatusError, httpx.RequestError) as e:
                logger.warning(f"Attempt {att+1} failed for {url}: {e}")
                if att == max_attempts - 1:
                    return []
                await asyncio.sleep(2**att + random.random())
        
        if not response or response.status_code != 200:
            logger.error(f"Failed to fetch {url} after {max_attempts} attempts")
            return []

        fetched = feedparser.parse(response.text)
        articles = []
        skipped = 0

        if fetched.bozo:
            logger.warning(f"Malformed XML: {fetched.bozo_exception}")
        
        if not fetched.entries:
            logger.warning(f"No entries found for {url}")
            logger.info(f"    Feed keys: {fetched.feed.keys() if hasattr(fetched, "feed") else 'None'}")
            return []

        for entry in fetched.entries:
            article = _parse_entry(fetched, entry)

            if article is not None:
                articles.append(article)
            else:
                skipped += 1
        
        if skipped > 0:
            logger.info(f"Skipped {skipped} articles from {url}")


        elapsed = time.perf_counter() - t_start
        logger.info(f"Completed fetch: {url} ({elapsed:.2f}s, {len(articles)} articles)")
        return articles
            
        
    async def fetch_all(self, urls_to_fetch: List[str]) -> List[List[Article]]:

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'Accept': 'application/rss+xml, application/xml;q=0.9, */*;q=0.8',
            'Accept-Language': 'application/rss+xml, application/xml;q=0.9, */*;q=0.8'
        }

        async with httpx.AsyncClient(timeout=10.0, headers=headers, follow_redirects=True) as client:
            # Though using semaphore to control how many workers execute,
            # All the tasks are still created at once
            # May need to think about limiting how many tasks are created
            # If I want to scale to hundreds of feeds
            tasks = [self.fetch_once(url, client) for url in urls_to_fetch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = 0
        failed = 0
        total_articles = 0

        valid_results = []
        for url, result in zip(urls_to_fetch, results):
            if isinstance(result, Exception):
                failed += 1
                logger.error(f"Failed to fetch {url}: {result}")
                valid_results.append([])
            else:
                num_articles = len(result)
                if num_articles > 0:
                    successful += 1
                    total_articles += num_articles
                valid_results.append(result)

        logger.info(f"Fetched {total_articles} articles from {successful}/{len(urls_to_fetch)} feeds")
        
        return valid_results

def feeds_to_json(feeds_to_add: List[str]):
    feeds = []

    for feed in feeds_to_add:
        feed_dict = {}
        feed_dict['url'] = feed
        feed_dict['category'] = ''
        feed_dict['active'] = True
        feeds.append(feed_dict)
    
    try:
        with open('feeds.json', 'a') as json_file:
            json.dump(feeds, json_file, indent=4)
    except FileNotFoundError as fe:
        raise fe

def json_to_feeds(json_file: str) -> List[str]:
    try: 
        with open(json_file, 'r') as jf:
            feeds = json.load(jf)
            feed_urls = []
            for feed in feeds:
                feed_urls.append(feed['url'])
            return feed_urls
    except FileNotFoundError as fe:
        raise fe

if __name__ == "__main__":
    fetcher = FeedFetcher()
    feeds_to_test = json_to_feeds('feeds.json')
    print(len(feeds_to_test))

    start = time.time()
    results = asyncio.run(fetcher.fetch_all(urls_to_fetch=feeds_to_test))
    end = time.time()

    for result in results:
        print(f"{'='*30} BEGIN RESULT {result[0].source} {'='*30}")
        for article in result:
            print(f"{'-'*30} BEGIN ARTICLE {article.title} {'-'*30}")
            print(article.content)

    print(f"Time for concurrent fetching: {(end - start):.2f}")

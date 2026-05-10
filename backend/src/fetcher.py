from dataclasses import dataclass, field
from typing import List
from datetime import datetime, timezone
import calendar
import logging
import time
import asyncio
import httpx
import random
import json
import re
import trafilatura

from urllib.parse import urlparse
from pathlib import Path

import feedparser
from feedparser import FeedParserDict


_LOG_FILE = Path(__file__).parent.parent / 'fetch.log'

logging.basicConfig(filename=_LOG_FILE, level=logging.INFO)
logger = logging.getLogger(__name__)

MIN_CONTENT_LENGTH = 500
NO_ENRICH_DOMAINS = {'nytimes.com', 'wsj.com', 'ft.com', 'forbes.com'}

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

def _get_headers() -> dict[str, str]:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'Accept': 'application/rss+xml, application/xml;q=0.9, */*;q=0.8',
        'Accept-Language': 'application/rss+xml, application/xml;q=0.9, */*;q=0.8'
    }

    return headers

def _strip_html(text: str) -> str:
    if not text:
        return ''
    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean

    
def _is_garbage_content(text: str) -> bool:
    lines = [l.strip() for l in text.splitlines()]
    if not lines:
        return True
    
    # Real articles have long lines, UI dumps are mostly short strings
    avg_line_length = sum(len(l) for l in lines) / len(lines)
    if avg_line_length < 40:
        return True
    
    # High uniqueness ratio check, UI text has many 1-3 word lines
    short_lines = sum(1 for l in lines if len(l.split()) < 4)
    if short_lines / len(lines) > 0.6:
        return True
    
    return False

def should_enrich(article: Article) -> bool:
    domain = urlparse(article.url).netloc.removeprefix('www.')
    return not any(domain.endswith(d) for d in NO_ENRICH_DOMAINS)

async def _enrich_content(article: Article, client: httpx.AsyncClient) -> Article:
    if article.content and len(article.content) > 200:
        # feedparser gave enough
        return article
    
    t = time.perf_counter()
    # Use trafilatura if feedparser not enough
    try:
        response = await client.get(article.url)
        response.raise_for_status()
        html = response.text

        if html:
            extracted = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: trafilatura.extract(
                    html, 
                    deduplicate=False,
                    include_comments=False,
                    no_fallback=True                    
                )
            )
            
            if extracted and len(extracted) > MIN_CONTENT_LENGTH and not _is_garbage_content(extracted):
                article.content = extracted
    except Exception as e:
        logger.warning(f"Trafilatura failed for {article.url}: {e}")
    finally:
        print(f"Enriched article in {time.perf_counter() - t:2f}s")

    return article

async def enrich_all(articles: List[Article]) -> List[Article]:
    max_concurrent = 50
    semaphore = asyncio.Semaphore(max_concurrent)
    timeout = httpx.Timeout(connect=3.0, read=8.0, write=3.0, pool=3.0)
    limits = httpx.Limits(max_keepalive_connections=max_concurrent)
    headers = _get_headers()

    async with httpx.AsyncClient(
        timeout=timeout, 
        limits=limits, 
        follow_redirects=True,
        headers=headers
    ) as client:
        async def enrich_one(article: Article):
            async with semaphore:
                return await _enrich_content(article, client)
        
        articles = [a for a in articles if should_enrich(a)]

        results = await asyncio.gather(*[enrich_one(a) for a in articles], return_exceptions=True)

    return [a for a in results if not isinstance(a, Exception)]
    

def _parse_feedparser_date(parsed_date) -> datetime:
    if not parsed_date:
        return None
    
    timestamp = calendar.timegm(parsed_date)
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)

def _parse_entry(fetched: FeedParserDict, entry: FeedParserDict) -> Article | None:
    title = entry.get('title')
    url = entry.get('link')

    if not url:
        return None
    
    if not title:
        title = "Untitled"
    
    source = fetched.feed.get('title')

    published_parsed = (
        entry.get('published_parsed') or
        entry.get('updated_parsed')
    )

    published_at = _parse_feedparser_date(published_parsed)
    
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

def deduplicate_articles(articles: List[Article]) -> List[Article]:
    seen_urls = set()
    unique = []
    for article in articles:
        if article.url not in seen_urls:
            seen_urls.add(article.url)
            unique.append(article)
    return unique


def _get_delay(response: httpx.Response, att: int) -> float:
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

async def _fetch_once(
        url: str, 
        client: httpx.AsyncClient, 
        semaphore: asyncio.Semaphore, 
        domain_locks: dict[str, asyncio.Lock],  
        max_attempts: int = 3
) -> List[Article]:
    logger.info(f"Starting fetch: {url}")
    t_start = time.perf_counter()
    
    response = None
    
    domain = urlparse(url).netloc

    # setdefault atomic, so no race condition when checking
    # if lock exists
    domain_lock = domain_locks.setdefault(domain, asyncio.Lock())

    for att in range(max_attempts):
        try:

            # domain lock and semaphore only acquired when 
            # fetching url, prevents starvation
            async with domain_lock:
                async with semaphore:
                    response = await client.get(url)

            status = response.status_code

            if status == 200:
                break
            elif status == 429 or status >= 500:
                delay = _get_delay(response, att)
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
        
    
async def fetch_all(urls_to_fetch: List[str]) -> List[Article]:
    semaphore = asyncio.Semaphore(10)
    domain_locks: dict[str, asyncio.Lock] = {}
    headers = _get_headers()

    async with httpx.AsyncClient(timeout=5.0, headers=headers, follow_redirects=True) as client:
        # Though using semaphore to control how many workers execute,
        # All the tasks are still created at once
        # May need to think about limiting how many tasks are created
        # If I want to scale to hundreds of feeds
        tasks = [_fetch_once(url, client, semaphore, domain_locks) for url in urls_to_fetch]
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

    all_articles = [a for feed in valid_results for a in feed]
    logger.info(f"Fetched {total_articles} articles from {successful}/{len(urls_to_fetch)} feeds")
    
    all_articles = deduplicate_articles(all_articles)
    logger.info(f"After deduplication: {len(all_articles)} unique articles")

    return all_articles

def feeds_to_json(feeds_to_add: List[str]):
    feeds = []

    for feed in feeds_to_add:
        feed_dict = {}
        feed_dict['url'] = feed
        feed_dict['category'] = ''
        feed_dict['active'] = True
        feeds.append(feed_dict)
    
    try:
        with open('feeds.json', 'a', encoding='utf-8') as json_file:
            json.dump(feeds, json_file, indent=4)
    except FileNotFoundError as fe:
        raise fe

def json_to_feeds(json_file: str) -> List[str]:
    try: 
        with open(json_file, 'r') as jf:
            feeds = json.load(jf)
            feed_urls = []
            for feed in feeds:
                if feed['active']:
                    feed_urls.append(feed['url'])
            return feed_urls
    except FileNotFoundError as fe:
        raise fe

# if __name__ == "__main__":
#     fetcher = FeedFetcher()
#     feeds_to_test = json_to_feeds('feeds.json')
#     print(len(feeds_to_test))

#     start = time.time()
#     results = asyncio.run(fetcher.fetch_all(urls_to_fetch=feeds_to_test))
#     end = time.time()

#     for result in results:
#         print(f"{'='*30} BEGIN RESULT {result[0].source} {'='*30}")
#         for article in result:
#             print(f"{'-'*30} BEGIN ARTICLE {article.title} {'-'*30}")
#             print(article.published_at)

#     print(f"Time for concurrent fetching: {(end - start):.2f}")

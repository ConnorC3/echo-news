import asyncio
import time
from typing import List

from fetcher import Article, FeedFetcher, json_to_feeds
from db import init_db, add_articles

async def main():
    fetcher = FeedFetcher()
    pool = await init_db()

    feeds_to_test = json_to_feeds('feeds.json')
    print(len(feeds_to_test))

    start = time.time()
    articles = await fetcher.fetch_all(urls_to_fetch=feeds_to_test)
    
    inserted = 0
    skipped = 0
    dups = 0

    counts = await add_articles(pool=pool, articles=articles)

    inserted += counts['inserted']
    skipped += counts['urls_skipped']
    dups += counts['content_duplicates']

    end = time.time()

    print(f"Inserted {inserted}, skipped {skipped}, counted {dups} duplicates")
    print(f"Time for concurrent fetching + adding to database: {(end - start):.2f}")

asyncio.run(main())

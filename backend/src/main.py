import asyncio
import time
import os
from pathlib import Path
from typing import List

from fetcher import Article, json_to_feeds, enrich_all, fetch_all
from db.db import init_db, add_articles, filter_new_articles



async def main():
    pool = await init_db()

    current_dir = Path(__file__).parent

    feeds_to_test = json_to_feeds(os.path.join(current_dir, 'feeds.json'))
    print(len(feeds_to_test))

    start = time.time()
    articles = await fetch_all(urls_to_fetch=feeds_to_test)

    new_articles = await filter_new_articles(pool, articles)
    print(f"New articles needing enrichment: {len(new_articles)}")

    enriched = await enrich_all(new_articles)
    counts = await add_articles(pool=pool, articles=enriched)
    end = time.time()

    inserted = counts['inserted']
    skipped = counts['urls_skipped']
    content_dups = counts['content_duplicates']
    near_dups = counts['near_duplicates']


    print(f"Inserted {inserted}, skipped {skipped}, counted {content_dups} content duplicates, {near_dups} near duplicates (minhash)")
    print(f"Time for concurrent fetching + adding to database: {(end - start):.2f}")

asyncio.run(main())

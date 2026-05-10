import asyncpg
import asyncio
import os
import hashlib
import pickle
from dotenv import load_dotenv

from typing import List
from fetcher import Article
from minhash.minhash import compute_minhash, jaccard_from_bytes

load_dotenv()

def compute_content_hash(content: str | None) -> str | None:
    if not content or len(content) < 50:
        return None
    
    content_bytes = content.encode("utf-8")
    return hashlib.sha256(content_bytes).hexdigest()

async def init_db():
    pool = await asyncpg.create_pool(
        host=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USER', 'echoes'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME', 'echoes'),
        min_size=2,
        max_size=10
    )

    return pool

async def add_articles(pool: asyncpg.Pool, articles: List[Article]) -> dict[str, int]:
    inserted = 0
    url_skipped = 0
    content_dups = 0
    near_dups = 0

    for article in articles:
        content_hash = compute_content_hash(article.content)
        result = await _insert_article(pool, article, content_hash)

        if result == "inserted":
            inserted += 1
        elif result == "url_duplicate":
            url_skipped += 1
        elif result == "content_duplicate":
            content_dups += 1
        elif result == "near_duplicate":
            near_dups += 1
    
    return {
        "inserted": inserted,
        "urls_skipped": url_skipped,
        "content_duplicates": content_dups,
        "near_duplicates": near_dups
    }

async def filter_new_articles(pool: asyncpg.Pool, articles: List[Article]) -> List[Article]:
    urls = [a.url for a in articles]
    existing_in_db = await pool.fetch(
        "SELECT url FROM articles WHERE url = ANY($1::text[])", 
        urls
    )
    existing_urls = {row['url'] for row in existing_in_db}
    return [a for a in articles if a.url not in existing_urls]


async def find_near_duplicates(pool: asyncpg.Pool, signature: bytes, threshold: float = 0.8):
    rows = await pool.fetch("""
        SELECT url, minhash_signature
        FROM articles
        WHERE minhash_signature IS NOT NULL
        AND created_at > NOW() - INTERVAL '7 days'
    """)

    for row in rows:
        existing_sig = row['minhash_signature']
        similarity = jaccard_from_bytes(signature, existing_sig)
        if similarity >= threshold:
            return row['url'] # Returns first found duplicate, not all
    
    return None

async def _insert_article(pool: asyncpg.Pool, article: Article, content_hash: str | None) -> str:
    # First check content hash
    if content_hash:
        content_dup_url = await pool.fetchval(
            "SELECT url FROM articles WHERE content_hash = $1",
            content_hash
        )
        if content_dup_url:
            await _record_content_duplicate(pool, article, content_hash)
            return "content_duplicate"
    
    # Check minhash signature
    minhash_sig = compute_minhash(article.content)

    if minhash_sig:
        duplicate = await find_near_duplicates(pool, minhash_sig)
        if duplicate:
            await _record_near_duplicate(pool, article, content_hash, minhash_sig, duplicate)
            return "near_duplicate"

    # Insert or skip if URL is the same
    result = await pool.fetchval("""
            INSERT INTO articles (url, title, source, published_at, content, content_hash, minhash_signature, author, tags, fetched_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (url) DO NOTHING
            RETURNING id
        """, 
        article.url, article.title, article.source, 
        article.published_at, article.content, content_hash, 
        minhash_sig, article.author, article.tags, article.fetched_at
    )
    return "inserted" if result is not None else "url_duplicate"

async def _record_content_duplicate(pool: asyncpg.Pool, article: Article, content_hash: str | None):
    async with pool.acquire() as conn:
        original_url = await conn.fetchval(
            "SELECT url FROM articles WHERE content_hash = $1", 
            content_hash
        )
        await conn.execute("""
            INSERT INTO articles (url, title, source, published_at, duplicate_of_url, author, tags, fetched_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (url) DO NOTHING
        """,
        article.url, article.title, article.source,
        article.published_at, original_url,
        article.author, article.tags, article.fetched_at
        )

async def _record_near_duplicate(pool: asyncpg.Pool, article: Article, content_hash: str | None, minhash: bytes, duplicate_url: str):
    await pool.execute("""
        INSERT INTO articles (url, title, source, published_at, content_hash, minhash_signature, duplicate_of_url, author, tags, fetched_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (url) DO NOTHING
    """,
    article.url, article.title, article.source,
    article.published_at, content_hash, minhash, duplicate_url,
    article.author, article.tags, article.fetched_at
    )




# TESTING
# async def main():
#     pool: asyncpg.Pool = await init_db()

#     await add_articles(pool=pool, articles=[
#         Article(
#             title="Title", 
#             url="http://example.com",
#             source="Example",
#             content="None",
#             author="Someone",
#             tags=["Ex1", "Ex2", "Ex3"]
#         )]
#     )


# asyncio.run(main())

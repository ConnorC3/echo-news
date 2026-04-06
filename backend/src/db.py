import asyncpg
import asyncio
import os
import hashlib
from dotenv import load_dotenv

from typing import List
from fetcher import Article

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

    for article in articles:
        content_hash = compute_content_hash(article.content)
        result = await _insert_article(pool, article, content_hash)

        if result == "inserted":
            inserted += 1
        elif result == "url_duplicate":
            url_skipped += 1
        elif result == "content_duplicate":
            content_dups += 1
    
    return {
        "inserted": inserted,
        "urls_skipped": url_skipped,
        "content_duplicates": content_dups
    }


async def _insert_article(pool: asyncpg.Pool, article: Article, content_hash: str | None) -> str:
    try:
        result = await pool.fetchval("""
                INSERT INTO articles (url, title, source, published_at, content, content_hash, author, tags, fetched_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (url) DO NOTHING
                RETURNING id
            """, 
            article.url, article.title, article.source, 
            article.published_at, article.content, content_hash, 
            article.author, article.tags, article.fetched_at
        )
        return "inserted" if result is not None else "url_duplicate"
    except asyncpg.UniqueViolationError:
        # content hash conflict, record duplicate
        await _record_content_duplicate(pool, article, content_hash)
        return "content_duplicate"

async def _record_content_duplicate(pool: asyncpg.Pool, article: Article, content_hash: str | None):
    async with pool.acquire() as conn:
        original_url = await conn.fetchval(
            "SELECT url FROM articles WHERE content_hash = $1", 
            content_hash
        )
        await conn.execute("""
            INSERT INTO articles (url, title, source, published_at, content_hash, duplicate_of_url, author, tags, fetched_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (url) DO NOTHING
        """,
        article.url, article.title, article.source,
        article.published_at, content_hash, original_url,
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

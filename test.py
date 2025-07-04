"""
5-crawl_recursive_internal_links.py
----------------------------------
Recursively crawls a site starting from a root URL using Crawl4AI,
and saves successful markdown pages to a JSONL file.
"""
import asyncio
from urllib.parse import urldefrag
from crawl4ai import (
    AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode,
    MemoryAdaptiveDispatcher
)
import json
import os

OUTPUT_PATH = "crawl_output.jsonl"

async def crawl_recursive_batch(start_urls, max_depth=3, max_concurrent=10):
    browser_config = BrowserConfig(headless=True, verbose=False)
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        stream=False
    )
    dispatcher = MemoryAdaptiveDispatcher(
        memory_threshold_percent=70.0,
        check_interval=1.0,
        max_session_permit=max_concurrent
    )

    visited = set()
    def normalize_url(url):
        return urldefrag(url)[0]
    
    def is_html_page(url):
        non_html_exts = [".zip", ".tar", ".gz", ".rar", ".jar", ".exe", ".iso", ".7z", ".bz2"]
        return url.startswith("http") and not any(url.lower().endswith(ext) for ext in non_html_exts)


    current_urls = set([normalize_url(u) for u in start_urls])

    output_dir = os.path.dirname(OUTPUT_PATH)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as output_file:
        async with AsyncWebCrawler(config=browser_config) as crawler:
            for depth in range(max_depth):
                print(f"\n=== Crawling Depth {depth+1} ===")
                all_urls = set([normalize_url(url) for url in current_urls if normalize_url(url) not in visited])

                urls_to_crawl = [url for url in all_urls if is_html_page(url)]
                skipped_urls = [url for url in all_urls if not is_html_page(url)]

                if skipped_urls:
                    os.makedirs("mosdac_data", exist_ok=True)
                    with open("mosdac_data/skipped_urls.txt", "a", encoding="utf-8") as f:
                        for url in skipped_urls:
                            f.write(url + "\n")
                            if not urls_to_crawl:
                                break

                results = await crawler.arun_many(
                    urls=urls_to_crawl,
                    config=run_config,
                    dispatcher=dispatcher
                )

                next_level_urls = set()

                for result in results:
                    norm_url = normalize_url(result.url)
                    visited.add(norm_url)

                    if result.success:
                        print(f"[OK] {result.url} | Markdown: {len(result.markdown or '')} chars")
                        
                        output_file.write(json.dumps({
                            "url": result.url,
                            "content": result.markdown,
                            "metadata": {
                                "status_code": result.status_code,
                                "depth": depth + 1
                            }
                        }) + "\n")

                        for link in result.links.get("internal", []):
                            next_url = normalize_url(link["href"])
                            if next_url not in visited:
                                next_level_urls.add(next_url)
                    else:
                        print(f"[ERROR] {result.url}: {result.error_message}")

                current_urls = next_level_urls


if __name__ == "__main__":
    asyncio.run(crawl_recursive_batch(["https://mosdac.gov.in/"], max_depth=3, max_concurrent=10))

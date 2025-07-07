import asyncio
from urllib.parse import urldefrag
from crawl4ai import (
    AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode,
    MemoryAdaptiveDispatcher
)
import os
import json

PDF_OUTPUT = "mosdac_pdfs.jsonl"

async def extract_pdfs(start_urls, max_depth=3, max_concurrent=10):
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
    pdf_links = set()

    def normalize_url(url):
        return urldefrag(url)[0]

    def is_html_page(url):
        non_html_exts = [".zip", ".tar", ".gz", ".rar", ".jar", ".exe", ".iso", ".7z", ".bz2"]
        return url.startswith("http") and not any(url.lower().endswith(ext) for ext in non_html_exts)

    current_urls = set([normalize_url(u) for u in start_urls])

    with open(PDF_OUTPUT, "w", encoding="utf-8") as f:
        async with AsyncWebCrawler(config=browser_config) as crawler:
            for depth in range(max_depth):
                print(f"\n=== Crawling Depth {depth+1} ===")
                all_urls = set([normalize_url(url) for url in current_urls if normalize_url(url) not in visited])
                urls_to_crawl = [url for url in all_urls if is_html_page(url)]

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
                        # Look for PDFs in internal and external links
                        links = result.links.get("internal", []) + result.links.get("external", [])
                        for link in links:
                            href = link["href"]
                            if href.endswith(".pdf"):
                                pdf_url = normalize_url(href)
                                if pdf_url not in pdf_links:
                                    pdf_links.add(pdf_url)
                                    print(f"[PDF] {pdf_url}")
                                    f.write(json.dumps({"pdf_url": pdf_url, "source_page": result.url}) + "\n")

                        # Continue crawling internal HTML pages
                        for link in result.links.get("internal", []):
                            next_url = normalize_url(link["href"])
                            if next_url not in visited:
                                next_level_urls.add(next_url)

                current_urls = next_level_urls

if __name__ == "__main__":
    asyncio.run(extract_pdfs(["https://mosdac.gov.in/"], max_depth=3, max_concurrent=10))

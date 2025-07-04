import json
import os
import requests
import uuid
import fitz
import time
from tqdm import tqdm
import tiktoken

def count_tokens(text: str) -> int:
    tokenizer = tiktoken.get_encoding("cl100k_base")
    return len(tokenizer.encode(text))

def extract_text_from_pdf(file_path: str) -> str:
    doc = fitz.open(file_path)
    text = ""
    for page in doc:
        text += page.get_text()
    return text

def main(input_file="mosdac_pdfs.jsonl", output_file="mosdac_pdfs_text.jsonl", download_folder="downloaded_pdfs"):
    os.makedirs(download_folder, exist_ok=True)

    with open(input_file, "r", encoding="utf-8") as f:
        urls = [json.loads(line)["pdf_url"] for line in f]

    with open(output_file, "w", encoding="utf-8") as out_file:
        for url in tqdm(urls, desc="Processing PDFs", unit="file"):
            try:
                filename = os.path.join(download_folder, url.split("/")[-1])
                # Download
                if not os.path.exists(filename):
                    r = requests.get(url, timeout=30)
                    with open(filename, "wb") as f:
                        f.write(r.content)

                # Extract Text
                text = extract_text_from_pdf(filename)
                title = os.path.basename(filename)
                tokens = count_tokens(text)

                out_file.write(json.dumps({
                    "id": str(uuid.uuid4()),
                    "url": url,
                    "title": title,
                    "text": text,
                    "tokens": tokens
                }, ensure_ascii=False) + "\n")

            except Exception as e:
                tqdm.write(f"[ERROR] Failed processing {url}: {e}")

if __name__ == "__main__":
    start = time.time()
    main()
    print(f"\n Done in {round(time.time() - start, 2)} seconds.")

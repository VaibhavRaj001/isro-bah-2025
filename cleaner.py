import json
import re
import tiktoken


enc = tiktoken.encoding_for_model("gpt-3.5-turbo")

def clean_markdown(md: str) -> str:
    md = re.sub(r'!\[.*?\]\(.*?\)', '', md)  
    md = re.sub(r'\[.*?\]\(javascript:[^)]+\)', '', md)  
    md = re.sub(r'\[\s*\]\(.*?\)', '', md)  
    md = re.sub(r'\[.*?\]\(.*?\)', lambda m: m.group(0).split(']')[0][1:], md)  
    md = re.sub(r'[#`\*]{1,}', '', md)  
    md = re.sub(r'\n{2,}', '\n\n', md)  
    return md.strip()

def extract_sections(md: str):
    sections = []
    current = {"heading": None, "content": []}
    for line in md.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#"):
            if current["heading"] and current["content"]:
                sections.append(current)
            current = {"heading": line.lstrip("#").strip(), "content": []}
        else:
            current["content"].append(line)
    if current["heading"] and current["content"]:
        sections.append(current)
    return sections or [{"heading": "General", "content": md.splitlines()}]

def chunk_text(text: str, max_tokens=512):
    words = text.split()
    chunks = []
    current = []

    for word in words:
        current.append(word)
        token_count = len(enc.encode(" ".join(current)))
        if token_count >= max_tokens:
            chunks.append(" ".join(current))
            current = []
    if current:
        chunks.append(" ".join(current))
    return chunks

# Input & output paths
INPUT_FILE = "crawl_output.jsonl"
OUTPUT_FILE = "llm_ready_output.jsonl"

with open(INPUT_FILE, "r", encoding="utf-8") as infile, open(OUTPUT_FILE, "w", encoding="utf-8") as outfile:
    for line in infile:
        try:
            data = json.loads(line)
            url = data.get("url")
            content = data.get("content", "")
            if not content or not url:
                continue

            cleaned = clean_markdown(content)
            sections = extract_sections(cleaned)

            for section in sections:
                text = " ".join(section["content"])
                chunks = chunk_text(text)

                for i, chunk in enumerate(chunks):
                    record = {
                        "id": f"{url}#chunk-{i+1}",
                        "url": url,
                        "title": section["heading"],
                        "text": chunk,
                        "tokens": len(enc.encode(chunk))
                    }
                    outfile.write(json.dumps(record, ensure_ascii=False) + "\n")

        except Exception as e:
            print(f"Error processing line: {e}")

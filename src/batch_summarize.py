"""
batch_summarize.py — send thousands of files to the /summarize endpoint in batches.

Usage:
    python src/batch_summarize.py --input-dir ./docs --output results.csv
    python src/batch_summarize.py --input-dir ./docs --output results.json --format json
    python src/batch_summarize.py --input-dir ./docs --output results.csv --batch-size 20 --concurrency 5
"""

import argparse
import asyncio
import json
import csv
import sys
from pathlib import Path

import httpx

API_URL = "http://127.0.0.1:8000/extract"
DEFAULT_BATCH_SIZE = 10   # files per request
DEFAULT_CONCURRENCY = 4   # simultaneous requests (keeps OpenAI rate limits sane)


async def send_batch(
    client: httpx.AsyncClient,
    paths: list[Path],
    semaphore: asyncio.Semaphore,
    output_format: str,
) -> list[dict]:
    """Send one batch of files and return parsed results."""
    async with semaphore:
        files = [
            ("files", (p.name, p.read_bytes(), "text/plain"))
            for p in paths
        ]
        try:
            response = await client.post(
                API_URL,
                files=files,
                params={"output_format": "json"},  # always JSON so we can merge
                timeout=120,
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            print(f"  [ERROR] batch {[p.name for p in paths]}: HTTP {e.response.status_code} — {e.response.text[:200]}")
            return []
        except Exception as e:
            print(f"  [ERROR] batch {[p.name for p in paths]}: {e}")
            return []


def chunk(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


async def run(input_dir: Path, output: Path, batch_size: int, concurrency: int, fmt: str):
    files = sorted([*input_dir.rglob("*.docx"), *input_dir.rglob("*.txt")])
    if not files:
        print(f"No .txt files found in {input_dir}")
        sys.exit(1)

    print(f"Found {len(files)} files — batch_size={batch_size}, concurrency={concurrency}")

    semaphore = asyncio.Semaphore(concurrency)
    batches = list(chunk(files, batch_size))
    all_results: list[dict] = []

    async with httpx.AsyncClient() as client:
        tasks = [
            send_batch(client, batch, semaphore, fmt)
            for batch in batches
        ]

        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            results = await coro
            all_results.extend(results)
            print(f"  [{i}/{len(batches)}] done — {len(all_results)} summaries so far")

    # Sort final results by word_count descending
    all_results.sort(key=lambda x: x.get("word_count", 0), reverse=True)

    if fmt == "csv":
        with output.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["filename", "word_count", "reading_time_minutes", "summary"]
            )
            writer.writeheader()
            writer.writerows(all_results)
    else:
        output.write_text(json.dumps(all_results, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\nDone. {len(all_results)}/{len(files)} files summarized → {output}")


def main():
    parser = argparse.ArgumentParser(description="Batch summarize documents via OpenFast API")
    parser.add_argument("--input-dir", required=True, type=Path, help="Directory containing .txt files")
    parser.add_argument("--output", required=True, type=Path, help="Output file path (.csv or .json)")
    parser.add_argument("--format", dest="fmt", choices=["csv", "json"], default="csv")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Files per request (default 10)")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Parallel requests (default 4)")
    args = parser.parse_args()

    if not args.input_dir.is_dir():
        print(f"Error: {args.input_dir} is not a directory")
        sys.exit(1)

    asyncio.run(run(args.input_dir, args.output, args.batch_size, args.concurrency, args.fmt))


if __name__ == "__main__":
    main()

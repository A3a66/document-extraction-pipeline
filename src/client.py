"""
client.py — send a file to OpenFast and print/save the response.

Usage:
    python src/client.py test_notes.txt
    python src/client.py test_notes.txt --format csv --output result.csv
    python src/client.py notes1.txt notes2.docx --format json
"""

import argparse
import json
import sys
from pathlib import Path

import httpx

API_URL = "http://127.0.0.1:8000/extract"


def send(file_paths: list[Path], output_format: str) -> dict | list | bytes:
    files = [
        ("files", (p.name, p.read_bytes(), "application/octet-stream"))
        for p in file_paths
    ]
    response = httpx.post(
        API_URL,
        files=files,
        params={"output_format": output_format},
        timeout=60,
    )
    response.raise_for_status()

    if output_format == "csv":
        return response.content  # raw bytes
    return response.json()


def main():
    parser = argparse.ArgumentParser(description="Send files to OpenFast and get structured data back")
    parser.add_argument("files", nargs="+", type=Path, help=".txt or .docx files to process")
    parser.add_argument("--format", choices=["json", "csv"], default="json")
    parser.add_argument("--output", type=Path, help="Save response to this file instead of printing")
    args = parser.parse_args()

    # Validate files exist
    for f in args.files:
        if not f.exists():
            print(f"Error: file not found — {f}")
            sys.exit(1)

    print(f"Sending {len(args.files)} file(s) to {API_URL} ...")

    try:
        result = send(args.files, args.format)
    except httpx.HTTPStatusError as e:
        print(f"Server error {e.response.status_code}: {e.response.text}")
        sys.exit(1)
    except httpx.ConnectError:
        print("Could not connect. Is the server running? (myenv/Scripts/uvicorn src.OpenFast:app --reload)")
        sys.exit(1)

    if args.output:
        if args.format == "csv":
            args.output.write_bytes(result)
        else:
            args.output.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(f"Saved to {args.output}")
    else:
        if args.format == "csv":
            print(result.decode("utf-8"))
        else:
            print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

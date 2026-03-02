from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse, RedirectResponse
from openai import AsyncOpenAI, APIError
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import os
import logging
import csv
import io
import json
import asyncio
from typing import List, Optional
from docx import Document

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")

if not api_key:
    raise ValueError("OPENAI_API_KEY environment variable is not set")

app = FastAPI(
    title="OpenFast API",
    description="Extract structured tabular data from unstructured Word documents",
    version="2.0.0"
)
client = AsyncOpenAI(api_key=api_key)


class ExtractedRecord(BaseModel):
    filename: str = Field(..., description="Source filename")
    fields: dict = Field(..., description="Extracted key-value pairs from the document")


def read_docx(raw_bytes: bytes) -> str:
    """Extract plain text from a .docx file."""
    doc = Document(io.BytesIO(raw_bytes))
    paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
    return "\n".join(paragraphs)


async def _extract_fields(f: UploadFile) -> dict:
    """Read a .docx file and extract structured fields using OpenAI."""
    raw_bytes = await f.read()

    filename = f.filename or "untitled"
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

    if ext == "docx":
        try:
            text = read_docx(raw_bytes)
        except Exception as e:
            raise HTTPException(status_code=422, detail=f"Could not read '{filename}': {e}")
    elif ext == "txt":
        text = raw_bytes.decode("utf-8", errors="replace")
    else:
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported file type '{ext}' for '{filename}'. Upload .docx or .txt files."
        )

    if not text.strip():
        raise HTTPException(status_code=422, detail=f"'{filename}' appears to be empty.")

    logger.info(f"Extracting fields from: {filename}")

    prompt = (
        "You are a data extraction assistant. Read the notes below and extract all structured "
        "information into a flat JSON object. Identify meaningful fields such as names, dates, "
        "topics, action items, decisions, or any other key facts present in the text. "
        "Return ONLY a valid JSON object with no explanation, markdown, or code fences. "
        "Example: {\"name\": \"John\", \"date\": \"2024-01-01\", \"topic\": \"Budget review\"}\n\n"
        f"Notes:\n{text}"
    )

    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You extract structured data from unstructured notes. Return only valid JSON."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            max_tokens=1000,
            response_format={"type": "json_object"},
        )
        raw_json = response.choices[0].message.content.strip()
        fields = json.loads(raw_json)
    except APIError as e:
        logger.error(f"OpenAI API error for {filename}: {e}")
        raise HTTPException(status_code=502, detail=f"OpenAI API error for '{filename}': {str(e)}")
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error for {filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Could not parse OpenAI response for '{filename}'")

    return {"filename": filename, "fields": fields}


@app.post("/extract")
async def extract_documents(
    files: List[UploadFile] = File(...),
    output_format: Optional[str] = "json",
):
    """Upload .docx or .txt notes — returns structured tabular data extracted by AI.

    - `files`: one or more Word (.docx) or plain text (.txt) files.
    - `output_format`: `json` (default) or `csv`.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    # Process all files concurrently
    outcomes = await asyncio.gather(
        *[_extract_fields(f) for f in files],
        return_exceptions=True,
    )

    records: List[dict] = []
    failed: List[str] = []

    for f, outcome in zip(files, outcomes):
        if isinstance(outcome, HTTPException):
            raise outcome
        elif isinstance(outcome, Exception):
            logger.exception(f"Failed processing {f.filename}: {outcome}")
            failed.append(f.filename or "untitled")
        else:
            records.append(outcome)

    if not records:
        raise HTTPException(
            status_code=500,
            detail=f"All files failed to process: {', '.join(failed)}",
        )

    if failed:
        logger.warning(f"Partial success — skipped: {', '.join(failed)}")

    if output_format and output_format.lower() == "csv":
        # Collect all unique field names across every record (preserving insertion order)
        all_keys: list[str] = []
        seen: set[str] = set()
        for record in records:
            for key in record["fields"]:
                if key not in seen:
                    all_keys.append(key)
                    seen.add(key)

        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=["filename"] + all_keys, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            row = {"filename": record["filename"], **record["fields"]}
            writer.writerow(row)

        csv_buffer.seek(0)
        return StreamingResponse(
            iter([csv_buffer.getvalue().encode("utf-8")]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=extracted.csv"},
        )

    return [ExtractedRecord(**r) for r in records]


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.get("/")
async def root():
    """Redirect root to the interactive docs."""
    return RedirectResponse(url="/docs")

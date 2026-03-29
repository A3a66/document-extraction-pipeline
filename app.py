"""
OpenFast — Document Extraction App
Run with: streamlit run app.py
"""

import io
import json
import csv
import os

import streamlit as st
from openai import OpenAI
from dotenv import load_dotenv
from docx import Document

load_dotenv()

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OpenFast — Document Extraction",
    page_icon="📄",
    layout="wide",
)

st.title("📄 OpenFast — Document Extraction")
st.markdown("Upload unstructured `.docx` or `.txt` files and get clean, structured tabular data powered by AI.")

# ── API key ─────────────────────────────────────────────────────────────────────
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    api_key = st.text_input("Enter your OpenAI API key", type="password")
    if not api_key:
        st.warning("An OpenAI API key is required to use this app.")
        st.stop()

client = OpenAI(api_key=api_key)

# ── Helpers ─────────────────────────────────────────────────────────────────────

def read_docx(raw_bytes: bytes) -> str:
    doc = Document(io.BytesIO(raw_bytes))
    return "\n".join(p.text for p in doc.paragraphs if p.text.strip())


def extract_fields(filename: str, text: str) -> dict:
    prompt = (
        "You are a data extraction assistant. Read the notes below and extract all structured "
        "information into a flat JSON object. Identify meaningful fields such as names, dates, "
        "topics, action items, decisions, or any other key facts present in the text. "
        "Return ONLY a valid JSON object with no explanation, markdown, or code fences.\n\n"
        f"Notes:\n{text}"
    )
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You extract structured data from unstructured notes. Return only valid JSON."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.0,
        max_tokens=1000,
        response_format={"type": "json_object"},
    )
    fields = json.loads(response.choices[0].message.content.strip())
    return {"filename": filename, **fields}


def to_csv_bytes(records: list[dict]) -> bytes:
    all_keys = []
    seen = set()
    for r in records:
        for k in r:
            if k not in seen:
                all_keys.append(k)
                seen.add(k)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=all_keys, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(records)
    return buf.getvalue().encode("utf-8")


# ── Upload ───────────────────────────────────────────────────────────────────────
uploaded_files = st.file_uploader(
    "Upload files",
    type=["docx", "txt"],
    accept_multiple_files=True,
    help="You can upload multiple files at once",
)

if not uploaded_files:
    st.info("Upload one or more files above to get started.")
    st.stop()

st.markdown(f"**{len(uploaded_files)} file(s) ready.** Click the button below to extract.")

if st.button("Extract Data", type="primary", use_container_width=True):
    records = []
    errors = []

    progress = st.progress(0, text="Starting extraction...")
    status = st.empty()

    for i, f in enumerate(uploaded_files):
        status.markdown(f"Processing **{f.name}**...")
        try:
            raw = f.read()
            ext = f.name.rsplit(".", 1)[-1].lower()
            if ext == "docx":
                text = read_docx(raw)
            else:
                text = raw.decode("utf-8", errors="replace")

            if not text.strip():
                errors.append(f"{f.name} — empty file, skipped")
                continue

            record = extract_fields(f.name, text)
            records.append(record)
        except Exception as e:
            errors.append(f"{f.name} — {e}")

        progress.progress((i + 1) / len(uploaded_files), text=f"{i+1}/{len(uploaded_files)} files done")

    progress.empty()
    status.empty()

    if errors:
        with st.expander(f"⚠️ {len(errors)} file(s) failed"):
            for e in errors:
                st.error(e)

    if not records:
        st.error("No files were successfully processed.")
        st.stop()

    st.success(f"Extracted data from {len(records)} file(s).")
    st.session_state["records"] = records

# ── Results ───────────────────────────────────────────────────────────────────────
if "records" in st.session_state:
    records = st.session_state["records"]

    st.subheader("Results")
    import pandas as pd
    df = pd.DataFrame(records).astype(str)
    st.dataframe(df, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.download_button(
            label="⬇️ Download CSV",
            data=to_csv_bytes(records),
            file_name="extracted.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with col2:
        st.download_button(
            label="⬇️ Download JSON",
            data=json.dumps(records, indent=2, ensure_ascii=False).encode("utf-8"),
            file_name="extracted.json",
            mime="application/json",
            use_container_width=True,
        )

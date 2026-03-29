import csv
import re
from pathlib import Path

INPUT_FILE  = r"C:\Users\CHtui\Downloads\List of Agencies.csv"
OUTPUT_FILE = r"C:\Users\CHtui\Downloads\List of Agencies - Cleaned.csv"

OUTPUT_COLUMNS = [
    "First Name", "Last Name", "Company", "LinkedIn URL",
    "Website", "Email", "Phone", "Employees",
    "Location", "Status", "Notes", "Date Connected",
]

# CSV header row: ,A,B,C,D,E,F,G,H,I,J,K,L,
# Leading comma means the first col has an empty key "".
# DictReader key mapping:
#   ""=row_num, A=First Name, B=Last Name, C=Company, D=LinkedIn URL,
#   E=Website,  F=Email,      G=Phone,     H=Employees,
#   I=Location, J=Status,     K=Notes,     L=Date Connected
#
# Later rows (no row number) also pack phone into K and email into L.

ERRORS = {"#ERROR!", "#N/A", "#REF!", "#VALUE!", "#DIV/0!", "#NUM!"}


def cell(raw, key):
    v = raw.get(key, "") or ""
    v = v.strip().strip('"')
    return "" if v in ERRORS else v


def normalize_phone(v):
    if not v:
        return ""
    digits = re.sub(r"\D", "", v)
    if len(digits) < 7:
        return ""
    return ("+" if v.lstrip().startswith("+") else "") + digits


def is_url(v):
    return bool(v and v.strip().startswith("http"))


def is_email(v):
    return bool(v and re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", v.strip()))


def is_phone_only(v):
    return bool(v and "@" not in v
                and len(re.sub(r"\D", "", v)) >= 7
                and re.match(r"^[\d\s().+\-]+$", v.strip()))


def extract_email(text):
    m = re.search(r"[\w.+-]+@[\w.-]+\.[a-zA-Z]{2,}", text)
    return m.group(0) if m else ""


def extract_phone(text):
    m = re.search(r"[\+\d][\d\s().+\-]{6,}", text)
    return normalize_phone(m.group(0)) if m else ""


def clean_row(raw):
    company = cell(raw, "C")
    if not company or company == "Company":
        return None

    email  = cell(raw, "F")
    phone  = normalize_phone(cell(raw, "G"))
    notes  = cell(raw, "K")
    date   = cell(raw, "L")

    # Later rows pack phone into Notes (K) and email into Date Connected (L)
    if is_phone_only(notes):
        phone = phone or normalize_phone(notes)
        notes = ""
    if is_email(date):
        email = email or date
        date = ""

    # Recover from Notes text if still missing
    if not is_email(email) and notes:
        email = extract_email(notes)
    if not phone and notes:
        phone = extract_phone(notes)

    website = cell(raw, "E")
    if website and not is_url(website):
        website = ""

    return {
        "First Name":     cell(raw, "A"),
        "Last Name":      cell(raw, "B"),
        "Company":        company,
        "LinkedIn URL":   cell(raw, "D"),
        "Website":        website,
        "Email":          email,
        "Phone":          phone,
        "Employees":      cell(raw, "H"),
        "Location":       cell(raw, "I"),
        "Status":         cell(raw, "J") or "Not contacted",
        "Notes":          notes,
        "Date Connected": date,
    }


def main():
    path = Path(INPUT_FILE)
    if not path.exists():
        print(f"ERROR: File not found: {INPUT_FILE}")
        return

    with open(path, newline="", encoding="utf-8-sig") as f:
        raw_rows = list(csv.DictReader(f))

    print(f"Read {len(raw_rows)} raw rows.")

    out, seen = [], set()
    skipped = 0

    for raw in raw_rows:
        row = clean_row(raw)
        if row is None:
            skipped += 1
            continue

        key = row["Company"].lower()
        if key in seen:
            print(f"  Duplicate: {row['Company']}")
            skipped += 1
            continue
        seen.add(key)
        out.append(row)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(out)

    print(f"Done. {len(out)} contacts written, {skipped} skipped.")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

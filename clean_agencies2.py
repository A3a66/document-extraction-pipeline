import csv
import re
from pathlib import Path

INPUT_FILE = r"C:\Users\CHtui\Downloads\List of Agencies.csv"
OUTPUT_FILE = r"C:\Users\CHtui\Downloads\List of Agencies - Cleaned.csv"

OUTPUT_COLUMNS = [
    "First Name", "Last Name", "Company", "LinkedIn URL",
    "Website", "Email", "Phone", "Employees",
    "Location", "Status", "Notes", "Date Connected",
]


def clean_error(value):
    if not value or value.strip() in ("#ERROR!", "#N/A", "#REF!", "#VALUE!", "#DIV/0!"):
        return ""
    return value.strip()


def normalize_phone(phone):
    phone = clean_error(phone)
    if not phone:
        return ""
    prefix = "+" if phone.lstrip().startswith("+") else ""
    digits = re.sub(r"\D", "", phone)
    if len(digits) < 7:
        return ""
    return prefix + digits


def looks_like_url(value):
    return bool(value and value.strip().startswith("http"))


def looks_like_email(value):
    return bool(value and re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value.strip()))


def is_phone_only(value):
    return bool(value and "@" not in value
                and len(re.sub(r"\D", "", value)) >= 7
                and re.match(r"^[\d\s().+\-]+$", value.strip()))


def extract_email(text):
    m = re.search(r"[\w.+-]+@[\w.-]+\.[a-zA-Z]{2,}", text)
    return m.group(0).strip() if m else ""


def extract_phone(text):
    m = re.search(r"[\+\d][\d\s().+\-]{6,}", text)
    return normalize_phone(m.group(0)) if m else ""


def clean_row(raw):
    # Header row is: A,B,C,D,E,F,G,H,I,J,K,L,  (trailing comma = 13th empty-key col)
    # A=row_num, B=First Name, C=Last Name, D=Company, E=LinkedIn URL,
    # F=Website, G=Email, H=Phone, I=Employees, J=Location,
    # K=Status, L=Notes, ""=Date Connected

    company = clean_error(raw.get("D", ""))

    # Skip blank rows and the header-as-data row
    if not company or company == "Company":
        return None

    notes = clean_error(raw.get("L", ""))
    date  = clean_error(raw.get("", ""))
    email = clean_error(raw.get("G", ""))
    phone = normalize_phone(raw.get("H", ""))

    # Later rows pack phone into Notes column and email into Date Connected column
    if is_phone_only(notes):
        phone = phone or normalize_phone(notes)
        notes = ""
    if looks_like_email(date):
        email = email or date
        date = ""

    # Last-resort recovery from Notes text
    if not looks_like_email(email) and notes:
        email = extract_email(notes)
    if not phone and notes:
        phone = extract_phone(notes)

    website = clean_error(raw.get("F", ""))
    if website and not looks_like_url(website):
        website = ""

    return {
        "First Name":     clean_error(raw.get("B", "")),
        "Last Name":      clean_error(raw.get("C", "")),
        "Company":        company,
        "LinkedIn URL":   clean_error(raw.get("E", "")),
        "Website":        website,
        "Email":          email,
        "Phone":          phone,
        "Employees":      clean_error(raw.get("I", "")),
        "Location":       clean_error(raw.get("J", "")),
        "Status":         clean_error(raw.get("K", "")) or "Not contacted",
        "Notes":          notes,
        "Date Connected": date,
    }


def main():
    input_path = Path(INPUT_FILE)
    if not input_path.exists():
        print(f"ERROR: File not found: {INPUT_FILE}")
        return

    raw_rows = []
    with open(input_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            raw_rows.append(dict(r))

    print(f"Read {len(raw_rows)} raw rows.")

    cleaned = []
    seen = set()
    skipped = 0

    for raw in raw_rows:
        row = clean_row(raw)
        if row is None:
            skipped += 1
            continue

        key = row["Company"].lower().strip()
        if key in seen:
            skipped += 1
            print(f"  Duplicate: {row['Company']}")
            continue
        seen.add(key)
        cleaned.append(row)

    output_path = Path(OUTPUT_FILE)
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(cleaned)

    print(f"\nDone. {len(cleaned)} contacts written, {skipped} skipped.")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

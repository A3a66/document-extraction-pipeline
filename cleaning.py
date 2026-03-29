import csv
import re
import pandas as pd

INPUT_FILE = r"C:\Users\CHtui\Downloads\ProperProcess.csv"
OUTPUT_FILE = r"C:\Users\CHtui\Downloads\ProcessListCleaned.csv"

# Read CSV
with open(INPUT_FILE, 'r', newline='') as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)  # first row
    print("Header:", header)
    
    remaining_rows = list(reader)  # store the rest
    for row in remaining_rows:
        print("Row:", row)

# Write header + remaining rows to one CSV
with open(OUTPUT_FILE, 'w', newline='') as outfile:
    writer = csv.writer(outfile)
    
    # Write header first
    writer.writerow(header)
    
    # Write all data rows
    writer.writerows(remaining_rows)

df = pd.read_csv(OUTPUT_FILE)
print(df)

Header =  ['First Name', 'Last Name', 'Company', 'LinkedIn URL', 'Website', 'Email', 'Phone', 'Employees', 'Location', 'Status', 'Notes', 'Date Connected']


def is_url(v):
    return bool(v and v.strip().startswith("http"))

def is_phone_only(v):
    return bool(v and "@" not in v
                and len(re.sub(r"\D", "", v)) >= 7
                and re.match(r"^[\d\s().+\-]+$", v.strip()))

def is_email(v):
    return bool(v and re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", v.strip()))


def clean_column(df, col_name):
    for idx, row in df.iterrows():
        text = str(row[col_name]) if pd.notna(row[col_name]) else ''
        items = text.split(' | ')
        remaining = []

        for item in items:
            item = item.strip()
            if is_email(item):
                df.loc[idx, 'Email'] = item
            elif is_phone_only(item):
                df.loc[idx, 'Phone'] = item
            elif is_url(item):
                df.loc[idx, 'LinkedIn URL'] = item
            else:
                remaining.append(item)

        df.loc[idx, col_name] = ' | '.join(remaining)

def clea_acc_column(df, column_name):
    df[column_name] = df[column_name].str.strip("")

def phone_clean(df):
       
        for idx, row in df.iterrows():
            value = str(row['Phone']).replace(' ', '') if pd.notna(row['Phone']) else ''
            Location = str(row['Location']).replace(' ', '') if pd.notna(row['Location']) else ''
            if Location == 'US' and value.startswith('1'):
                clean_value = '+' + value
                df.loc[idx, 'Phone'] = clean_value
            elif Location == 'US' and not value.startswith('1') and not value == '':
                clean_value = '+1' + value
                df.loc[idx, 'Phone'] = clean_value
            elif Location not in ['US',] and value.startswith('44'):
                clean_value = '+' + value
                df.loc[idx, 'Phone'] = clean_value
            elif Location not in ['US'] and value.startswith('0'):
                clean_value = value
                df.loc[idx, 'Phone'] = clean_value
            elif Location not in ['US'] and not value.startswith(('44', '0', '+44')):
                clean_value = '0' + value
                df.loc[idx, 'Phone'] = clean_value
            else:
                df.loc[idx, 'Phone'] = value
    



  

clean_column(df, 'Notes')
clean_column(df, 'Date Connected')
clea_acc_column(df, 'Email')
phone_clean(df)


df.to_csv(r"C:\Users\CHtui\Downloads\FinalListCleanedDone.csv", index=False)
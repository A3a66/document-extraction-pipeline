from pathlib import Path
import csv


file_pathh = r"C:\Users\CHtui\Downloads\List of Agencies.csv"

with open(file_pathh , 'r') as f:
    grades = csv.DictReader(f) #maps csv to csv column headers
    for row in grades:
        print(row)


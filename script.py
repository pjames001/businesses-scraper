import os
import re
import json
import csv

input_file = "free_company_dataset.json"
output_dir = "states"

# Create output folder if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

fieldnames = [
    "ID", "Name", "Industry", "Country", "Region", "Locality",
    "Website", "Founded", "Size", "Linkedin URL"
]

def sanitize_filename(name):
    """Clean region names into safe filenames."""
    if not name or not name.strip():
        return "unknown_state"
    clean = re.sub(r'[^\w\s-]', '', name.strip()).strip().lower()
    clean = re.sub(r'[-\s]+', '_', clean)
    return clean or "unknown_state"

# Dictionary to keep open file handles and CSV writers
file_handles = {}
writers = {}

try:
    with open(input_file, "r", encoding="utf-8") as infile:
        for line in infile:
            if not line.strip():
                continue
            try:
                company = json.loads(line)
                
                country = company.get("country", "")
                if country and country.upper() == "UNITED STATES":
                    # Get region (state)
                    region = company.get("region", "")
                    state_key = sanitize_filename(region)
                    
                    # Create a new CSV writer/file for this state if not already open
                    if state_key not in writers:
                        file_path = os.path.join(output_dir, f"{state_key}.csv")
                        f = open(file_path, "w", newline="", encoding="utf-8")
                        writer = csv.writer(f)
                        writer.writerow(fieldnames)
                        file_handles[state_key] = f
                        writers[state_key] = writer
                    
                    # Write company row matching header alignment
                    writers[state_key].writerow([
                        company.get("id"),
                        company.get("name"),
                        company.get("industry"),
                        country,
                        region,
                        company.get("locality"),
                        company.get("website"),
                        company.get("founded"),
                        company.get("size"),
                        company.get("linkedin_url"),
                    ])
            except json.JSONDecodeError:
                continue

finally:
    # Close all open file streams safely
    for f in file_handles.values():
        f.close()

print(f"Done! Check the '{output_dir}/' directory for individual state CSV files.")

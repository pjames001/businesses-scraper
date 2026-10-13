import os
import re
import json
import csv
from collections import Counter
from datetime import date

input_file = "free_company_dataset.json"

today_str = date.today().strftime("%Y-%m-%d")
output_dir = "filtered_b2b_california"
os.makedirs(output_dir, exist_ok=True)

combined_filename = os.path.join(output_dir, f"philip_{today_str}_california_all_b2b.csv")

fieldnames = [
    "ID", "Name", "Matched Category", "Original Industry", "Country", "Region", 
    "Locality", "Website", "Founded", "Size", "Linkedin URL"
]

CATEGORY_PATTERNS = {
    "security": r"\b(security|security systems|security guards|physical security|cybersecurity|alarm systems|surveillance|private investigation|loss prevention|access control|security services)\b",
    "automotive": r"\b(auto|automotive|car dealer|vehicle|motor|dealership|auto repair)\b",
    "medical_supplies_equipment": r"\b(medical supply|medical equipment|dme|medtech|biomedical equipment|surgical supply|healthcare equipment)\b",
    "restaurant_supplies": r"\b(restaurant supply|foodservice equipment|commercial kitchen|food equipment|hospitality supply)\b",
    "wholesale": r"\b(wholesale|wholesaler|distributor|distribution|merchant wholesalers|import export|b2b trade)\b",
    "moving_companies": r"\b(moving|movers|relocation|moving and storage|van line|freight mover)\b",
    "logistics": r"\b(logistics|supply chain|freight|freight forwarding|3pl|transportation management|dispatch)\b",
    "trucking": r"\b(trucking|motor freight|haulage|hauling|drayage|carrier|truck transport)\b",
    "employment_recruitment": r"\b(recruitment|recruiting|headhunting|talent acquisition|executive search|employment agency|job placement)\b",
    "staffing": r"\b(staffing|temp agency|temporary staffing|workforce solutions|staffing agency)\b",
    "plumbing": r"\b(plumbing|plumber|pipefitting|pipe fitter|drainage|sewer service)\b",
    "law_firms": r"\b(law firm|legal services|attorney|lawyer|legal practice|legal consultancy|counsel)\b",
    "solar_systems": r"\b(solar|photovoltaic|solar energy|solar power|clean energy installation|renewable energy contracting)\b",
    "financial_accounting": r"\b(accounting|bookkeeping|cpa|financial services|tax preparation|audit|tax services|wealth management|payroll)\b",
    "pool_services": r"\b(pool service|pool maintenance|pool construction|swimming pool|pool repair)\b",
    "it_and_technology": r"\b(it services|technology|software|information technology|tech consulting|msp|cloud services|network infrastructure)\b",
    "dental_lab": r"\b(dental lab|dental laboratory|prosthodontics lab|dental prosthetics)\b",
    "warehouses": r"\b(warehouse|warehousing|cold storage|fulfillment|storage facility|logistics center)\b",
    "aerospace_defense": r"\b(aerospace|defense|aviation|defense contractor|avionics|military equipment)\b",
    "debt_collection": r"\b(debt collection|collection agency|accounts receivable management|receivables|credit collection)\b",
    "construction": r"\b(construction|general contractor|building contractor|civil engineering|heavy construction|commercial construction|remodeling)\b",
    "cleaning_services": r"\b(janitorial|commercial cleaning|building maintenance|cleaning services|facility services|pressure washing)\b",
    "handymen": r"\b(handyman|home repair|property maintenance|facilities repair|general repair)\b",
    "real_estate": r"\b(real estate|commercial real estate|property management|realty|real estate development|brokerage)\b",
    "pest_control": r"\b(pest control|exterminator|exterminating|pest management|fumigation)\b",
    "hvac": r"\b(hvac|heating|cooling|air conditioning|refrigeration|climate control|heating and air)\b",
}

GOVT_PATTERN = re.compile(
    r"\b(government|gov|state of|city of|county of|department of|ministry|municipality|public safety|embassy|consulate|federal agency|public school|armed forces)\b", 
    re.IGNORECASE
)

EXCLUDED_B2C_PATTERN = re.compile(
    r"\b(barber|hair salon|nail salon|spa|massage|pet grooming|daycare|nursery|beauty salon|gym|fitness studio|apparel retail|shoe store|thrift store)\b",
    re.IGNORECASE
)

def is_california(region_str):
    if not region_str:
        return False
    r = region_str.strip().lower()
    return r in ["ca", "california", "cal", "calif", "calif."]

def classify_industry(industry_str, name_str):
    combined_text = f"{industry_str or ''} {name_str or ''}".strip().lower()
    
    if not combined_text:
        return None

    if GOVT_PATTERN.search(combined_text):
        return None
        
    if EXCLUDED_B2C_PATTERN.search(combined_text):
        return None

    for cat_name, pattern in CATEGORY_PATTERNS.items():
        if re.search(pattern, combined_text, re.IGNORECASE):
            return cat_name

    return None

def sanitize_filename(name):
    return re.sub(r'[-\s]+', '_', name.strip().lower())

# Track counts per category
category_counts = Counter()

file_handles = {}
writers = {}

combined_file = open(combined_filename, "w", newline="", encoding="utf-8")
combined_writer = csv.writer(combined_file)
combined_writer.writerow(fieldnames)

processed_count = 0

try:
    with open(input_file, "r", encoding="utf-8") as infile:
        for line in infile:
            if not line.strip():
                continue
            try:
                company = json.loads(line)
                
                country = company.get("country", "")
                region = company.get("region", "")
                
                if country and country.upper() == "UNITED STATES" and is_california(region):
                    industry_raw = company.get("industry", "")
                    name_raw = company.get("name", "")
                    
                    matched_cat = classify_industry(industry_raw, name_raw)
                    
                    if matched_cat:
                        processed_count += 1
                        category_counts[matched_cat] += 1
                        
                        row_data = [
                            company.get("id"),
                            name_raw,
                            matched_cat.replace("_", " ").title(),
                            industry_raw,
                            country,
                            region,
                            company.get("locality"),
                            company.get("website"),
                            company.get("founded"),
                            company.get("size"),
                            company.get("linkedin_url"),
                        ]
                        
                        combined_writer.writerow(row_data)
                        
                        if matched_cat not in writers:
                            cat_filename = os.path.join(
                                output_dir, f"philip_{today_str}_ca_{sanitize_filename(matched_cat)}.csv"
                            )
                            f = open(cat_filename, "w", newline="", encoding="utf-8")
                            w = csv.writer(f)
                            w.writerow(fieldnames)
                            
                            file_handles[matched_cat] = f
                            writers[matched_cat] = w
                            
                        writers[matched_cat].writerow(row_data)

            except json.JSONDecodeError:
                continue

finally:
    # Append total count summary row to each category CSV file
    for cat_key, w in writers.items():
        count_for_cat = category_counts[cat_key]
        clean_cat_title = cat_key.replace("_", " ").title()
        
        # Write blank line, then total count summary
        w.writerow([])
        w.writerow([
            "SUMMARY", 
            f"Total {clean_cat_title} Records:", 
            f"{count_for_cat:,}", 
            "", "", "", "", "", "", "", ""
        ])
        
    # Append total count summary row to the main combined CSV file
    combined_writer.writerow([])
    combined_writer.writerow([
        "SUMMARY", 
        "Total All California B2B Records:", 
        f"{processed_count:,}", 
        "", "", "", "", "", "", "", ""
    ])

    # Close main file
    combined_file.close()
    
    # Close category file handles
    for f in file_handles.values():
        f.close()

# Print detailed console breakdown
print("\n" + "="*40)
print(f" TOTAL MATCHED CALIFORNIA RECORDS: {processed_count:,}")
print("="*40)
print(" BREAKDOWN BY CATEGORY:")
for cat, count in category_counts.most_common():
    clean_cat = cat.replace("_", " ").title()
    print(f" - {clean_cat:<30}: {count:,} records")
print("="*40 + "\n")

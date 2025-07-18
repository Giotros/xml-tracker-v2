import pandas as pd
import requests
from datetime import datetime
from io import BytesIO
import os
import csv

# --- Ρυθμίσεις ---
XML_URL = "https://acalight.gr/xml/data.xml"
# Ορίζουμε το output να είναι ΕΝΑ ΕΠΙΠΕΔΟ ΠΑΝΩ, στον κεντρικό φάκελο
OUTPUT_DIR = "data"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "history.csv")
FIELDS = ["datetime", "code", "price", "stock"]

# --- Βοηθητικές Συναρτήσεις ---

def safe_float(x):
    try:
        return float(str(x).replace(",", "."))
    except (ValueError, TypeError):
        return 0.0

def fetch_xml():
    print("Fetching XML from URL...")
    # Προσθέτουμε ένα header για να μοιάζουμε με browser
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
    r = requests.get(XML_URL, headers=headers)
    r.raise_for_status()
    print("XML fetched successfully.")
    if not r.content:
        print("Warning: Fetched XML content is empty.")
    return r.content

def calculate_stock_and_price(xml_bytes):
    print("Parsing XML and calculating stock...")
    if not xml_bytes:
        return []
    
    try:
        df = pd.read_xml(BytesIO(xml_bytes), xpath=".//product")
    except ValueError:
        print("Warning: No 'product' tags found in the XML. Returning empty list.")
        return []

    if df.empty:
        print("Warning: DataFrame is empty after parsing. No products found.")
        return []

    rows = []
    for _, r in df.iterrows():
        code = r.get("code")
        price = safe_float(r.get("WholeSalePricegr") or r.get("WholeSalePriceGR") or 0)
        total_stock = 0
        
        for wh, col in [("Serres", "SerresStockStatus"), ("Athens", "AthensStockStatus"), ("Bulgaria", "BgStockStatus")]:
            status = str(r.get(col, "")).strip().lower()
            qty_col = "B2BGreenFromQty" if status == "green" else "B2BOrangeFromQty"
            qty = r.get(qty_col, 0)
            try:
                total_stock += int(qty)
            except (ValueError, TypeError):
                pass
        
        rows.append({
            "datetime": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "code": code,
            "price": price,
            "stock": total_stock
        })
    print(f"Processed {len(rows)} products.")
    return rows

def store_data(data_rows):
    print(f"Ensuring directory exists at {OUTPUT_DIR}...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    file_exists = os.path.isfile(OUTPUT_CSV)
    
    print(f"Storing data to {OUTPUT_CSV}...")
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            print("CSV does not exist. Writing header.")
            writer.writeheader()
        
        if data_rows:
            writer.writerows(data_rows)
            print(f"{len(data_rows)} rows written to CSV.")
        else:
            print("No new data rows to write.")

# --- Κύρια Λειτουργία ---
if __name__ == "__main__":
    try:
        xml_content = fetch_xml()
        product_data = calculate_stock_and_price(xml_content)
        store_data(product_data)
        print("\nAll tasks completed successfully!")
    except requests.exceptions.RequestException as e:
        print(f"\nError: Could not fetch data from URL. {e}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
import pandas as pd
import requests
from datetime import datetime
from io import BytesIO
import os
import csv
import xml.etree.ElementTree as ET

# --- Ρυθμίσεις ---
PRODUCTS_XML_URL = "https://acalight.gr/xml/data.xml"
CATEGORIES_XML_URL = "https://www.acalight.com/cat_attr_gr_uk.xml" # Το νέο link
OUTPUT_DIR = "data"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "history.csv")
# Προσθέσαμε το 'category' στα πεδία
FIELDS = ["datetime", "code", "price", "stock", "category"] 

# --- Βοηθητικές Συναρτήσεις ---

def fetch_xml_content(url):
    """Κατεβάζει το περιεχόμενο ενός XML από ένα URL."""
    print(f"Fetching XML from {url}...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    print("XML fetched successfully.")
    return r.content

def create_category_map(categories_xml_bytes):
    """Δημιουργεί ένα λεξικό {code: category} από το XML των κατηγοριών."""
    print("Creating category map...")
    category_map = {}
    if not categories_xml_bytes:
        return category_map
    
    root = ET.fromstring(categories_xml_bytes)
    # Η δομή είναι <root><product><Code>...</Code><BigCatDescrGR>...</BigCatDescrGR>...</product></root>
    for product in root.findall('product'):
        code = product.find('Code')
        category = product.find('BigCatDescrGR')
        if code is not None and category is not None and code.text:
            category_map[code.text.strip()] = category.text.strip()
    print(f"Category map created with {len(category_map)} entries.")
    return category_map

def process_products(products_xml_bytes, category_map):
    """Επεξεργάζεται τα προϊόντα και τα συνδέει με τις κατηγορίες."""
    print("Parsing products XML and calculating stock...")
    if not products_xml_bytes:
        return []

    try:
        df = pd.read_xml(BytesIO(products_xml_bytes), xpath=".//product")
    except ValueError:
        print("Warning: No 'product' tags found in products XML.")
        return []

    rows = []
    for _, r in df.iterrows():
        code = r.get("code")
        price = float(str(r.get("WholeSalePricegr") or r.get("WholeSalePriceGR") or 0).replace(",", "."))
        total_stock = 0
        
        for col in ["SerresStockStatus", "AthensStockStatus", "BgStockStatus"]:
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
            "stock": total_stock,
            "category": category_map.get(code, "Άγνωστη Κατηγορία") # Αναζήτηση της κατηγορίας
        })
    print(f"Processed {len(rows)} products.")
    return rows

def store_data(data_rows):
    """Αποθηκεύει τα δεδομένα στο αρχείο CSV."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_exists = os.path.isfile(OUTPUT_CSV)
    
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            writer.writeheader()
        if data_rows:
            writer.writerows(data_rows)
    print("Data stored successfully.")

# --- Κύρια Λειτουργία ---
if __name__ == "__main__":
    try:
        categories_xml = fetch_xml_content(CATEGORIES_XML_URL)
        cat_map = create_category_map(categories_xml)
        
        products_xml = fetch_xml_content(PRODUCTS_XML_URL)
        product_data = process_products(products_xml, cat_map)
        
        store_data(product_data)
        print("\nAll tasks completed successfully!")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
import pandas as pd
import requests
from datetime import datetime
from io import BytesIO
import os
import csv
import xml.etree.ElementTree as ET

# --- Ρυθμίσεις ---
# Προσθέσαμε τα URLs για τον νέο προμηθευτή
ACALIGHT_PRODUCTS_URL = "https://acalight.gr/xml/data.xml"
ACALIGHT_CATEGORIES_URL = "https://acalight.gr/xml/cat_attr_gr_uk.xml"
PAKOWORLD_URL = "https://www.pakoworld.com/?route=extension/feed/csxml_feed&token=MTYxMThMUDg0Mw==&lang=el"

OUTPUT_DIR = "data"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "history.csv")
# Προσθέσαμε το πεδίο 'supplier'
FIELDS = ["datetime", "code", "price", "stock", "category", "supplier"]

# --- Συνάρτηση για "Καθάρισμα" Κειμένου ---
def normalize_text(text):
    if not isinstance(text, str): return ""
    text = text.lower()
    replacements = {'ά': 'α', 'έ': 'ε', 'ή': 'η', 'ί': 'ι', 'ό': 'ο', 'ύ': 'υ', 'ώ': 'ω', 'ϊ': 'ι', 'ϋ': 'υ', 'ΐ': 'ι', 'ΰ': 'υ'}
    for accented, unaccented in replacements.items(): text = text.replace(accented, unaccented)
    for p in ".,-/_": text = text.replace(p, " ")
    return text.strip()

KEYWORD_CATEGORIES = {'ανεμιστηρας': 'Ανεμιστήρες', 'φωτιστικο οροφης': 'Φωτιστικά Οροφής', 'spot': 'Φωτιστικά Spot', 'απλικα': 'Απλίκες Τοίχου', 'led': 'Προϊόντα LED', 'ταινια': 'Ταινίες LED'}

# --- Βοηθητικές Συναρτήσεις ---
def fetch_xml_content(url, supplier_name):
    print(f"[{supplier_name}] Fetching XML from {url}...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    print(f"[{supplier_name}] XML fetched successfully.")
    return r.content

# --- Συναρτήσεις για AcaLight ---
def create_acalight_category_map(categories_xml_bytes):
    print("[AcaLight] Creating category map...")
    category_map = {}
    if not categories_xml_bytes: return category_map
    root = ET.fromstring(categories_xml_bytes)
    for product in root.findall('.//product'):
        code = product.find('Code')
        category = product.find('BigCatDescrGR')
        if code is not None and category is not None and code.text:
            category_map[code.text.strip()] = category.text.strip()
    print(f"[AcaLight] Category map created with {len(category_map)} entries.")
    return category_map

def process_acalight_products(products_xml_bytes, category_map):
    print("[AcaLight] Processing products...")
    if not products_xml_bytes: return []
    df = pd.read_xml(BytesIO(products_xml_bytes), xpath=".//product")
    rows = []
    for _, r in df.iterrows():
        code = r.get("code")
        product_name_raw = str(r.get("descr_gr", "")).strip()
        price = float(str(r.get("WholeSalePricegr") or r.get("WholeSalePriceGR") or 0).replace(",", "."))
        total_stock = sum(int(r.get(qty_col, 0) or 0) for status_col in ["SerresStockStatus", "AthensStockStatus", "BgStockStatus"] for qty_col in [("B2BGreenFromQty" if str(r.get(status_col, "")).strip().lower() == "green" else "B2BOrangeFromQty")])

        category = category_map.get(code)
        if not category:
            normalized_name = normalize_text(product_name_raw)
            found_by_keyword = False
            for keyword, cat_name in KEYWORD_CATEGORIES.items():
                if keyword in normalized_name:
                    category = cat_name; found_by_keyword = True; break
            if not found_by_keyword and product_name_raw:
                category = product_name_raw.split()[0].capitalize()
        
        rows.append({"code": code, "price": price, "stock": total_stock, "category": category or "Άγνωστη Κατηγορία", "supplier": "AcaLight"})
    print(f"[AcaLight] Processed {len(rows)} products.")
    return rows

# --- Συνάρτηση για Pakoworld ---
def process_pakoworld_products(xml_bytes):
    print("[Pakoworld] Processing products...")
    if not xml_bytes: return []
    root = ET.fromstring(xml_bytes)
    rows = []
    for product in root.findall('.//product'):
        rows.append({
            "code": product.findtext('model', default='').strip(),
            "price": float(product.findtext('price_wholesale', default='0').replace(",", ".")),
            "stock": int(product.findtext('quantity', default='0')),
            "category": product.findtext('category', default='').strip() or "Άγνωστη Κατηγορία",
            "supplier": "Pakoworld"
        })
    print(f"[Pakoworld] Processed {len(rows)} products.")
    return rows

# --- Συνάρτηση Αποθήκευσης ---
def store_data(data_rows):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_exists = os.path.isfile(OUTPUT_CSV)
    
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            writer.writeheader()
        if data_rows:
            # Προσθέτουμε το datetime σε κάθε σειρά πριν την αποθήκευση
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for row in data_rows:
                row['datetime'] = now_str
            writer.writerows(data_rows)
    print("Data stored successfully.")

# --- Κύρια Λειτουργία ---
if __name__ == "__main__":
    all_products = []
    try:
        # ---- Επεξεργασία AcaLight ----
        acalight_categories_xml = fetch_xml_content(ACALIGHT_CATEGORIES_URL, "AcaLight")
        acalight_cat_map = create_acalight_category_map(acalight_categories_xml)
        acalight_products_xml = fetch_xml_content(ACALIGHT_PRODUCTS_URL, "AcaLight")
        acalight_data = process_acalight_products(acalight_products_xml, acalight_cat_map)
        all_products.extend(acalight_data)
        
        # ---- Επεξεργασία Pakoworld ----
        pakoworld_xml = fetch_xml_content(PAKOWORLD_URL, "Pakoworld")
        pakoworld_data = process_pakoworld_products(pakoworld_xml)
        all_products.extend(pakoworld_data)

        # ---- Αποθήκευση Όλων των Δεδομένων ----
        if all_products:
            store_data(all_products)
            print(f"\nTotal products processed from all suppliers: {len(all_products)}")
            print("All tasks completed successfully!")
        else:
            print("\nNo products found from any supplier.")
            
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

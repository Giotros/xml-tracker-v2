import pandas as pd
import requests
from datetime import datetime
from io import BytesIO
import os
import csv
import xml.etree.ElementTree as ET

# --- Ρυθμίσεις ---
ACALIGHT_PRODUCTS_URL = "https://acalight.gr/xml/data.xml"
ACALIGHT_CATEGORIES_URL = "https://acalight.gr/xml/cat_attr_gr_uk.xml"
PAKOWORLD_URL = "https://www.pakoworld.com/?route=extension/feed/csxml_feed&token=MTYxMThMUDg0Mw==&lang=el"

OUTPUT_DIR = "data"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "history.csv")
FIELDS = ["datetime", "code", "price", "stock", "category", "supplier"]

# (Όλες οι υπόλοιπες συναρτήσεις παραμένουν ακριβώς οι ίδιες... παραλείπονται για συντομία)
# normalize_text, KEYWORD_CATEGORIES, fetch_xml_content, create_acalight_category_map, 
# process_acalight_products, process_pakoworld_products...

# --- Η ΑΝΑΒΑΘΜΙΣΜΕΝΗ ΣΥΝΑΡΤΗΣΗ ΑΠΟΘΗΚΕΥΣΗΣ ---
def store_data(data_rows):
    """
    Αποθηκεύει τα δεδομένα, ελέγχοντας πρώτα τη δομή του αρχείου CSV
    για να αποφύγει ασυνέπειες.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Έλεγχος αν το αρχείο υπάρχει και έχει σωστή δομή
    schema_is_ok = False
    if os.path.isfile(OUTPUT_CSV):
        try:
            with open(OUTPUT_CSV, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                if header == FIELDS:
                    schema_is_ok = True
                else:
                    print("!!! Schema mismatch detected. Archiving old history file.")
        except (StopIteration, csv.Error): # Το αρχείο είναι άδειο ή κατεστραμμένο
            print("!!! History file is empty or corrupt. Will create a new one.")
            schema_is_ok = False

    # 2. Αν η δομή δεν είναι σωστή, αρχειοθετούμε το παλιό αρχείο
    if os.path.isfile(OUTPUT_CSV) and not schema_is_ok:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_name = os.path.join(OUTPUT_DIR, f"history_archived_{timestamp}.csv")
        os.rename(OUTPUT_CSV, archive_name)
        print(f"Old history file archived as: {archive_name}")

    # 3. Αποθηκεύουμε τα νέα δεδομένα
    # Η 'a' (append) mode θα δημιουργήσει νέο αρχείο αν δεν υπάρχει
    file_exists_and_is_ok = os.path.isfile(OUTPUT_CSV) and schema_is_ok
    
    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists_and_is_ok:
            writer.writeheader() # Γράφουμε κεφαλίδα μόνο αν το αρχείο είναι νέο ή διορθώθηκε
        
        if data_rows:
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for row in data_rows:
                row['datetime'] = now_str
            writer.writerows(data_rows)
            
    print("Data stored successfully.")


# (Η υπόλοιπη κύρια λειτουργία if __name__ == "__main__": παραμένει η ίδια)

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

# (Σημείωση: Οι υπόλοιπες συναρτήσεις όπως process_acalight_products, fetch_xml_content κ.λπ. 
# παραμένουν οι ίδιες με τον κώδικα που σου έδωσα προηγουμένως)

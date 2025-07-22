import streamlit as st
import pandas as pd
import os
from datetime import datetime

# --- Page Config & Title ---
st.set_page_config(layout="wide", page_title="Αναφορά Αλλαγών")
st.title("📊 Αναφορά Αλλαγών Αποθέματος & Τιμών")
st.markdown("---")

# --- Data Loading ---
HISTORY_CSV = "data/history.csv" 

@st.cache_data
def load_data():
    """Διαβάζει το κεντρικό CSV αρχείο."""
    if not os.path.exists(HISTORY_CSV):
        return pd.DataFrame()
    df = pd.read_csv(HISTORY_CSV)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["date"] = df["datetime"].dt.date
    return df

df_history = load_data()

if df_history.empty:
    st.warning(f"Δεν βρέθηκαν δεδομένα στο '{HISTORY_CSV}'. Βεβαιωθείτε ότι το GitHub Action έχει τρέξει με επιτυχία.")
    st.stop()

# --- UI: Date Filtering ---
st.subheader("🔍 Επίλεξε Διάστημα για Σύγκριση")
min_date = df_history["date"].min()
today = datetime.now().date()

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Από ημερομηνία", min_date, min_value=min_date, max_value=today)
with col2:
    end_date = st.date_input("Έως ημερομηνία", today, min_value=min_date, max_value=today)

if start_date > end_date:
    st.error("Η ημερομηνία έναρξης πρέπει να είναι πριν την ημερομηνία λήξης.")
    st.stop()

# --- Data Filtering for the selected dates ---
df_start = df_history[df_history["date"] == start_date].drop_duplicates(subset='code', keep='last')
df_end = df_history[df_history["date"] == end_date].drop_duplicates(subset='code', keep='last')

st.markdown("---")

# --- UI: High-Level Metrics ---
st.subheader("Κύρια Στοιχεία Περιόδου")
# ... (Ο κώδικας για τα st.metric παραμένει ίδιος)
col1, col2, col3 = st.columns(3)
total_products_start = df_start.shape[0] if not df_start.empty else 0
total_products_end = df_end.shape[0] if not df_end.empty else 0
with col1:
    st.metric(label=f"Σύνολο Προϊόντων ({end_date})", value=f"{total_products_end:,}", delta=f"{total_products_end - total_products_start:,} από {start_date}")
stock_start = df_start['stock'].sum() if not df_start.empty else 0
stock_end = df_end['stock'].sum() if not df_end.empty else 0
with col2:
    st.metric(label=f"Συνολικό Απόθεμα ({end_date})", value=f"{int(stock_end):,}", delta=f"{int(stock_end - stock_start):,}")
price_start = df_start['price'].mean() if not df_start.empty else 0
price_end = df_end['price'].mean() if not df_end.empty else 0
with col3:
    st.metric(label=f"Μέση Τιμή ({end_date})", value=f"{price_end:.2f} €", delta=f"{price_end - price_start:.2f} €", delta_color="inverse")

st.markdown("---")

# --- Comparison Logic & Display ---
if df_start.empty or df_end.empty:
    st.info("Δεν υπάρχουν δεδομένα για μία από τις δύο ημερομηνίες που επιλέξατε. Παρακαλώ επιλέξτε διαφορετικές ημερομηνίες.")
elif start_date == end_date:
     st.info("Επιλέξτε δύο διαφορετικές ημερομηνίες για να δείτε τη σύγκριση.")
else:
    st.subheader("🚨 Προϊόντα με Αλλαγές")
    
    df_merged = pd.merge(df_start[['code', 'price', 'stock', 'category']], df_end[['code', 'price', 'stock', 'category']], on='code', suffixes=('_start', '_end'), how='inner')
    df_merged['stock_diff'] = df_merged['stock_end'] - df_merged['stock_start']
    df_merged['price_diff'] = (df_merged['price_end'] - df_merged['price_start']).round(2)

    df_changed = df_merged[(df_merged['stock_diff'] != 0) | (df_merged['price_diff'] != 0)].copy()

    if df_changed.empty:
        st.success("✅ Δεν βρέθηκαν αλλαγές στο απόθεμα ή την τιμή για τα κοινά προϊόντα του διαστήματος.")
    else:
        st.write(f"Βρέθηκαν **{len(df_changed)}** προϊόντα με αλλαγές:")

        # Ο πίνακας τώρα περιλαμβάνει και την κατηγορία
        df_display = df_changed[['code', 'category_end', 'stock_end', 'stock_diff', 'price_end', 'price_diff']].rename(columns={
            'code': 'Κωδικός',
            'category_end': 'Κατηγορία',
            'stock_end': 'Τελικό Απόθεμα', # Αυτή είναι η ποσότητα την "Έως ημερομηνία"
            'stock_diff': 'Διαφορά Αποθ.', # Αυτή είναι η αφαίρεση
            'price_end': 'Τελική Τιμή',
            'price_diff': 'Διαφορά Τιμής'
        })
        
        st.dataframe(df_display, use_container_width=True)
        st.markdown("---")
        
        # --- ΝΕΟ: Γράφημα "Hot" Κατηγοριών ---
        st.markdown("##### 🔥 Hot Κατηγορίες (με τις περισσότερες αλλαγές)")
        hot_categories = df_changed['category_end'].value_counts().nlargest(10)
        st.bar_chart(hot_categories)

        # --- Οπτικοποίηση Top 5 Αποθέματος ---
        st.markdown("##### 📈 Top 5 Μεταβολές Αποθέματος")
        col1, col2 = st.columns(2)
        top_increases = df_changed[df_changed['stock_diff'] > 0].nlargest(5, 'stock_diff')
        with col1:
            st.write("Μεγαλύτερη Αύξηση")
            st.bar_chart(top_increases.set_index('code')['stock_diff'])
        top_decreases = df_changed[df_changed['stock_diff'] < 0].nsmallest(5, 'stock_diff')
        with col2:
            st.write("Μεγαλύτερη Μείωση")
            st.bar_chart(top_decreases.set_index('code')['stock_diff'])

# --- Expander for Raw Data ---
with st.expander("🗂️ Προβολή όλων των δεδομένων για το επιλεγμένο διάστημα"):
    df_range = df_history[(df_history["date"] >= start_date) & (df_history["date"] <= end_date)]
    st.dataframe(df_range, use_container_width=True)
# app.py
import streamlit as st
import pandas as pd
import os
from datetime import datetime
import threading
from queue import Queue
import time
from scraper.email_scraper import EmailScraper, format_phone_number
import io
import zipfile

st.set_page_config(page_title="Email Scraper", layout="wide")

# Session state initialization
if 'processing_queue' not in st.session_state:
    st.session_state.processing_queue = Queue()
if 'results' not in st.session_state:
    st.session_state.results = []
if 'processing_status' not in st.session_state:
    st.session_state.processing_status = {}

def process_file(file_data, filename, selected_sheets, row_ranges):
    """Process a single Excel file"""
    file_id = f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    st.session_state.processing_status[file_id] = {
        'status': 'processing',
        'progress': 0,
        'filename': filename
    }
    
    try:
        # Read Excel file
        excel_file = pd.ExcelFile(io.BytesIO(file_data))
        scraper = EmailScraper()
        all_results = []
        
        for sheet_idx in selected_sheets:
            sheet_name = excel_file.sheet_names[sheet_idx]
            df = excel_file.parse(sheet_name)
            
            if 'Website' not in df.columns or 'Title' not in df.columns:
                continue
            
            start_idx, end_idx = row_ranges.get(sheet_idx, (0, len(df)))
            df_subset = df.iloc[start_idx:end_idx] if start_idx or end_idx != len(df) else df
            
            for idx, (index, row) in enumerate(df_subset.iterrows()):
                website = row['Website']
                company = row['Title']
                phone = row.get('Phone Number', '')
                formatted_phone = format_phone_number(phone)
                
                if pd.isna(website) or not website:
                    all_results.append({
                        'Company': company,
                        'Website': 'No website',
                        'Phone Number': formatted_phone,
                        'Email': 'No website provided',
                        'City': sheet_name
                    })
                    continue
                
                scraper.emails = set()
                try:
                    scraper.scrape_page(website, max_depth=2)
                    if scraper.emails:
                        for email in scraper.emails:
                            all_results.append({
                                'Company': company,
                                'Website': website,
                                'Phone Number': formatted_phone,
                                'Email': email,
                                'City': sheet_name
                            })
                    else:
                        all_results.append({
                            'Company': company,
                            'Website': website,
                            'Phone Number': formatted_phone,
                            'Email': 'No email found',
                            'City': sheet_name
                        })
                except Exception as e:
                    all_results.append({
                        'Company': company,
                        'Website': website,
                        'Phone Number': formatted_phone,
                        'Email': f'Error: {str(e)}',
                        'City': sheet_name
                    })
                
                # Update progress
                progress = ((idx + 1) / len(df_subset)) * 100
                st.session_state.processing_status[file_id]['progress'] = progress
        
        # Save results
        if all_results:
            results_df = pd.DataFrame(all_results)
            output_dir = 'outputs'
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"results_{file_id}.xlsx")
            results_df.to_excel(output_path, index=False)
            
            st.session_state.processing_status[file_id]['status'] = 'completed'
            st.session_state.processing_status[file_id]['output_file'] = output_path
            st.session_state.results.append({
                'file_id': file_id,
                'filename': filename,
                'output_path': output_path,
                'timestamp': datetime.now()
            })
    except Exception as e:
        st.session_state.processing_status[file_id]['status'] = 'failed'
        st.session_state.processing_status[file_id]['error'] = str(e)

def worker():
    """Background worker to process files from queue"""
    while True:
        task = st.session_state.processing_queue.get()
        if task is None:
            break
        process_file(**task)
        st.session_state.processing_queue.task_done()

# Main UI
st.title("🔍 Email Scraper Application")
st.markdown("Upload Excel files to extract email addresses from websites")

# Sidebar for configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    max_depth = st.slider("Scraping Depth", 1, 3, 2)
    max_urls_per_domain = st.number_input("Max URLs per Domain", 5, 50, 15)
    concurrent_files = st.number_input("Concurrent Files", 1, 10, 3)

# File upload section
st.header("📤 Upload Files")
uploaded_files = st.file_uploader(
    "Choose Excel files",
    type=['xlsx', 'xls'],
    accept_multiple_files=True
)

if uploaded_files:
    st.subheader(f"📁 {len(uploaded_files)} file(s) uploaded")
    
    for uploaded_file in uploaded_files:
        with st.expander(f"📊 Configure: {uploaded_file.name}"):
            file_data = uploaded_file.getvalue()
            excel_file = pd.ExcelFile(io.BytesIO(file_data))
            
            st.write(f"**Sheets available:** {len(excel_file.sheet_names)}")
            selected_sheets = st.multiselect(
                "Select sheets to process",
                range(len(excel_file.sheet_names)),
                format_func=lambda x: excel_file.sheet_names[x],
                key=f"sheets_{uploaded_file.name}"
            )
            
            row_ranges = {}
            for sheet_idx in selected_sheets:
                df = excel_file.parse(sheet_idx)
                col1, col2 = st.columns(2)
                with col1:
                    start_row = st.number_input(
                        f"Start row (Sheet {excel_file.sheet_names[sheet_idx]})",
                        0, len(df), 0,
                        key=f"start_{uploaded_file.name}_{sheet_idx}"
                    )
                with col2:
                    end_row = st.number_input(
                        f"End row (Sheet {excel_file.sheet_names[sheet_idx]})",
                        start_row, len(df), len(df),
                        key=f"end_{uploaded_file.name}_{sheet_idx}"
                    )
                row_ranges[sheet_idx] = (start_row, end_row)
            
            if st.button(f"🚀 Queue {uploaded_file.name}", key=f"queue_{uploaded_file.name}"):
                if selected_sheets:
                    st.session_state.processing_queue.put({
                        'file_data': file_data,
                        'filename': uploaded_file.name,
                        'selected_sheets': selected_sheets,
                        'row_ranges': row_ranges
                    })
                    st.success(f"✅ {uploaded_file.name} added to queue")
                else:
                    st.error("Please select at least one sheet")
    
    # Start processing button
    if st.button("▶️ Start Processing All"):
        # Start worker threads
        for _ in range(min(concurrent_files, st.session_state.processing_queue.qsize())):
            thread = threading.Thread(target=worker, daemon=True)
            thread.start()
        st.success(f"Processing started with {concurrent_files} concurrent workers")

# Processing status
st.header("📊 Processing Status")
if st.session_state.processing_status:
    for file_id, status in st.session_state.processing_status.items():
        with st.container():
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                st.write(f"**{status['filename']}**")
            with col2:
                if status['status'] == 'processing':
                    st.progress(status['progress'] / 100)
                elif status['status'] == 'completed':
                    st.success("✅ Completed")
                elif status['status'] == 'failed':
                    st.error("❌ Failed")
            with col3:
                st.write(f"{status.get('progress', 0):.1f}%")

# Download section
st.header("📥 Download Results")
if st.session_state.results:
    # Individual downloads
    for result in st.session_state.results:
        if os.path.exists(result['output_path']):
            with open(result['output_path'], 'rb') as f:
                st.download_button(
                    label=f"⬇️ Download {result['filename']}",
                    data=f,
                    file_name=f"scraped_{result['filename']}",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"download_{result['file_id']}"
                )
    
    # Bulk download
    if len(st.session_state.results) > 1:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for result in st.session_state.results:
                if os.path.exists(result['output_path']):
                    zip_file.write(result['output_path'], os.path.basename(result['output_path']))
        
        st.download_button(
            label="📦 Download All Results (ZIP)",
            data=zip_buffer.getvalue(),
            file_name=f"all_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
            mime="application/zip"
        )
else:
    st.info("No results available yet. Upload and process files to see results.")

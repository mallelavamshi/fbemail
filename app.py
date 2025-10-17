# app.py
import streamlit as st
import pandas as pd
import os
from datetime import datetime
import io
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import time

# Import the scraper classes
from get_emails_fb11 import EmailScraper, format_phone_number

st.set_page_config(
    page_title="Email Scraper",
    page_icon="🔍",
    layout="wide"
)

# Initialize session state
if 'processing_results' not in st.session_state:
    st.session_state.processing_results = []
if 'is_processing' not in st.session_state:
    st.session_state.is_processing = False
if 'current_status' not in st.session_state:
    st.session_state.current_status = {}

def process_single_file(uploaded_file, selected_sheets, max_workers=3):
    """Process a single uploaded Excel file"""
    file_id = f"{uploaded_file.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # Read the uploaded file
        file_bytes = uploaded_file.getvalue()
        excel_file = pd.ExcelFile(io.BytesIO(file_bytes))
        
        all_results = []
        scraper = EmailScraper()
        
        for sheet_idx in selected_sheets:
            sheet_name = excel_file.sheet_names[sheet_idx]
            st.session_state.current_status[file_id] = f"Processing sheet: {sheet_name}"
            
            df = excel_file.parse(sheet_name)
            
            # Check for required columns
            if 'Website' not in df.columns or 'Title' not in df.columns:
                st.warning(f"Sheet '{sheet_name}' missing required columns. Skipping...")
                continue
            
            # Process each row
            total_rows = len(df)
            for idx, (_, row) in enumerate(df.iterrows()):
                website = row['Website']
                company = row['Title']
                phone = row.get('Phone Number', '')
                formatted_phone = format_phone_number(phone)
                
                # Update progress
                progress = ((idx + 1) / total_rows) * 100
                st.session_state.current_status[file_id] = f"Processing {sheet_name}: {idx+1}/{total_rows} ({progress:.1f}%)"
                
                # Handle missing websites
                if pd.isna(website) or not website or not isinstance(website, str):
                    all_results.append({
                        'Company': company,
                        'Website': 'No website',
                        'Phone Number': formatted_phone,
                        'Email': 'No website provided',
                        'City': sheet_name
                    })
                    continue
                
                # Check if domain is blocked
                if scraper.is_blocked_domain(website):
                    all_results.append({
                        'Company': company,
                        'Website': website,
                        'Phone Number': formatted_phone,
                        'Email': 'Blocked domain',
                        'City': sheet_name
                    })
                    continue
                
                # Scrape the website
                scraper.emails = set()
                scraper.visited_urls = set()
                
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
                        'Email': f'Error: {str(e)[:50]}',
                        'City': sheet_name
                    })
        
        # Save results
        if all_results:
            results_df = pd.DataFrame(all_results)
            output_dir = 'outputs'
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"results_{file_id}.xlsx")
            results_df.to_excel(output_path, index=False)
            
            st.session_state.current_status[file_id] = "✅ Completed"
            
            return {
                'status': 'success',
                'file_id': file_id,
                'filename': uploaded_file.name,
                'output_path': output_path,
                'total_emails': len(results_df[~results_df['Email'].str.startswith('No')]),
                'total_rows': len(results_df)
            }
        else:
            return {
                'status': 'error',
                'file_id': file_id,
                'filename': uploaded_file.name,
                'error': 'No results found'
            }
            
    except Exception as e:
        st.session_state.current_status[file_id] = f"❌ Failed: {str(e)}"
        return {
            'status': 'error',
            'file_id': file_id,
            'filename': uploaded_file.name,
            'error': str(e)
        }

# Main UI
st.title("🔍 Email Scraper Application")
st.markdown("Upload Excel files to extract email addresses from websites")

# Sidebar configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    max_workers = st.slider("Concurrent Files", 1, 5, 3, 
                            help="Number of files to process simultaneously")
    st.info("💡 **Tip**: Start with 1-2 concurrent files for testing")

# File upload section
st.header("📤 Upload Excel Files")
uploaded_files = st.file_uploader(
    "Choose Excel files (.xlsx or .xls)",
    type=['xlsx', 'xls'],
    accept_multiple_files=True,
    help="Upload one or more Excel files containing Website and Title columns"
)

if uploaded_files:
    st.success(f"✅ {len(uploaded_files)} file(s) uploaded")
    
    # File configuration
    file_configs = {}
    
    for uploaded_file in uploaded_files:
        with st.expander(f"📊 Configure: {uploaded_file.name}"):
            try:
                # Read sheet names
                file_bytes = uploaded_file.getvalue()
                excel_file = pd.ExcelFile(io.BytesIO(file_bytes))
                
                st.write(f"**Available sheets:** {len(excel_file.sheet_names)}")
                
                # Sheet selection
                selected_sheets = st.multiselect(
                    "Select sheets to process",
                    range(len(excel_file.sheet_names)),
                    format_func=lambda x: excel_file.sheet_names[x],
                    default=list(range(min(3, len(excel_file.sheet_names)))),
                    key=f"sheets_{uploaded_file.name}"
                )
                
                if selected_sheets:
                    file_configs[uploaded_file.name] = {
                        'file': uploaded_file,
                        'sheets': selected_sheets
                    }
                    st.info(f"Will process {len(selected_sheets)} sheet(s)")
                else:
                    st.warning("⚠️ Please select at least one sheet")
                    
            except Exception as e:
                st.error(f"Error reading file: {str(e)}")
    
    # Process button
    if file_configs:
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            start_button = st.button("🚀 Start Processing", type="primary", use_container_width=True)
        with col2:
            if st.session_state.is_processing:
                st.warning("⏳ Processing...")
        
        if start_button and not st.session_state.is_processing:
            st.session_state.is_processing = True
            st.session_state.processing_results = []
            st.session_state.current_status = {}
            
            # Create progress placeholders
            progress_container = st.container()
            status_placeholder = st.empty()
            
            with progress_container:
                st.subheader("📊 Processing Status")
                progress_bar = st.progress(0)
                status_text = st.empty()
            
            # Process files
            completed = 0
            total_files = len(file_configs)
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                
                for filename, config in file_configs.items():
                    future = executor.submit(
                        process_single_file,
                        config['file'],
                        config['sheets'],
                        max_workers
                    )
                    futures[future] = filename
                
                # Monitor progress
                for future in as_completed(futures):
                    filename = futures[future]
                    result = future.result()
                    
                    st.session_state.processing_results.append(result)
                    completed += 1
                    
                    # Update progress
                    progress_bar.progress(completed / total_files)
                    status_text.text(f"Completed {completed}/{total_files} files")
                    
                    # Show individual file status
                    if result['status'] == 'success':
                        st.success(f"✅ {filename}: Found {result['total_emails']} emails")
                    else:
                        st.error(f"❌ {filename}: {result.get('error', 'Failed')}")
            
            st.session_state.is_processing = False
            st.balloons()

# Results section
if st.session_state.processing_results:
    st.header("📥 Download Results")
    
    successful_results = [r for r in st.session_state.processing_results if r['status'] == 'success']
    
    if successful_results:
        # Summary statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Files Processed", len(successful_results))
        with col2:
            total_emails = sum(r['total_emails'] for r in successful_results)
            st.metric("Total Emails Found", total_emails)
        with col3:
            total_rows = sum(r['total_rows'] for r in successful_results)
            st.metric("Total Rows Processed", total_rows)
        
        st.divider()
        
        # Individual file downloads
        for result in successful_results:
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.write(f"**{result['filename']}**")
                st.caption(f"Emails: {result['total_emails']} | Rows: {result['total_rows']}")
            
            with col2:
                if os.path.exists(result['output_path']):
                    with open(result['output_path'], 'rb') as f:
                        st.download_button(
                            label="⬇️ Download",
                            data=f,
                            file_name=f"scraped_{result['filename']}",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"download_{result['file_id']}"
                        )
        
        # Bulk download
        if len(successful_results) > 1:
            st.divider()
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for result in successful_results:
                    if os.path.exists(result['output_path']):
                        zip_file.write(
                            result['output_path'],
                            os.path.basename(result['output_path'])
                        )
            
            st.download_button(
                label="📦 Download All Results (ZIP)",
                data=zip_buffer.getvalue(),
                file_name=f"all_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                mime="application/zip",
                use_container_width=True
            )
    else:
        st.warning("No successful results to download")
else:
    st.info("👆 Upload files and click 'Start Processing' to begin")

# Footer
st.divider()
st.caption("💡 Make sure your Excel files have 'Website' and 'Title' columns")

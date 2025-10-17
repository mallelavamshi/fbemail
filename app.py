# app.py - WORKING VERSION
import streamlit as st
import pandas as pd
import os
from datetime import datetime
import io
import zipfile

# Import the scraper
from get_emails_fb11 import EmailScraper, format_phone_number

st.set_page_config(
    page_title="Email Scraper",
    page_icon="🔍",
    layout="wide"
)

# Initialize session state properly
if 'results' not in st.session_state:
    st.session_state.results = []

def process_file(file_bytes, filename, selected_sheets):
    """Process a single Excel file - NO THREADING"""
    file_id = f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # Read Excel
        excel_file = pd.ExcelFile(io.BytesIO(file_bytes))
        scraper = EmailScraper()
        all_results = []
        
        # Create progress placeholders
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        total_sheets = len(selected_sheets)
        
        for sheet_num, sheet_idx in enumerate(selected_sheets):
            sheet_name = excel_file.sheet_names[sheet_idx]
            status_text.info(f"📄 Processing sheet: {sheet_name}")
            
            df = excel_file.parse(sheet_name)
            
            # Check required columns
            if 'Website' not in df.columns or 'Title' not in df.columns:
                st.warning(f"⚠️ Sheet '{sheet_name}' missing required columns. Skipping...")
                continue
            
            # Process each row
            total_rows = len(df)
            
            for idx, (_, row) in enumerate(df.iterrows()):
                # Update progress
                overall_progress = (sheet_num + (idx + 1) / total_rows) / total_sheets
                progress_bar.progress(overall_progress)
                status_text.info(f"📄 {sheet_name}: Row {idx+1}/{total_rows}")
                
                website = row['Website']
                company = row['Title']
                phone = row.get('Phone Number', '')
                formatted_phone = format_phone_number(phone)
                
                # Handle missing website
                if pd.isna(website) or not website or not isinstance(website, str):
                    all_results.append({
                        'Company': company,
                        'Website': 'No website',
                        'Phone Number': formatted_phone,
                        'Email': 'No website provided',
                        'City': sheet_name
                    })
                    continue
                
                # Check blocked domains
                if scraper.is_blocked_domain(website):
                    all_results.append({
                        'Company': company,
                        'Website': website,
                        'Phone Number': formatted_phone,
                        'Email': 'Blocked domain',
                        'City': sheet_name
                    })
                    continue
                
                # Scrape website
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
        
        progress_bar.progress(1.0)
        status_text.success("✅ Processing complete!")
        
        # Save results
        if all_results:
            results_df = pd.DataFrame(all_results)
            output_dir = 'outputs'
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"results_{file_id}.xlsx")
            results_df.to_excel(output_path, index=False)
            
            return {
                'status': 'success',
                'file_id': file_id,
                'filename': filename,
                'output_path': output_path,
                'total_emails': len([r for r in all_results if not r['Email'].startswith(('No', 'Error', 'Blocked'))]),
                'total_rows': len(results_df)
            }
        else:
            return {
                'status': 'error',
                'filename': filename,
                'error': 'No results found'
            }
            
    except Exception as e:
        st.error(f"❌ Error processing file: {str(e)}")
        return {
            'status': 'error',
            'filename': filename,
            'error': str(e)
        }

# Main UI
st.title("🔍 Email Scraper Application")
st.markdown("Upload Excel files to extract email addresses from websites")

# Instructions
with st.expander("ℹ️ How to Use"):
    st.markdown("""
    1. Upload one or more Excel files (.xlsx or .xls)
    2. Select which sheets to process
    3. Click "Start Processing"
    4. Download results when complete
    
    **Required columns in Excel:**
    - `Website`: URL to scrape
    - `Title`: Company name
    - `Phone Number` (optional)
    """)

# File upload
st.header("📤 Upload Excel Files")
uploaded_files = st.file_uploader(
    "Choose Excel files",
    type=['xlsx', 'xls'],
    accept_multiple_files=True,
    help="Upload Excel files containing Website and Title columns"
)

if uploaded_files:
    st.success(f"✅ {len(uploaded_files)} file(s) uploaded")
    
    # Process files one by one
    for uploaded_file in uploaded_files:
        with st.expander(f"📊 {uploaded_file.name}", expanded=True):
            try:
                # Read file
                file_bytes = uploaded_file.getvalue()
                excel_file = pd.ExcelFile(io.BytesIO(file_bytes))
                
                st.write(f"**Sheets found:** {len(excel_file.sheet_names)}")
                
                # Sheet selection
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    selected_sheets = st.multiselect(
                        "Select sheets to process",
                        range(len(excel_file.sheet_names)),
                        format_func=lambda x: excel_file.sheet_names[x],
                        default=list(range(min(3, len(excel_file.sheet_names)))),
                        key=f"sheets_{uploaded_file.name}"
                    )
                
                with col2:
                    process_button = st.button(
                        "🚀 Process",
                        key=f"process_{uploaded_file.name}",
                        use_container_width=True,
                        type="primary"
                    )
                
                if process_button and selected_sheets:
                    with st.spinner(f"Processing {uploaded_file.name}..."):
                        result = process_file(file_bytes, uploaded_file.name, selected_sheets)
                        
                        if result['status'] == 'success':
                            st.session_state.results.append(result)
                            st.success(f"✅ Complete! Found {result['total_emails']} emails in {result['total_rows']} rows")
                        else:
                            st.error(f"❌ Failed: {result.get('error', 'Unknown error')}")
                
                elif process_button and not selected_sheets:
                    st.warning("⚠️ Please select at least one sheet")
                    
            except Exception as e:
                st.error(f"Error reading file: {str(e)}")

# Results section
if st.session_state.results:
    st.divider()
    st.header("📥 Download Results")
    
    # Summary
    total_emails = sum(r['total_emails'] for r in st.session_state.results if r['status'] == 'success')
    total_files = len([r for r in st.session_state.results if r['status'] == 'success'])
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Files Processed", total_files)
    with col2:
        st.metric("Total Emails Found", total_emails)
    
    st.divider()
    
    # Individual downloads
    for result in st.session_state.results:
        if result['status'] == 'success' and os.path.exists(result['output_path']):
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.write(f"**{result['filename']}**")
                st.caption(f"📧 {result['total_emails']} emails | 📄 {result['total_rows']} rows")
            
            with col2:
                with open(result['output_path'], 'rb') as f:
                    st.download_button(
                        label="⬇️ Download",
                        data=f,
                        file_name=f"scraped_{result['filename']}",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"download_{result['file_id']}",
                        use_container_width=True
                    )
    
    # Bulk download
    if total_files > 1:
        st.divider()
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for result in st.session_state.results:
                if result['status'] == 'success' and os.path.exists(result['output_path']):
                    zip_file.write(result['output_path'], os.path.basename(result['output_path']))
        
        st.download_button(
            label="📦 Download All Results (ZIP)",
            data=zip_buffer.getvalue(),
            file_name=f"all_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
            mime="application/zip",
            use_container_width=True
        )
    
    # Clear results button
    if st.button("🗑️ Clear Results"):
        st.session_state.results = []
        st.rerun()

else:
    st.info("👆 Upload files and click 'Process' to begin scraping")

# Footer
st.divider()
st.caption("Made with ❤️ using Streamlit | Email Scraper v1.0")

# app.py
import streamlit as st
import pandas as pd
import os
from datetime import datetime
import io
import shutil
from jobs import JobManager, JobStatus

st.set_page_config(
    page_title="Email Scraper",
    page_icon="🔍",
    layout="wide"
)

job_manager = JobManager()

st.title("🔍 Email Scraper Application")
st.markdown("Upload files, queue processing jobs, and download results - **works even after closing browser!**")

# Create tabs
tab1, tab2, tab3, tab4 = st.tabs(["📤 Upload Files", "🎯 Queue Jobs", "📊 Job Status", "📥 Download Results"])

# Tab 1: Upload Files
with tab1:
    st.header("Upload Excel Files")
    st.info("📁 Files are saved on the server and can be processed later")
    
    uploaded_files = st.file_uploader(
        "Choose Excel files",
        type=['xlsx', 'xls'],
        accept_multiple_files=True,
        help="Upload multiple files at once"
    )
    
    if uploaded_files:
        if st.button("💾 Save Files to Server", type="primary"):
            saved_count = 0
            for uploaded_file in uploaded_files:
                filepath = os.path.join(job_manager.uploads_dir, uploaded_file.name)
                with open(filepath, 'wb') as f:
                    f.write(uploaded_file.getbuffer())
                saved_count += 1
            st.success(f"✅ Saved {saved_count} file(s) to server!")
            st.rerun()

# Tab 2: Queue Jobs
with tab2:
    st.header("Select Files for Processing")
    
    uploaded_files = job_manager.get_uploaded_files()
    
    if not uploaded_files:
        st.warning("⚠️ No files uploaded yet. Go to 'Upload Files' tab first.")
    else:
        st.success(f"📁 {len(uploaded_files)} file(s) available on server")
        
        for file_info in uploaded_files:
            with st.expander(f"📊 {file_info['filename']} ({file_info['size'] / 1024:.1f} KB)"):
                try:
                    excel_file = pd.ExcelFile(file_info['filepath'])
                    
                    st.write(f"**Sheets:** {len(excel_file.sheet_names)}")
                    
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        selected_sheets = st.multiselect(
                            "Select sheets to process",
                            range(len(excel_file.sheet_names)),
                            format_func=lambda x: excel_file.sheet_names[x],
                            default=list(range(min(3, len(excel_file.sheet_names)))),
                            key=f"sheets_{file_info['filename']}"
                        )
                    
                    with col2:
                        if st.button("🚀 Queue Job", key=f"queue_{file_info['filename']}", use_container_width=True):
                            if selected_sheets:
                                job_id = job_manager.create_job(
                                    file_info['filename'],
                                    selected_sheets
                                )
                                st.success(f"✅ Job queued! ID: {job_id[:8]}...")
                                st.rerun()
                            else:
                                st.error("Please select at least one sheet")
                    
                    # Delete file button
                    if st.button("🗑️ Delete File", key=f"delete_{file_info['filename']}"):
                        os.remove(file_info['filepath'])
                        st.success("Deleted!")
                        st.rerun()
                        
                except Exception as e:
                    st.error(f"Error reading file: {str(e)}")

# Tab 3: Job Status
with tab3:
    st.header("Processing Jobs")
    
    # Auto-refresh
    if st.button("🔄 Refresh Status"):
        st.rerun()
    
    all_jobs = job_manager.get_all_jobs()
    
    if not all_jobs:
        st.info("No jobs yet. Queue a job from the 'Queue Jobs' tab.")
    else:
        # Filter by status
        status_filter = st.multiselect(
            "Filter by status",
            [JobStatus.PENDING.value, JobStatus.PROCESSING.value, 
             JobStatus.COMPLETED.value, JobStatus.FAILED.value],
            default=[JobStatus.PENDING.value, JobStatus.PROCESSING.value]
        )
        
        filtered_jobs = [j for j in all_jobs if j['status'] in status_filter]
        
        for job in filtered_jobs:
            status_emoji = {
                'pending': '⏳',
                'processing': '🔄',
                'completed': '✅',
                'failed': '❌'
            }
            
            emoji = status_emoji.get(job['status'], '❓')
            
            with st.expander(f"{emoji} {job['filename']} - {job['status'].upper()}", 
                           expanded=job['status'] == 'processing'):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Status", job['status'].upper())
                with col2:
                    st.metric("Progress", f"{job['progress']:.1f}%")
                with col3:
                    if job['status'] == 'completed':
                        st.metric("Emails Found", job['total_emails'])
                
                if job['current_sheet']:
                    st.info(f"📄 Processing: {job['current_sheet']}")
                
                st.progress(job['progress'] / 100)
                
                st.caption(f"Created: {job['created_at']}")
                if job['completed_at']:
                    st.caption(f"Completed: {job['completed_at']}")
                if job['error']:
                    st.error(f"Error: {job['error']}")

# Tab 4: Download Results
with tab4:
    st.header("Download Processed Files")
    
    completed_jobs = [j for j in job_manager.get_all_jobs() 
                     if j['status'] == JobStatus.COMPLETED.value]
    
    if not completed_jobs:
        st.info("No completed jobs yet.")
    else:
        st.success(f"✅ {len(completed_jobs)} file(s) ready for download")
        
        # Summary
        total_emails = sum(j['total_emails'] for j in completed_jobs)
        st.metric("Total Emails Extracted", total_emails)
        
        st.divider()
        
        for job in completed_jobs:
            output_path = os.path.join(job_manager.outputs_dir, job['output_file'])
            
            if os.path.exists(output_path):
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.write(f"**{job['output_file']}**")
                    st.caption(f"📧 {job['total_emails']} emails | 📄 {job['total_rows']} rows | ✅ {job['completed_at']}")
                
                with col2:
                    with open(output_path, 'rb') as f:
                        st.download_button(
                            label="⬇️ Download",
                            data=f,
                            file_name=job['output_file'],
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"download_{job['job_id']}",
                            use_container_width=True
                        )

# Footer
st.divider()
st.caption("💡 Background worker processes jobs continuously - safe to close browser!")

# Add auto-refresh for active jobs
active_jobs = [j for j in job_manager.get_all_jobs() 
              if j['status'] in ['pending', 'processing']]
if active_jobs and 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = datetime.now()
    # Auto-refresh every 10 seconds if there are active jobs
    st.markdown('<meta http-equiv="refresh" content="10">', unsafe_allow_html=True)

# app.py - Enhanced with no auto-refresh and tile views
import streamlit as st
import pandas as pd
import os
from datetime import datetime
import io
from jobs import JobManager, JobStatus, JobControl
import time  # <--- ADD THIS LINE

st.set_page_config(
    page_title="Email Scraper Pro",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Remove auto-refresh
if 'disable_refresh' not in st.session_state:
    st.session_state.disable_refresh = True

job_manager = JobManager()

# Custom CSS for tiles
st.markdown("""
<style>
    .file-tile {
        padding: 20px;
        border-radius: 10px;
        border: 2px solid #e0e0e0;
        margin: 10px 0;
        background: #f9f9f9;
    }
    .file-tile:hover {
        border-color: #4CAF50;
        background: #f0f7f0;
    }
    .status-badge {
        padding: 5px 10px;
        border-radius: 5px;
        font-size: 12px;
        font-weight: bold;
    }
    .status-processing { background: #2196F3; color: white; }
    .status-completed { background: #4CAF50; color: white; }
    .status-failed { background: #f44336; color: white; }
    .status-paused { background: #FF9800; color: white; }
    .status-stopped { background: #9E9E9E; color: white; }
    .status-pending { background: #FFC107; color: black; }
</style>
""", unsafe_allow_html=True)

st.title("🔍 Email Scraper Pro")
st.markdown("**Async processing with pause/stop controls**")

# Sidebar
with st.sidebar:
    st.header("⚙️ Quick Stats")
    uploaded_count = len(job_manager.get_uploaded_files())
    output_count = len(job_manager.get_output_files())
    active_jobs = len([j for j in job_manager.get_all_jobs() 
                      if j['status'] in ['processing', 'pending']])
    
    st.metric("📁 Uploaded Files", uploaded_count)
    st.metric("📤 Output Files", output_count)
    st.metric("⚡ Active Jobs", active_jobs)
    
    st.divider()
    if st.button("🔄 Refresh Dashboard", use_container_width=True):
        st.rerun()

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["📤 Upload Files", "🎯 Manage Jobs", "📊 Job Status", "📥 Downloads"])

# TAB 1: Upload Files (Tile View)
with tab1:
    st.header("Upload Excel Files")
    
    # Upload section
    with st.container():
        uploaded_files = st.file_uploader(
            "Choose Excel files",
            type=['xlsx', 'xls'],
            accept_multiple_files=True,
            help="Upload multiple files - they will be saved permanently",
            key="file_uploader"
        )
        
        if uploaded_files:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.info(f"📁 {len(uploaded_files)} file(s) selected")
            with col2:
                if st.button("💾 Save to Server", type="primary", use_container_width=True):
                    progress_bar = st.progress(0)
                    for idx, file in enumerate(uploaded_files):
                        filepath = os.path.join(job_manager.uploads_dir, file.name)
                        with open(filepath, 'wb') as f:
                            f.write(file.getbuffer())
                        progress_bar.progress((idx + 1) / len(uploaded_files))
                    st.success(f"✅ Saved {len(uploaded_files)} file(s)!")
                    time.sleep(1)
                    st.rerun()
    
    st.divider()
    
    # Display uploaded files as tiles
    st.subheader("📁 Saved Files")
    uploaded_files = job_manager.get_uploaded_files()
    
    if not uploaded_files:
        st.info("No files uploaded yet")
    else:
        # Display in grid (3 columns)
        cols_per_row = 3
        for i in range(0, len(uploaded_files), cols_per_row):
            cols = st.columns(cols_per_row)
            for j, col in enumerate(cols):
                if i + j < len(uploaded_files):
                    file_info = uploaded_files[i + j]
                    with col:
                        with st.container():
                            st.markdown(f"""
                            <div class="file-tile">
                                <h4>📄 {file_info['filename']}</h4>
                                <p><strong>Size:</strong> {file_info['size'] / 1024:.1f} KB</p>
                                <p><strong>Uploaded:</strong> {file_info['uploaded_at'][:10]}</p>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            col_a, col_b = st.columns(2)
                            with col_a:
                                if st.button("🎯 Queue", key=f"queue_tile_{file_info['filename']}", use_container_width=True):
                                    st.session_state[f"queue_modal_{file_info['filename']}"] = True
                                    st.rerun()
                            with col_b:
                                if st.button("🗑️ Delete", key=f"del_up_{file_info['filename']}", use_container_width=True):
                                    job_manager.delete_uploaded_file(file_info['filename'])
                                    st.success("Deleted!")
                                    time.sleep(0.5)
                                    st.rerun()
                            
                            # Queue modal
                            if st.session_state.get(f"queue_modal_{file_info['filename']}", False):
                                with st.expander("Select Sheets", expanded=True):
                                    excel_file = pd.ExcelFile(file_info['filepath'])
                                    selected_sheets = st.multiselect(
                                        "Sheets to process",
                                        range(len(excel_file.sheet_names)),
                                        format_func=lambda x: excel_file.sheet_names[x],
                                        default=list(range(min(3, len(excel_file.sheet_names)))),
                                        key=f"sheets_modal_{file_info['filename']}"
                                    )
                                    if st.button("✅ Create Job", key=f"create_job_{file_info['filename']}"):
                                        if selected_sheets:
                                            job_id = job_manager.create_job(file_info['filename'], selected_sheets)
                                            st.success(f"Job created: {job_id[:8]}")
                                            st.session_state[f"queue_modal_{file_info['filename']}"] = False
                                            time.sleep(1)
                                            st.rerun()

# TAB 2: Manage Jobs
with tab2:
    st.header("Manage Processing Jobs")
    
    all_jobs = job_manager.get_all_jobs()
    
    if not all_jobs:
        st.info("No jobs created yet")
    else:
        for job in all_jobs:
            with st.expander(f"{job['filename']} - {job['status'].upper()}", expanded=job['status'] == 'processing'):
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Status", job['status'].upper())
                with col2:
                    st.metric("Progress", f"{job['progress']:.1f}%")
                with col3:
                    if job['status'] == 'completed':
                        st.metric("Emails", job['total_emails'])
                    else:
                        st.metric("Current Row", job.get('current_row', 0))
                with col4:
                    st.metric("Total Rows", job.get('total_rows', 0))
                
                st.progress(job['progress'] / 100)
                
                if job['current_sheet']:
                    st.info(f"📄 Current: {job['current_sheet']}")
                
                # Control buttons
                st.divider()
                col_a, col_b, col_c, col_d = st.columns(4)
                
                with col_a:
                    if job['status'] in ['processing', 'paused'] and st.button(
                        "⏸️ Pause" if job['status'] == 'processing' else "▶️ Resume",
                        key=f"pause_{job['job_id']}",
                        use_container_width=True
                    ):
                        new_control = JobControl.PAUSE if job['status'] == 'processing' else JobControl.RUN
                        job_manager.set_job_control(job['job_id'], new_control)
                        st.success("Control signal sent!")
                        time.sleep(0.5)
                        st.rerun()
                
                with col_b:
                    if job['status'] in ['processing', 'paused'] and st.button(
                        "⏹️ Stop",
                        key=f"stop_{job['job_id']}",
                        use_container_width=True
                    ):
                        job_manager.set_job_control(job['job_id'], JobControl.STOP)
                        st.warning("Stop signal sent! Results will be saved.")
                        time.sleep(0.5)
                        st.rerun()
                
                with col_c:
                    if job['status'] in ['completed', 'failed', 'stopped'] and st.button(
                        "🗑️ Delete Job",
                        key=f"del_job_{job['job_id']}",
                        use_container_width=True
                    ):
                        job_manager.delete_job(job['job_id'])
                        st.success("Job deleted!")
                        time.sleep(0.5)
                        st.rerun()
                
                # Show error if failed
                if job['error']:
                    st.error(f"Error: {job['error']}")

# TAB 3: Job Status (same as tab 2 but different view)
with tab3:
    st.header("Job Status Overview")
    
    status_filter = st.multiselect(
        "Filter by status",
        ['pending', 'processing', 'paused', 'stopped', 'completed', 'failed'],
        default=['pending', 'processing', 'paused']
    )
    
    filtered_jobs = [j for j in job_manager.get_all_jobs() if j['status'] in status_filter]
    
    if filtered_jobs:
        for job in filtered_jobs:
            status_class = f"status-{job['status']}"
            st.markdown(f"""
            <div class="file-tile">
                <h4>📊 {job['filename']}</h4>
                <span class="{status_class} status-badge">{job['status'].upper()}</span>
                <p>Progress: {job['progress']:.1f}% | Emails: {job.get('total_emails', 0)}</p>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No jobs matching filter")

# TAB 4: Downloads (Tile View)
with tab4:
    st.header("Download Processed Files")
    
    output_files = job_manager.get_output_files()
    
    if not output_files:
        st.info("No output files yet")
    else:
        st.success(f"✅ {len(output_files)} file(s) ready")
        
        # Display in grid
        cols_per_row = 3
        for i in range(0, len(output_files), cols_per_row):
            cols = st.columns(cols_per_row)
            for j, col in enumerate(cols):
                if i + j < len(output_files):
                    file_info = output_files[i + j]
                    with col:
                        st.markdown(f"""
                        <div class="file-tile">
                            <h4>📤 {file_info['filename']}</h4>
                            <p><strong>Size:</strong> {file_info['size'] / 1024:.1f} KB</p>
                            <p><strong>Created:</strong> {file_info['created_at'][:10]}</p>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        col_x, col_y = st.columns(2)
                        with col_x:
                            with open(file_info['filepath'], 'rb') as f:
                                st.download_button(
                                    "⬇️ Download",
                                    data=f,
                                    file_name=file_info['filename'],
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key=f"dl_{file_info['filename']}",
                                    use_container_width=True
                                )
                        with col_y:
                            if st.button("🗑️ Delete", key=f"del_out_{file_info['filename']}", use_container_width=True):
                                job_manager.delete_output_file(file_info['filename'])
                                st.success("Deleted!")
                                time.sleep(0.5)
                                st.rerun()

st.divider()
st.caption("⚡ Powered by Async Python | Background processing active")

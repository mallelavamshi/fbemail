# worker.py
import time
import pandas as pd
import io
from jobs import JobManager, JobStatus
from get_emails_fb11 import EmailScraper, format_phone_number
import os

def process_job(job_id):
    """Process a single job"""
    job_manager = JobManager()
    job = job_manager.get_job(job_id)
    
    if not job:
        return
    
    try:
        # Update to processing
        job_manager.update_job(job_id, {
            'status': JobStatus.PROCESSING.value,
            'started_at': pd.Timestamp.now().isoformat()
        })
        
        # Read the uploaded file
        filepath = os.path.join(job_manager.uploads_dir, job['filename'])
        excel_file = pd.ExcelFile(filepath)
        
        scraper = EmailScraper()
        all_results = []
        
        total_sheets = len(job['selected_sheets'])
        
        for sheet_num, sheet_idx in enumerate(job['selected_sheets']):
            sheet_name = excel_file.sheet_names[sheet_idx]
            
            # Update progress
            job_manager.update_job(job_id, {
                'current_sheet': sheet_name,
                'progress': (sheet_num / total_sheets) * 100
            })
            
            df = excel_file.parse(sheet_name)
            
            if 'Website' not in df.columns or 'Title' not in df.columns:
                continue
            
            total_rows = len(df)
            
            for idx, (_, row) in enumerate(df.iterrows()):
                website = row['Website']
                company = row['Title']
                phone = row.get('Phone Number', '')
                formatted_phone = format_phone_number(phone)
                
                # Update progress
                row_progress = (sheet_num + (idx + 1) / total_rows) / total_sheets * 100
                job_manager.update_job(job_id, {'progress': row_progress})
                
                if pd.isna(website) or not website or not isinstance(website, str):
                    all_results.append({
                        'Company': company,
                        'Website': 'No website',
                        'Phone Number': formatted_phone,
                        'Email': 'No website provided',
                        'City': sheet_name
                    })
                    continue
                
                if scraper.is_blocked_domain(website):
                    all_results.append({
                        'Company': company,
                        'Website': website,
                        'Phone Number': formatted_phone,
                        'Email': 'Blocked domain',
                        'City': sheet_name
                    })
                    continue
                
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
            output_filename = f"scraped_{job['filename']}"
            output_path = os.path.join(job_manager.outputs_dir, output_filename)
            results_df.to_excel(output_path, index=False)
            
            total_emails = len([r for r in all_results 
                              if not r['Email'].startswith(('No', 'Error', 'Blocked'))])
            
            job_manager.update_job(job_id, {
                'status': JobStatus.COMPLETED.value,
                'completed_at': pd.Timestamp.now().isoformat(),
                'progress': 100,
                'total_emails': total_emails,
                'total_rows': len(results_df),
                'output_file': output_filename
            })
        else:
            job_manager.update_job(job_id, {
                'status': JobStatus.FAILED.value,
                'completed_at': pd.Timestamp.now().isoformat(),
                'error': 'No results found'
            })
            
    except Exception as e:
        job_manager.update_job(job_id, {
            'status': JobStatus.FAILED.value,
            'completed_at': pd.Timestamp.now().isoformat(),
            'error': str(e)
        })

def worker_loop():
    """Main worker loop - runs continuously"""
    job_manager = JobManager()
    print("🚀 Worker started - waiting for jobs...")
    
    while True:
        try:
            pending_jobs = job_manager.get_pending_jobs()
            
            if pending_jobs:
                job = pending_jobs[0]  # Process oldest pending job
                print(f"📝 Processing job {job['job_id']} - {job['filename']}")
                process_job(job['job_id'])
                print(f"✅ Completed job {job['job_id']}")
            else:
                time.sleep(5)  # Wait 5 seconds before checking again
                
        except Exception as e:
            print(f"❌ Worker error: {str(e)}")
            time.sleep(10)

if __name__ == "__main__":
    worker_loop()

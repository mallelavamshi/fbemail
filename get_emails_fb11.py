import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import random
import os
from datetime import datetime

class EmailScraper:
    def __init__(self):
        self.visited_urls = set()
        self.emails = set()
        self.scraped_domains = {}  # Track domains and number of URLs scraped
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        self.blocked_domains = [
            'estatesales.net',
            'estatesales.org',
            'godaddy.com',
            'hibid.com',
            'bluemoonestatesales.com',
            'galleryauctions.com'
        ]

        self.MAX_URLS_PER_DOMAIN = 15
        self.unwanted_patterns = [
            'wix', 'example', 'domain', 'sentry',
            'webp', 'jpg', 'png'
        ]

    def is_blocked_domain(self, url):
        """Check if URL belongs to blocked domains"""
        if pd.isna(url) or not url or not isinstance(url, str):
            return False
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc.lower()
            domain = domain.replace('www.', '')
            return domain in self.blocked_domains
        except Exception:
            return False

    def get_domain(self, url):
        """Extract domain from URL"""
        try:
            parsed_url = urlparse(url)
            return parsed_url.netloc.lower().replace('www.', '')
        except Exception:
            return None

    def can_scrape_domain(self, url):
        """Check if we can scrape more URLs from this domain"""
        domain = self.get_domain(url)
        if not domain:
            return False
        return self.scraped_domains.get(domain, 0) < self.MAX_URLS_PER_DOMAIN

    def extract_emails(self, text):
        """Extract email addresses from text and filter unwanted ones"""
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        found_emails = set(re.findall(email_pattern, text))

        # Filter out emails containing unwanted patterns
        filtered_emails = set()
        for email in found_emails:
            if not any(pattern.lower() in email.lower() for pattern in self.unwanted_patterns):
                filtered_emails.add(email)

        return filtered_emails

    def should_skip_url(self, url):
        """Check if URL should be skipped based on extension or other criteria"""
        skip_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.pdf', '.doc', '.docx',
                          '.xlsx', '.xls', '.zip', '.rar', '.mp4', '.avi', '.mov'}
        try:
            parsed_url = urlparse(url)
            path = parsed_url.path.lower()
            return any(path.endswith(ext) for ext in skip_extensions)
        except Exception:
            return True

    def get_internal_links(self, soup, base_url):
        """Get all internal links from a page"""
        internal_links = set()
        domain = urlparse(base_url).netloc
        base_domain = domain.lower().replace('www.', '')

        for link in soup.find_all('a', href=True):
            url = urljoin(base_url, link['href'])

            if self.should_skip_url(url):
                continue

            try:
                url_domain = urlparse(url).netloc.lower().replace('www.', '')
                if url_domain == base_domain:
                    internal_links.add(url)
            except Exception:
                continue

            if not self.can_scrape_domain(url):
                break

        return internal_links

    def scrape_page(self, url, max_depth=2, current_depth=0):
        """Scrape a page and its internal links up to max_depth"""
        if current_depth > max_depth or url in self.visited_urls:
            return

        if (self.is_blocked_domain(url) or 
            self.should_skip_url(url) or 
            not self.can_scrape_domain(url)):
            return

        if not self.is_allowed_by_robots(url):
            print(f"Skipping {url} (not allowed by robots.txt)")
            return

        self.visited_urls.add(url)
        domain = self.get_domain(url)
        if domain:
            self.scraped_domains[domain] = self.scraped_domains.get(domain, 0) + 1

        try:
            time.sleep(random.uniform(1, 3))
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()

            print(f"Scraping: {url} ({self.scraped_domains.get(domain, 0)}/{self.MAX_URLS_PER_DOMAIN} URLs for this domain)")

            soup = BeautifulSoup(response.text, 'html.parser')
            emails = self.extract_emails(response.text)
            self.emails.update(emails)

            if current_depth < max_depth and self.can_scrape_domain(url):
                internal_links = self.get_internal_links(soup, url)
                for link in internal_links:
                    if self.can_scrape_domain(link):
                        self.scrape_page(link, max_depth, current_depth + 1)

        except Exception as e:
            print(f"Error scraping {url}: {str(e)}")

    def is_allowed_by_robots(self, url):
        """Check if URL is allowed by robots.txt"""
        try:
            rp = RobotFileParser()
            parsed_url = urlparse(url)
            robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
            rp.set_url(robots_url)
            rp.read()
            return rp.can_fetch(self.headers['User-Agent'], url)
        except:
            return True


def format_phone_number(phone):
    """Format phone number to +1XXXXXXXXXX format"""
    if pd.isna(phone):
        return ""

    cleaned = str(phone)
    cleaned = re.sub(r'[\(\)\-\s\.]', '', cleaned)
    cleaned = cleaned.replace('+1', '')
    cleaned = re.sub(r'^[^\d]+|[^\d]+$', '', cleaned)

    if len(cleaned) == 10 and cleaned.isdigit():
        return f"+1{cleaned}"

    return cleaned


def get_excel_file_path():
    """Get file path from user via terminal input"""
    while True:
        file_path = input("\nPlease enter the path to your Excel file: ").strip()
        file_path = file_path.strip('"\'')

        if os.path.exists(file_path):
            if file_path.endswith(('.xlsx', '.xls')):
                return file_path
            else:
                print("Error: File must be an Excel file (.xlsx or .xls)")
        else:
            print("Error: File not found. Please enter a valid path")


def get_sheet_selection(total_sheets):
    """Prompt user to select which sheets to scrape"""
    while True:
        print(f"\nThere are {total_sheets} sheets in the Excel file.")
        print("Enter the sheet numbers you want to scrape (comma-separated)")
        print("Example: 1,2,3 will scrape first three sheets")
        print("Enter 'all' to scrape all sheets")

        selection = input("Sheet numbers to scrape: ").strip().lower()

        if selection == 'all':
            return list(range(total_sheets))

        try:
            selected_sheets = [int(x.strip()) - 1 for x in selection.split(',')]
            invalid_sheets = [x + 1 for x in selected_sheets if x < 0 or x >= total_sheets]

            if invalid_sheets:
                print(f"Invalid sheet numbers: {invalid_sheets}")
                print(f"Please enter numbers between 1 and {total_sheets}")
                continue

            selected_sheets = list(dict.fromkeys(selected_sheets))
            return selected_sheets

        except ValueError:
            print("Invalid input. Please enter numbers separated by commas.")


def get_row_range(total_rows):
    """Prompt user to select row range to process"""
    while True:
        print(f"\nTotal rows in sheet: {total_rows}")
        print("Enter the row range you want to extract (e.g., 200-350)")
        print("Enter 'all' to process all rows")
        print("Note: Row 1 is the header row, data starts from row 2")

        selection = input("Row range (e.g., 200-350): ").strip().lower()

        if selection == 'all':
            return 0, total_rows

        try:
            if '-' in selection:
                start, end = selection.split('-')
                start_row = int(start.strip())
                end_row = int(end.strip())

                # Validate row numbers
                if start_row < 1:
                    print(f"Error: Start row must be at least 1")
                    continue

                if end_row > total_rows:
                    print(f"Error: End row cannot exceed {total_rows}")
                    continue

                if start_row > end_row:
                    print(f"Error: Start row ({start_row}) cannot be greater than end row ({end_row})")
                    continue

                # Convert to 0-indexed (accounting for header)
                # User input row 2 = index 0 in dataframe (after header)
                return start_row - 2, end_row - 1
            else:
                print("Invalid format. Please use format like: 200-350")

        except ValueError:
            print("Invalid input. Please enter numbers in format: 200-350")


def create_output_directory():
    """Create output directory structure"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_base = os.path.join(script_dir, "email_scraping_results")

    if not os.path.exists(output_base):
        os.makedirs(output_base)
        print(f"Created output directory: {output_base}")

    date_str = datetime.now().strftime("%Y-%m-%d")
    date_dir = os.path.join(output_base, date_str)

    if not os.path.exists(date_dir):
        os.makedirs(date_dir)
        print(f"Created date directory: {date_dir}")

    return date_dir


def generate_unique_filename(output_dir, source_file, sheet_names, row_range=None):
    """Generate unique filename based on source file name and sheets"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    source_name = os.path.splitext(os.path.basename(source_file))[0]

    if len(sheet_names) <= 3:
        sheet_str = '_'.join(sheet_names)
    else:
        sheet_str = f"{sheet_names[0]}_{sheet_names[1]}_and_{len(sheet_names)-2}_more"

    sheet_str = re.sub(r'[^\w\-_]', '_', sheet_str)

    if row_range:
        range_str = f"_rows{row_range[0]+2}to{row_range[1]+1}"
    else:
        range_str = ""

    base_filename = f"{source_name}_{sheet_str}{range_str}_{timestamp}.xlsx"
    return os.path.join(output_dir, base_filename)


def main():
    print("Email Scraper Script")
    print("===================")
    print("The script will create a folder 'email_scraping_results' in the current directory")
    print("All output files will be saved there in date-based subfolders")

    file_path = get_excel_file_path()
    print(f"\nSelected file: {file_path}")

    output_dir = create_output_directory()

    xl = pd.ExcelFile(file_path)
    selected_sheet_indices = get_sheet_selection(len(xl.sheet_names))
    selected_sheet_names = [xl.sheet_names[i] for i in selected_sheet_indices]

    print("\nSelected sheets to scrape:")
    for idx, sheet_name in zip(selected_sheet_indices, selected_sheet_names):
        print(f"Sheet {idx + 1}: {sheet_name}")

    all_results = []
    scraper = EmailScraper()

    for sheet_idx in selected_sheet_indices:
        sheet_name = xl.sheet_names[sheet_idx]
        print(f"\nProcessing sheet {sheet_idx + 1}: {sheet_name}")

        df = xl.parse(sheet_name)

        if 'Website' not in df.columns or 'Title' not in df.columns:
            print(f"Warning: Sheet '{sheet_name}' missing required columns (Website, Title). Skipping...")
            continue

        # Get row range for this sheet
        total_rows = len(df) + 1  # +1 to account for header in user's perspective
        start_idx, end_idx = get_row_range(total_rows)

        # Slice the dataframe
        if start_idx == 0 and end_idx == total_rows:
            df_subset = df
            print(f"\nProcessing all {len(df)} rows")
        else:
            df_subset = df.iloc[start_idx:end_idx]
            print(f"\nProcessing rows {start_idx+2} to {end_idx+1} ({len(df_subset)} rows)")

        proceed = input("\nProceed with scraping these rows? (y/n): ").strip().lower()
        if proceed != 'y':
            print("Skipping this sheet.")
            continue

        for original_idx, (index, row) in enumerate(df_subset.iterrows(), start=start_idx+2):
            website = row['Website']
            company = row['Title']
            phone = row.get('Phone Number', '')
            formatted_phone = format_phone_number(phone)

            print(f"\n[Row {original_idx}] Processing {company}: {website}")

            if pd.isna(website) or not website or not isinstance(website, str):
                print(f"Invalid website URL for {company}: {website}")
                all_results.append({
                    'Row Number': original_idx,
                    'Company': company,
                    'Website': website if not pd.isna(website) else "No website provided",
                    'Phone Number': formatted_phone,
                    'Email': 'No website provided',
                    'City': sheet_name
                })
                continue

            elif scraper.is_blocked_domain(website):
                print(f"Skipping blocked domain: {website}")
                all_results.append({
                    'Row Number': original_idx,
                    'Company': company,
                    'Website': website,
                    'Phone Number': formatted_phone,
                    'Email': 'Blocked domain',
                    'City': sheet_name
                })
                continue

            scraper.emails = set()

            try:
                scraper.scrape_page(website, max_depth=2)

                if scraper.emails:
                    for email in scraper.emails:
                        all_results.append({
                            'Row Number': original_idx,
                            'Company': company,
                            'Website': website,
                            'Phone Number': formatted_phone,
                            'Email': email,
                            'City': sheet_name
                        })
                else:
                    all_results.append({
                        'Row Number': original_idx,
                        'Company': company,
                        'Website': website,
                        'Phone Number': formatted_phone,
                        'Email': 'No email found',
                        'City': sheet_name
                    })

            except Exception as e:
                print(f"Error processing {website}: {str(e)}")
                all_results.append({
                    'Row Number': original_idx,
                    'Company': company,
                    'Website': website,
                    'Phone Number': formatted_phone,
                    'Email': f'Error: {str(e)}',
                    'City': sheet_name
                })

    if all_results:
        results_df = pd.DataFrame(all_results)
        results_df = results_df.drop_duplicates()

        column_order = ['Row Number', 'Company', 'Phone Number', 'Website', 'Email', 'City']
        results_df = results_df[column_order]

        output_filename = generate_unique_filename(output_dir, file_path, selected_sheet_names, 
                                                   (start_idx, end_idx) if start_idx != 0 or end_idx != total_rows else None)

        results_df.to_excel(output_filename, index=False)
        print(f"\nScraping completed. Results saved to '{output_filename}'")

        summary = {
            'Timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'Input File': os.path.basename(file_path),
            'Sheets Processed': ', '.join(selected_sheet_names),
            'Total Sheets Selected': len(selected_sheet_names),
            'Rows Processed': len(results_df['Row Number'].unique()),
            'Total Websites Processed': len(set(results_df['Website'])),
            'Total Emails Found': len(results_df[~results_df['Email'].isin(['No email found', 'No website provided', 'Blocked domain']) & 
                                                 ~results_df['Email'].str.startswith('Error:')]),
            'Domains Scraped': len(scraper.scraped_domains)
        }

        summary_df = pd.DataFrame([summary])
        source_name = os.path.splitext(os.path.basename(file_path))[0]
        summary_filename = os.path.join(output_dir, f"{source_name}_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")

        summary_df.to_excel(summary_filename, index=False)
        print(f"Summary saved to '{summary_filename}'")

    else:
        print("\nNo results found to save.")


if __name__ == "__main__":
    main()

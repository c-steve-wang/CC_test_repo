import datetime
import requests
from urllib.parse import urljoin

# Configuration
warc_paths_file_path = 'warc.paths'  # Path to the warc.paths file
start_date = datetime.date(2016, 9, 9)         # Start date of the range
end_date = datetime.date(2016, 9, 15)          # End date of the range
base_url = 'https://data.commoncrawl.org/'     # Base URL for Common Crawl data

# Function to parse date from filename
def parse_date_from_filename(filename):
    # Extract the date part from the filename
    parts = filename.split('/')
    date_part = parts[-1].split('-')[2]
    date_str = date_part[:8]  # Get the date part from the timestamp
    return datetime.datetime.strptime(date_str, '%Y%m%d').date()

# Function to download a file
def download_file(url, target_filename):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(target_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f'Downloaded {target_filename}')
    else:
        print(f'Failed to download {url}')

# Read the warc.paths file and download files in the date range
with open(warc_paths_file_path, 'r') as file:
    for line in file:
        filename = line.strip()
        file_date = parse_date_from_filename(filename)

        # Check if the file's date falls within the specified range
        if start_date <= file_date <= end_date:
            file_url = urljoin(base_url, filename)
            target_filename = filename.split('/')[-1]  # Extract the filename from the path
            print(f'Starting download of {file_url}...')
            download_file(file_url, target_filename)

print('Download process completed.')
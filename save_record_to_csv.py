from warcio.archiveiterator import ArchiveIterator
import gzip
from bs4 import BeautifulSoup
import csv
import time
import os

# Configuration
data_folder = './filtered_data'  # Folder containing the filtered WARC files
output_csv_path = '2016_09_12_p.csv'  # Path for the output CSV file
keywords = "Peru earthquake"

def extract_text(html_content):
    """Extract text from HTML content using BeautifulSoup."""
    soup = BeautifulSoup(html_content, 'lxml')
    text = soup.get_text(separator=' ', strip=True)
    return text

def filter_records(data_folder, output_csv_path, keywords):
    start_time = time.time()  # Start timing
    total_count = 0  # Initialize a counter for processed records across all files

    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Date', 'URL', 'Text snippet'])  # CSV Header

        # Process each WARC file in the data_folder
        for filename in os.listdir(data_folder):
            if filename.endswith(".warc.gz"):
                input_warc_path = os.path.join(data_folder, filename)
                print(f'Starting to process {input_warc_path}...')

                count = 0  # Initialize a counter for processed records in the current file
                with gzip.open(input_warc_path, 'rb') as stream:
                    for record in ArchiveIterator(stream):
                        if record.rec_type == 'response':
                            # Extract URL and date
                            url = record.rec_headers.get_header('WARC-Target-URI')
                            date = record.rec_headers.get_header('WARC-Date')
                            # Extract content and convert to text
                            content = record.content_stream().read()
                            text = extract_text(content)

                            # Check if the keywords are in the text
                            if keywords.lower() in text.lower():
                                # Write the filtered record to the CSV file
                                writer.writerow([date, url, text[:1000]])  # Writing a snippet of the text

                            count += 1  # Increment the counter
                            total_count += 1  # Increment the total counter

                            # Print progress every 100 records (adjust as needed)
                            if count % 100 == 0:
                                print(f'Processed {count} records in {filename}...')

                print(f'Completed processing {filename}. Total records processed in file: {count}')

    end_time = time.time()  # End timing
    print(f'Overall process completed in {end_time - start_time:.2f} seconds. Total records processed across all files: {total_count}.')

# Process all WARC files in the data_folder and save filtered records to a CSV file
filter_records(data_folder, output_csv_path, keywords)
from warcio import WARCWriter
from warcio.archiveiterator import ArchiveIterator
import gzip
import time
import os

# Configuration
data_folder = './data'  # Folder containing the WARC files
output_folder = './filtered_data'  # Folder to save the filtered WARC files

# Ensure the output folder exists
os.makedirs(output_folder, exist_ok=True)

# Function to process WARC records and filter out HTML content
def filter_html_records(input_warc_path, output_warc_path):
    start_time = time.time()  # Start timing
    count = 0  # Initialize a counter for processed records

    with open(input_warc_path, 'rb') as stream_in:
        # Open the output file in binary write mode and use gzip compression
        with gzip.open(output_warc_path, 'wb') as stream_out:
            # Create a WARC writer for the output file
            writer = WARCWriter(stream_out, gzip=True)

            for record in ArchiveIterator(stream_in):
                # Increment the counter for each record
                count += 1

                # We only want to process response records
                if record.rec_type == 'response':
                    content_type = record.http_headers.get_header('Content-Type')
                    # Check if the Content-Type header exists and indicates HTML content
                    if content_type and 'text/html' in content_type:
                        # Write the record to the new WARC file
                        writer.write_record(record)

                # Print progress every 100 records (adjust as needed)
                if count % 100 == 0:
                    print(f'Processed {count} records...')

    end_time = time.time()  # End timing
    print(f'Process completed in {end_time - start_time:.2f} seconds. Total records processed: {count}.')

# Iterate over all WARC files in the data folder and filter them
for filename in os.listdir(data_folder):
    if filename.endswith(".warc.gz"):
        input_warc_path = os.path.join(data_folder, filename)
        output_warc_path = os.path.join(output_folder, f'filtered_{filename}')  # Save filtered files to the output folder
        print(f'Starting to filter {input_warc_path}...')
        filter_html_records(input_warc_path, output_warc_path)
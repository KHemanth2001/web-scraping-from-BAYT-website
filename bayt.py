import os.path
import requests
import csv
import time
from bs4 import BeautifulSoup
import logging
from mtranslate import translate
from random import uniform
from datetime import datetime, timedelta

# Create a logger object
logger = logging.getLogger(__name__)

# Define user agent headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

# Define constants file paths and directories
dir_path = os.path.abspath(os.path.dirname(__file__))
folder_name = "_Output01"
file_name = 'Bayt_Saudi_Arabia_Job_Data.csv'
translated_file_name = 'Bayt_Saudi_Arabia_Translated_Job_Data.csv'
path = os.path.join(dir_path, folder_name)

# Create the output directory if it doesn't exist
try:
    os.makedirs(path, exist_ok=True)
except OSError as error:
    logger.error(f"Error occurred while creating directory: {str(error)}")

# Generate a random time delay between 1 and 2 seconds
time_delay = uniform(1, 2)


def convert_relative_date_to_dd_mm_yyyy(relative_date):
    '''This function is used to convert the date into dd-mm-yyyy format.'''
    today = datetime.now()

    if relative_date == "Today":
        return today.strftime('%d-%m-%Y')
    elif relative_date == "Yesterday":
        yesterday = today - timedelta(days=1)
        return yesterday.strftime('%d-%m-%Y')
    elif "days ago" in relative_date:
        days_ago = int(relative_date.split()[0])
        target_date = today - timedelta(days=days_ago)
        return target_date.strftime('%d-%m-%Y')

    return relative_date  # Return as is if it doesn't match the expected formats


def translate_to_english(input_csv_file, output_csv_file, job_name, company_name):
    '''This function translate all the Arabic text into English from file'''

    with open(input_csv_file, 'r', newline='', encoding='utf-8-sig') as input_file, \
            open(output_csv_file, 'w', newline='', encoding='utf-8-sig') as output_file:
        reader = csv.DictReader(input_file)
        fieldnames = reader.fieldnames

        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            text1 = row[job_name]
            text2 = row[company_name]

            # Check if the text in both columns is in English
            if not text1.isascii():
                # Translate the text in job name to English and replace it in the same column
                try:
                    translated_text1 = translate(text1, 'en')
                    row[job_name] = translated_text1
                except Exception as e:
                    logger.error(f"Translation error for {job_name}: {str(e)}")

            if not text2.isascii():
                # Translate the text in company name to English and replace it in the same column
                try:
                    translated_text2 = translate(text2, 'en')
                    row[company_name] = translated_text2  # Update the column with translated text
                except Exception as e:
                    logger.error(f"Translation error for {company_name}: {str(e)}")
            # Write the updated row to the output CSV file
            writer.writerow(row)

    print(f"Translation complete. Translated data has been saved to '{output_csv_file}'.")


def fetch_failed_job_data(failed_job_ids):
    ''' Reprocess the job IDs that encountered errors and retrieve their corresponding details '''

    all_failed_data = []

    for job_id in failed_job_ids:
        result = fetch_data_for_job_id(job_id)
        all_failed_data.append(result)

    return all_failed_data


def goto_next_page(url, page, last_page_content=None):
    ''' Proceed to subsequent pages until arriving at the final page of the website '''

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }
        response = requests.get(url + f'?page={page}', headers=headers, timeout=30)

        if response.status_code == 200:
            content = response.content
            if content == last_page_content:
                print(f"Reached the last page. Stopping data fetching.")
                return None, True
            return content, False
        elif response.status_code == 502:
            print(f"Bad Gateway error (502) occurred while fetching data from {url} (Page: {page}). Retrying...")
            logger.error(f"Bad Gateway error (502) occurred while fetching data from {url} (Page: {page}). Retrying...")
        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 5))
            logger.error(f"Rate limited. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
        elif response.status_code == 404:
            print(f"Reached the last page. Stopping data fetching.")
            return None, True
        else:
            print(f"Failed to retrieve data from {url} (Page: {page}). Status code: {response.status_code}")
            logger.error(f"Failed to retrieve data from {url} (Page: {page}). Status code: {response.status_code}")

        time.sleep(time_delay)
        return goto_next_page(url, page, last_page_content=last_page_content)
    except requests.Timeout:
        print(f"Request timed out while fetching data from {url} (Page: {page}). Retrying...")
        logger.error(f"Request timed out while fetching data from {url} (Page: {page}). Retrying...")
        time.sleep(time_delay)
        return goto_next_page(url, page, last_page_content=last_page_content)

    except (requests.ConnectionError, requests.exceptions.RequestException) as e:
        if "RemoteDisconnected('Remote end closed connection without response')" in str(e):
            print(f"Connection aborted error occurred. Retrying...")
            logger.error(f"Connection aborted error occurred. Retrying...")
        else:
            print(f"Connection error occurred while fetching data from {url} (Page: {page}). Retrying...")
            logger.error(f"Connection error occurred while fetching data from {url} (Page: {page}). Retrying...")

        time.sleep(time_delay)
        return goto_next_page(url, page, last_page_content=last_page_content)

    except Exception as e:
        logger.error(f"Error occurred while fetching data from {url} (Page: {page}). {str(e)}")
        logger.error(f"Error occurred while fetching data from {url} (Page: {page}). {str(e)}")
        return None, False


def fetch_job_ids(url):
    ''' This function retrieves all the job IDs '''

    try:
        all_job_ids = set()
        page = 1
        prev_page_content = None

        while True:
            response, last_page_reached = goto_next_page(url, page, last_page_content=prev_page_content)

            if response is not None:
                soup = BeautifulSoup(response, 'html.parser')
                job_elements = soup.find_all('li', class_='has-pointer-d')
                if not job_elements:
                    print("No job IDs found.")
                    break

                job_ids = {job_element.get("data-job-id") for job_element in job_elements}
                all_job_ids.update(job_ids)

                if last_page_reached:
                    break

                prev_page_content = response
                page += 1
                time.sleep(time_delay)
            else:
                print(f"Failed to fetch data from page {page}. Exiting the loop.")
                break

        return list(all_job_ids)

    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        return []


def fetch_data_for_job_id(job_id, retries=3, backoff_factor=2):
    ''' This function is to fetch all job details based on the job IDs '''

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        url = f'https://www.bayt.com/en/job/{job_id}/'
        with requests.Session() as session:
            response = session.get(url, headers=headers, timeout=30)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser', from_encoding='utf-8')
            details_desc_mapping = {}

            job_name_element = soup.find('h1', class_='h3')
            job_name = job_name_element.text.strip() if job_name_element else ''
            details_desc_mapping['Job ID'] = job_id
            details_desc_mapping['Job Name'] = job_name

            company_element = soup.find('ul', class_='list is-basic t-small')
            if company_element:
                company_name_element = company_element.find('a', class_='is-black')
                company_name = company_name_element.text.strip() if company_name_element else ''
                details_desc_mapping['Company Name'] = company_name
                date_element = company_element.find('li', class_='t-mute')
                if date_element:
                    date_span = date_element.find('span')
                    if date_span:
                        date = date_span.text.strip()
                        if not date.startswith('30+'):
                            date = convert_relative_date_to_dd_mm_yyyy(date)
                            details_desc_mapping['Date'] = date
                    else:
                        details_desc_mapping['Date'] = None  # Skip storing the 'Date'

            job_elements = soup.find_all('dl', class_='dlist is-spaced is-fitted t-small')

            for job_element in job_elements:
                job_attributes = job_element.find_all('dt')
                job_desc = job_element.find_all('dd')

                for title, data in zip(job_attributes, job_desc):
                    title_name = title.text.strip()
                    data_text = data.text.strip()
                    details_desc_mapping[title_name] = data_text

            return details_desc_mapping
        elif response.status_code == 429 and retries > 0:
            retry_after = int(response.headers.get('Retry-After', 5))
            logger.error(f"Rate limited. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
            return fetch_data_for_job_id(job_id, retries - 1, backoff_factor * 2)

        elif response.status_code == 404:
            print(f"Job ID not found: {job_id}")
            logger.error(f"Job ID not found: {job_id}")
            return None

        logger.error(f"Failed to retrieve data for Job ID: {job_id}. Status code: {response.status_code}")
        return {}

    except requests.Timeout:
        print(f"Request timed out while fetching data for Job ID: {job_id}. Retrying...")
        logger.error(f"Request timed out while fetching data for Job ID: {job_id}. Retrying...")
        time.sleep(time_delay)
        return fetch_data_for_job_id(job_id, retries - 1, backoff_factor * 2)

    except (requests.ConnectionError, requests.exceptions.RequestException) as e:
        if "RemoteDisconnected('Remote end closed connection without response')" in str(e):
            print(f"Connection aborted error occurred. Retrying...")
            logger.error(f"Connection aborted error occurred. Retrying...")
        else:
            print(f"Connection error occurred while fetching data for Job ID: {job_id}. Retrying...")
            logger.error(f"Connection error occurred while fetching data for Job ID: {job_id}. Retrying...")

        time.sleep(time_delay)
        return fetch_data_for_job_id(job_id, retries - 1, backoff_factor * 2)

    except Exception as e:
        logger.error(f"Error occurred while fetching data for Job ID: {job_id}. {str(e)}")
        return {}


def remove_empty_rows_from_csv(file_path):
    try:
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as csvfile:
            rows = [row for row in csv.reader(csvfile) if any(field.strip() for field in row)]

        with open(file_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(rows)

    except Exception as e:
        print(f"Error occurred while removing empty rows: {str(e)}")

def main():
    ''' Main synchronous function to enable the execution '''

    # Set up logging for any errors during execution
    log_directory = os.path.join(dir_path, folder_name)
    # Create log file to save all error logs
    log_filename = os.path.join(log_directory, 'Bayt_log.log')

    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=log_filename, level=logging.ERROR, format=log_format)

    url = 'https://www.bayt.com/en/saudi-arabia/jobs/'
    job_ids = fetch_job_ids(url)

    if job_ids:
        all_data = []
        failed_job_ids = []
        field_names = set()
        prev_page_content = None

        for idx, job_id in enumerate(job_ids):
            result = fetch_data_for_job_id(job_id)
            if result is not None:  # Check if result is not None
                all_data.append(result)
                field_names.update(result.keys())
            else:
                failed_job_ids.append(job_id)

            print(f"Processed {idx + 1} of {len(job_ids)} job IDs")

        # Check if there's valid data before saving to CSV
        if all_data:
            for details_desc_mapping in all_data:
                for field_name in field_names:
                    if field_name not in details_desc_mapping:
                        details_desc_mapping[field_name] = ''

            # Fetch data for failed job IDs
            failed_data = fetch_failed_job_data(failed_job_ids)
            all_data.extend(failed_data)
            csv_path = os.path.join(path, file_name)

            try:
                with open(csv_path, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    field_names = ['Job ID', 'Job Name'] + sorted(field_names - {'Job ID', 'Job Name'})
                    writer = csv.DictWriter(csvfile, fieldnames=field_names)
                    writer.writeheader()
                    writer.writerows(all_data)
                    # remove empty rows in the csv file
                    remove_empty_rows_from_csv(csv_path)
                print(f"Data has been successfully saved to '{csv_path}'")
            except Exception as e:
                logger.error(f"Error occurred while saving to CSV: {str(e)}")
        else:
            print("No valid data to save to the CSV file.")

        translated_csv_path = os.path.join(path, translated_file_name)

        translate_to_english(csv_path, translated_csv_path, 'Job Name', 'Company Name')
        remove_empty_rows_from_csv(translated_csv_path)


if __name__ == '__main__':
    ''' Initiate the synchronous 'main' function to fetch all job data and save in csv file '''

    main()
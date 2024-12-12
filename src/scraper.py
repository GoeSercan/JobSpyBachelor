import os
import random
import time
import json
from datetime import datetime, timedelta
from config import (
    COUNTRIES, # Dictionary containing countries categorized for Indeed and Glassdoor
    SEARCH_TERMS, # List of search terms to query job listings
    BRD_USER,
    BRD_ZONE,
    BRD_PASSWD,
    BRD_SUPERPROXY,
    CA_CERT_PATH, # Path to the SSL certificate for proxy verification
    LOG_FILE,
    OFFSET_FILE, # Path to the JSON file storing offsets for search terms
    DAILY_JOB_COUNT_FILE,
    RESULTS_PER_PAGE_OPTIONS,
    NO_RESULT_THRESHOLD,
    MAX_RUNS,  # Number of scraping runs in the loop
    INTERVAL_SECONDS,  # Time interval (in seconds) between consecutive scraping runs
)

from proxy import get_proxy_url, test_proxy, validate_proxy_config
from src.jobspy.scrapers.utils import load_offsets, save_offsets, connect_db, insert_unique_job_data, log_to_file
from src.jobspy import scrape_jobs

# Total jobs added to the db
jobs_added = 0
# Jobs added in the current 24-hour period
daily_job_count = 0  #ToDo fix issue of having more jobs than actual found ones within a 24 hour period in DAILY_JOB_COUNT_FILE - currently getting average
last_log_date = None


def initialize_missing_files():
    """Ensure required JSON files are present or initialize them."""
    default_files = {
        "offsets.json": {},
        "daily_job_counts.json": []
    }
    for file, default_content in default_files.items():
        if not os.path.exists(file):
            print(f"{file} not found. Initializing...")
            with open(file, "w") as f:
                json.dump(default_content, f, indent=4)
            print("Missing files initialized successfully.")

def validate_config():
    """Validate essential configurations and paths."""
    required_env_vars = ["BRD_USER", "BRD_ZONE", "BRD_PASSWD", "BRD_SUPERPROXY", "CA_CERT_PATH"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        raise EnvironmentError(f"Missing environment variables: {', '.join(missing_vars)}")

    required_files = ["countries.json", "search_terms.json", "offsets.json", "daily_job_counts.json"]
    for file in required_files:
        if not os.path.exists(file):
            if file == "offsets.json":  # Allow offsets to initialize if missing
                print(f"{file} not found, initializing with default offsets...")
                continue
            raise FileNotFoundError(f"Required file not found: {file}")

    print("Configuration validation passed.")


def scrape_all_sites_and_countries():
    """Main function to scrape job data across all sites and countries."""
    global jobs_added, daily_job_count
    print("Initializing scraping process...")  # Debug

    try:
        # Load offsets to resume scraping from the last saved state
        print(f"Loading offsets from {OFFSET_FILE}...")
        offsets = load_offsets(OFFSET_FILE, SEARCH_TERMS, COUNTRIES["indeed_glassdoor"], COUNTRIES["indeed_only"])
        print("Offsets loaded successfully.")

        # Session timing for the scraping process
        session_start = datetime.now()

        # Establish database connection
        print("Connecting to the database...")
        connection = connect_db()
        if not connection:
            print("Failed to connect to the database. Exiting.")
            return

        print("Database connection established.")
        try:
            for country in get_all_countries():
                print(f"\n--- Starting scrape for {country} ---")

                # Get list of job sites to scrape for this country
                site_list = get_site_list_for_country(country)

                for site in site_list:
                    result = process_site_scraping(site, country, offsets, connection)

                    if not result["success"]:
                        print(f"Partial failures for site {site} in country {country}: {result['failed_terms']}")

            summarize_session(session_start)

        finally:
            print("Closing database connection.")
            connection.close()
            print("Database connection closed.")

    except Exception as e:
        print(f"Unexpected error occurred in scrape_with_varied_params: {e}")
        raise


def get_all_countries():
    """Return a combined list of countries for both Indeed and Glassdoor."""
    return COUNTRIES["indeed_glassdoor"] + COUNTRIES["indeed_only"]


def get_site_list_for_country(country):
    """Determine which job sites to scrape for a given country."""
    return ["indeed", "glassdoor"] if country in COUNTRIES["indeed_glassdoor"] else ["indeed"]


def process_site_scraping(site, country, offsets, connection):
    """Process scraping for a specific site in a given country."""
    print(f"Processing site: {site}, country: {country}")  # Debug
    failed_terms = []

    for search_term in SEARCH_TERMS:
        print(f"Searching for term: {search_term} on {site} in {country}")
        current_offset = offsets[country][site][search_term]["offset"]
        no_results_count = offsets[country][site][search_term].get("no_results_count", 0)

        # Attempt to scrape the search term
        if not scrape_search_term(site, country, search_term, current_offset, no_results_count, RESULTS_PER_PAGE_OPTIONS, offsets, connection):
            print(f"Search term failed: {search_term} on {site} in {country}")
            failed_terms.append(search_term)  # Log failed term
            continue  # Skip to the next search term

    # Log summary of success and failures
    if failed_terms:
        print(f"Failed terms for site {site} in country {country}: {failed_terms}")
    else:
        print(f"All terms scraped successfully for site {site} in country {country}.")

    # Return a summary of the results
    return {
        "site": site,
        "country": country,
        "failed_terms": failed_terms,
        "success": len(failed_terms) == 0  # True if no failures occurred
    }



def scrape_search_term(site, country, search_term, current_offset, no_results_count, results_wanted, offsets, connection):
    """Scrape jobs for a specific search term, site, and country."""
    print(f"Scraping term: {search_term}, offset: {current_offset}, results wanted: {results_wanted}")  # Debug
    retry_attempts = 0

    while retry_attempts < 3:
        try:
            print(f"Scraping {site} for {country} - Term: '{search_term}', Offset: {current_offset}")
            proxy_url = get_proxy_url(BRD_USER, BRD_ZONE, BRD_PASSWD, BRD_SUPERPROXY)

            print(f"Using proxy: {proxy_url}")
            jobs = scrape_jobs(
                site_name=[site],
                search_term=search_term,
                results_wanted=random.choice(results_wanted),  # Adjustable per-page results
                country_indeed=country,
                offset=current_offset,
                proxies={proxy_url},
                ssl_context=CA_CERT_PATH,
            )

            # Process job results
            if process_job_results(jobs, country, site, search_term, offsets, results_wanted, no_results_count, connection):
                print(f"Successfully processed jobs for {search_term} on {site} in {country}")
                return True  # Exit if successful

        except Exception as e:
            retry_attempts += 1
            print(f"Error scraping {site} for {country} at offset {current_offset}: {e}. Retrying...")
            time.sleep(3)

    print(f"Maximum retry attempts reached for {search_term} on {site} in {country}")
    return False  # Mark as failed if all retry attempts are exhausted


def process_job_results(jobs, country, site, search_term, offsets, results_wanted, no_results_count, connection):
    """Process the results of a job scrape."""
    global jobs_added, daily_job_count

    if jobs.empty:
        # Increment no_results_count if no jobs are found
        no_results_count += 1
        offsets[country][site][search_term]["no_results_count"] = no_results_count

        # Reset offset if no results threshold is exceeded - important to ensure unnecessary scraping when receiving duplicates
        if no_results_count >= NO_RESULT_THRESHOLD:
            print(f"No results for {country} - {search_term}. Resetting offset.")
            offsets[country][site][search_term]["offset"] = 0
            offsets[country][site][search_term]["no_results_count"] = 0
            save_offsets(offsets, OFFSET_FILE)
        return False

    # Reset no_results_count if jobs are found
    no_results_count = 0
    offsets[country][site][search_term]["no_results_count"] = no_results_count

    # Save unique jobs to the database
    unique_jobs_added = save_jobs_to_database(jobs, connection)

    # Adjust offsets based on the number of unique jobs added - to ensure not offsetting too much to find as many unique entries asp
    adjust_offsets(offsets, country, site, search_term, unique_jobs_added, results_wanted)

    return True


def save_jobs_to_database(jobs, connection):
    """Save job entries to the database, ensuring no duplicates."""
    global jobs_added, daily_job_count
    unique_jobs_added = 0

    # Iterate through each job and attempt to insert into the database
    for job in jobs.to_dict(orient="records"):
        try:

            if insert_unique_job_data(connection, job, 1, LOG_FILE): # Avoid duplicate entries
                jobs_added += 1
                daily_job_count += 1
                unique_jobs_added += 1
                connection.commit()
                log_to_file(f"Job saved to database: ID={job['id']}", LOG_FILE) # Log the action for each commit
        except Exception as e:
            connection.rollback() #rollback transaction in case of error
            log_to_file(f"Error saving job to database: {e}", LOG_FILE)
            print(f"Error: Failed to save job ID {job.get('id', 'Unknown')} due to {e}")
            continue  # Skip to the next job

    if unique_jobs_added != 0:
        print(f"Added {unique_jobs_added} unique jobs to the database.")

    return unique_jobs_added


def adjust_offsets(offsets, country, site, search_term, unique_jobs_added, results_wanted):
    """Update offsets based on the number of unique jobs added."""
    duplicates = random.choice(results_wanted) - unique_jobs_added # Calculate duplicates
    offsets[country][site][search_term]["offset"] += duplicates # Increment offset
    # Update total_count in the offset.json file to track how many unique entries for a certain search term have been found
    offsets[country][site][search_term]["total_count"] += unique_jobs_added
    save_offsets(offsets, OFFSET_FILE)


def summarize_session(session_start):
    """Print and log a summary of the scraping session."""
    session_end = datetime.now()
    duration = session_end - session_start

    summary = (
        f"Scraping session completed\n"
        f"Session Duration: {duration}\n"
        f"Total Jobs Added: {jobs_added}\n"
    )
    print(summary)
    log_to_file(summary, LOG_FILE)


def monitor_24_hour_job_counts(start_time, daily_job_count_file):
    """
    Monitor and save the count of unique jobs added in a 24-hour timeframe.
    Updates the DAILY_JOB_COUNT_FILE with the date range and count.

    Parameters:
    - start_time: datetime object representing when the script started.
    - daily_job_count_file: str, the path to the JSON file where daily job counts are saved.
    """
    global jobs_added  # Access the global jobs_added

    # Ensure the file exists
    if not os.path.exists(daily_job_count_file):
        with open(daily_job_count_file, "w") as f:
            json.dump([], f, indent=4)

    # Read existing data
    with open(daily_job_count_file, "r") as f:
        job_data = json.load(f)

    # Calculate the 24-hour range
    end_time = start_time + timedelta(hours=24)
    now = datetime.now()

    # If 24 hours have passed, record the count
    if now >= end_time:
        # Format the time range and count
        date_range = (
            f"{start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}"
        )
        record = {
            "date": date_range,
            "count": jobs_added,
        }

        # Append the record to the existing data
        job_data.append(record)

        # Save the updated data back to the file
        with open(daily_job_count_file, "w") as f:
            json.dump(job_data, f, indent=4)

        print(f"Recorded 24-hour job count: {record}")

        # Reset the start time and job count for the next 24-hour period
        start_time = now
        jobs_added = 0  # Reset the global jobs_added

    return start_time


def run_scraping_loop():
    """Run the scraping loop for a specified number of runs."""
    print("Starting scraping loop...")  # Debug

    # Initialize monitoring variables
    script_start_time = datetime.now()

    for run_count in range(MAX_RUNS):
        print(f"Run {run_count + 1} of {MAX_RUNS}")
        try:
            # Test proxy connection
            if test_proxy(get_proxy_url(BRD_USER, BRD_ZONE, BRD_PASSWD, BRD_SUPERPROXY), CA_CERT_PATH):
                print("Proxy test successful. Proceeding with scraping.")
                scrape_all_sites_and_countries()  # Main scraping function

                # Monitor and save 24-hour job counts
                script_start_time = monitor_24_hour_job_counts(
                    script_start_time, DAILY_JOB_COUNT_FILE
                )

                if run_count < MAX_RUNS - 1:  # Wait to spread out requests if not the last run
                    print(f"Waiting {INTERVAL_SECONDS / 60:.2f} minutes until next run...")
                    time.sleep(INTERVAL_SECONDS)
            else:
                print("Proxy test failed. Skipping this run.")
                break
        except Exception as e:
            print(f"Error during scraping loop: {e}")
            break

    print("Scraping loop completed.")


if __name__ == "__main__":
    try:
        # Step 1: Ensure required files are initialized
        initialize_missing_files()  # Create missing files like offsets.json and daily_job_counts.json

        # Step 2: Validate configurations (environment variables and file paths)
        validate_config()  # Check environment variables and other required files

        # Step 3: Validate proxy and certificate configuration
        proxy_url = get_proxy_url(BRD_USER, BRD_ZONE, BRD_PASSWD, BRD_SUPERPROXY)
        validate_proxy_config(CA_CERT_PATH, proxy_url)

        # Step 4: Start the scraping loop
        run_scraping_loop()
        print("Scraping loop completed successfully.")

    except Exception as err:
        print(f"Unexpected Error: {err}")
        exit(1)

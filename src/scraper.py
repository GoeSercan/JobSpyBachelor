import random
import time
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

from proxy import get_proxy_url, test_proxy
from jobspy.scrapers.utils import (
    load_offsets,
    save_offsets,
    connect_db,
    insert_unique_job_data,
    log_to_file,
    # clean_scraping_log,
)
from src.jobspy import scrape_jobs

# Total jobs added to the db
jobs_added = 0
# Jobs added in the current 24-hour period
daily_job_count = 0  #ToDo fix issue of having more jobs than actual found ones within a 24 hour period in DAILY_JOB_COUNT_FILE - currently getting average
last_log_date = None


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
            if not process_job_results(jobs, country, site, search_term, offsets, results_wanted, no_results_count, connection):
                print(f"Failed to process jobs for {search_term} on {site} in {country}")
                return False  # Skip to next term if job results processing fails

            print(f"Successfully processed jobs for {search_term} on {site} in {country}")
            time.sleep(random.randint(1, 2))
            retry_attempts = 3  # Exit retry loop if successful

        except Exception as e:
            retry_attempts += 1
            print(f"Error scraping {site} for {country} at offset {current_offset}: {e}. Retrying...")
            time.sleep(3)

    if retry_attempts == 3:
        print(f"Maximum retry attempts reached for {search_term} on {site} in {country}")
    return retry_attempts < 3


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


# def calculate_24_hour_average():
#     """Calculate 24-hour average and save it to a summary file."""
#     if not os.path.exists(DAILY_JOB_COUNT_FILE):
#         print("No job data available.")
#         return
#
#     # Read job counts
#     with open(DAILY_JOB_COUNT_FILE, "r") as f:
#         job_data = json.load(f)
#
#     now = datetime.now()
#     cutoff = now - timedelta(hours=24)
#     recent_jobs = [entry["count"] for entry in job_data if datetime.fromisoformat(entry["date"]) > cutoff]
#     total_jobs = sum(recent_jobs)
#     average_jobs = total_jobs / len(recent_jobs) if recent_jobs else 0
#
#     summary = {
#         "timestamp": now.isoformat(),
#         "rolling_24_hour_average": average_jobs,
#         "total_jobs_last_24_hours": total_jobs,
#     }
#     log_to_file(f"Rolling 24-hour average: {average_jobs:.2f}, Total jobs: {total_jobs}", LOG_FILE)


def run_scraping_loop():
    """Run the scraping loop for a specified number of runs."""
    print("Starting scraping loop...")  # Debug
    for run_count in range(MAX_RUNS):
        print(f"Run {run_count + 1} of {MAX_RUNS}")
        try:
            # Test proxy connection
            if test_proxy(get_proxy_url(BRD_USER, BRD_ZONE, BRD_PASSWD, BRD_SUPERPROXY), CA_CERT_PATH):
                print("Proxy test successful. Proceeding with scraping.")
                scrape_all_sites_and_countries() # Main scraping function

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
    print("Starting the script...")
    run_scraping_loop()
    print("Script completed.")
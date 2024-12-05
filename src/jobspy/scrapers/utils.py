from __future__ import annotations

import os
import psycopg2
import json
from datetime import datetime
import pandas as pd

import re
import logging
from itertools import cycle

import requests
import tls_client
import numpy as np
from markdownify import markdownify as md
from requests.adapters import HTTPAdapter, Retry

from ..jobs import CompensationInterval, JobType


def create_logger(name: str):
    logger = logging.getLogger(f"JobSpy:{name}")
    logger.propagate = False
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        format = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        formatter = logging.Formatter(format)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    return logger


class RotatingProxySession:
    def __init__(self, proxies=None):
        if isinstance(proxies, str):
            self.proxy_cycle = cycle([self.format_proxy(proxies)])
        elif isinstance(proxies, list):
            self.proxy_cycle = (
                cycle([self.format_proxy(proxy) for proxy in proxies])
                if proxies
                else None
            )
        else:
            self.proxy_cycle = None

    @staticmethod
    def format_proxy(proxy):
        """Utility method to format a proxy string into a dictionary."""
        if proxy.startswith("http://") or proxy.startswith("https://"):
            return {"http": proxy, "https": proxy}
        return {"http": f"http://{proxy}", "https": f"http://{proxy}"}


class RequestsRotating(RotatingProxySession, requests.Session):

    def __init__(self, proxies=None, has_retry=False, delay=1, clear_cookies=False):
        RotatingProxySession.__init__(self, proxies=proxies)
        requests.Session.__init__(self)
        self.clear_cookies = clear_cookies
        self.allow_redirects = True
        self.setup_session(has_retry, delay)

    def setup_session(self, has_retry, delay):
        if has_retry:
            retries = Retry(
                total=3,
                connect=3,
                status=3,
                status_forcelist=[500, 502, 503, 504, 429],
                backoff_factor=delay,
            )
            adapter = HTTPAdapter(max_retries=retries)
            self.mount("http://", adapter)
            self.mount("https://", adapter)

    def request(self, method, url, **kwargs):
        if self.clear_cookies:
            self.cookies.clear()

        if self.proxy_cycle:
            next_proxy = next(self.proxy_cycle)
            if next_proxy["http"] != "http://localhost":
                self.proxies = next_proxy
            else:
                self.proxies = {}
        return requests.Session.request(self, method, url, **kwargs)


class TLSRotating(RotatingProxySession, tls_client.Session):

    def __init__(self, proxies=None):
        RotatingProxySession.__init__(self, proxies=proxies)
        tls_client.Session.__init__(self, random_tls_extension_order=True)

    def execute_request(self, *args, **kwargs):
        if self.proxy_cycle:
            next_proxy = next(self.proxy_cycle)
            if next_proxy["http"] != "http://localhost":
                self.proxies = next_proxy
            else:
                self.proxies = {}
        response = tls_client.Session.execute_request(self, *args, **kwargs)
        response.ok = response.status_code in range(200, 400)
        return response


def create_session(
    *,
    proxies: dict | str | None = None,
    ca_cert: str | None = None,
    is_tls: bool = True,
    has_retry: bool = False,
    delay: int = 1,
    clear_cookies: bool = False,
) -> requests.Session:
    """
    Creates a requests session with optional tls, proxy, and retry settings.
    :return: A session object
    """
    if is_tls:
        session = TLSRotating(proxies=proxies)
    else:
        session = RequestsRotating(
            proxies=proxies,
            has_retry=has_retry,
            delay=delay,
            clear_cookies=clear_cookies,
        )

    if ca_cert:
        session.verify = ca_cert

    return session


def set_logger_level(verbose: int = 2):
    """
    Adjusts the logger's level. This function allows the logging level to be changed at runtime.

    Parameters:
    - verbose: int {0, 1, 2} (default=2, all logs)
    """
    if verbose is None:
        return
    level_name = {2: "INFO", 1: "WARNING", 0: "ERROR"}.get(verbose, "INFO")
    level = getattr(logging, level_name.upper(), None)
    if level is not None:
        for logger_name in logging.root.manager.loggerDict:
            if logger_name.startswith("JobSpy:"):
                logging.getLogger(logger_name).setLevel(level)
    else:
        raise ValueError(f"Invalid log level: {level_name}")


def markdown_converter(description_html: str):
    if description_html is None:
        return None
    markdown = md(description_html)
    return markdown.strip()


def extract_emails_from_text(text: str) -> list[str] | None:
    if not text:
        return None
    email_regex = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
    return email_regex.findall(text)


def get_enum_from_job_type(job_type_str: str) -> JobType | None:
    """
    Given a string, returns the corresponding JobType enum member if a match is found.
    """
    res = None
    for job_type in JobType:
        if job_type_str in job_type.value:
            res = job_type
    return res


def currency_parser(cur_str):
    # Remove any non-numerical characters
    # except for ',' '.' or '-' (e.g. EUR)
    cur_str = re.sub("[^-0-9.,]", "", cur_str)
    # Remove any 000s separators (either , or .)
    cur_str = re.sub("[.,]", "", cur_str[:-3]) + cur_str[-3:]

    if "." in list(cur_str[-3:]):
        num = float(cur_str)
    elif "," in list(cur_str[-3:]):
        num = float(cur_str.replace(",", "."))
    else:
        num = float(cur_str)

    return np.round(num, 2)


def remove_attributes(tag):
    for attr in list(tag.attrs):
        del tag[attr]
    return tag


def extract_salary(
    salary_str,
    lower_limit=1000,
    upper_limit=700000,
    hourly_threshold=350,
    monthly_threshold=30000,
    enforce_annual_salary=False,
):
    """
    Extracts salary information from a string and returns the salary interval, min and max salary values, and currency.
    (TODO: Needs test cases as the regex is complicated and may not cover all edge cases)
    """
    if not salary_str:
        return None, None, None, None

    annual_max_salary = None
    min_max_pattern = r"\$(\d+(?:,\d+)?(?:\.\d+)?)([kK]?)\s*[-—–]\s*(?:\$)?(\d+(?:,\d+)?(?:\.\d+)?)([kK]?)"

    def to_int(s):
        return int(float(s.replace(",", "")))

    def convert_hourly_to_annual(hourly_wage):
        return hourly_wage * 2080

    def convert_monthly_to_annual(monthly_wage):
        return monthly_wage * 12

    match = re.search(min_max_pattern, salary_str)

    if match:
        min_salary = to_int(match.group(1))
        max_salary = to_int(match.group(3))
        # Handle 'k' suffix for min and max salaries independently
        if "k" in match.group(2).lower() or "k" in match.group(4).lower():
            min_salary *= 1000
            max_salary *= 1000

        # Convert to annual if less than the hourly threshold
        if min_salary < hourly_threshold:
            interval = CompensationInterval.HOURLY.value
            annual_min_salary = convert_hourly_to_annual(min_salary)
            if max_salary < hourly_threshold:
                annual_max_salary = convert_hourly_to_annual(max_salary)

        elif min_salary < monthly_threshold:
            interval = CompensationInterval.MONTHLY.value
            annual_min_salary = convert_monthly_to_annual(min_salary)
            if max_salary < monthly_threshold:
                annual_max_salary = convert_monthly_to_annual(max_salary)

        else:
            interval = CompensationInterval.YEARLY.value
            annual_min_salary = min_salary
            annual_max_salary = max_salary

        # Ensure salary range is within specified limits
        if not annual_max_salary:
            return None, None, None, None
        if (
            lower_limit <= annual_min_salary <= upper_limit
            and lower_limit <= annual_max_salary <= upper_limit
            and annual_min_salary < annual_max_salary
        ):
            if enforce_annual_salary:
                return interval, annual_min_salary, annual_max_salary, "USD"
            else:
                return interval, min_salary, max_salary, "USD"
    return None, None, None, None


def extract_job_type(description: str):
    if not description:
        return []

    keywords = {
        JobType.FULL_TIME: r"full\s?time",
        JobType.PART_TIME: r"part\s?time",
        JobType.INTERNSHIP: r"internship",
        JobType.CONTRACT: r"contract",
    }

    listing_types = []
    for key, pattern in keywords.items():
        if re.search(pattern, description, re.IGNORECASE):
            listing_types.append(key)

    return listing_types if listing_types else None


def connect_db():
    try:
        connection = psycopg2.connect(
            dbname="job_listings",
            user="job_user",
            password="your_password",  # Ensure this matches the actual password
            host="localhost"
        )
        return connection
    except Exception as e:
        print("Database connection failed:",e)
        return None

def insert_job_data(conn, job):
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO jobs (
                job_id, site, job_url, job_url_direct, title, company, location,
                date_posted, job_type, salary_source, interval, min_amount,
                max_amount, currency, is_remote, job_level, job_function,
                listing_type, emails, description, company_industry, company_url,
                company_logo, company_url_direct, company_addresses,
                company_num_employees, company_revenue, company_description
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (job_id) DO NOTHING;
        """, (
            job.get('id'), job.get('site'), job.get('job_url'), job.get('job_url_direct'),
            job.get('title'), job.get('company'), job.get('location'), job.get('date_posted'),
            job.get('job_type'), job.get('salary_source'), job.get('interval'), job.get('min_amount'),
            job.get('max_amount'), job.get('currency'), job.get('is_remote'), job.get('job_level'),
            job.get('job_function'), job.get('listing_type'), job.get('emails'), job.get('description'),
            job.get('company_industry'), job.get('company_url'), job.get('company_logo'),
            job.get('company_url_direct'), job.get('company_addresses'), job.get('company_num_employees'),
            job.get('company_revenue'), job.get('company_description')
        ))




def is_valid_job(job):
    """
    Check if any key field is NaN and return a dictionary with the status.
    """
    status = {
        "title": pd.notna(job.get("title")),
        "company": pd.notna(job.get("company")),
        "description": pd.notna(job.get("description"))
    }
    return status, all(status.values())  # Return dict with each status and overall validity


def fetch_duplicate_job(connection, title, company, description):
    """
    Check if the job with the specified composite key already exists.
    If it exists, fetch and return it for comparison; otherwise, return None.
    """
    query = """
    SELECT job_id, title, company, description FROM jobs
    WHERE title = %s AND company = %s AND description = %s
    LIMIT 1
    """
    cursor = connection.cursor()
    cursor.execute(query, (title, company, description))
    return cursor.fetchone()  # Returns the duplicate job if found, otherwise None


def insert_unique_job_data(connection, job, duplicate_count, logfile):
    """
    Insert job data only if it is valid and does not exist in the database.
    Provides a detailed, visually aligned output if the entry is skipped.
    """
    validity_status, is_valid = is_valid_job(job)
    if not is_valid:
        # Skip validation for LinkedIn jobs
        if job.get("site") == "linkedin":
            print("Skipping detailed validation for LinkedIn job.")
        else:
            missing_fields = [field for field, valid in validity_status.items() if not valid]
            print(f"Skipped job due to missing values in fields: {', '.join(missing_fields)}")
            return False

    try:
        # Check if job already exists based on composite key
        duplicate_job = fetch_duplicate_job(connection, job["title"], job["company"], job["description"])
        if duplicate_job:
            # Display the new job and existing job in a side-by-side format with alignment
            print(f"\nDuplicate #{duplicate_count} for this country")
            print("=" * 80)
            print(f"{'New Job':<38} | {'Existing Job in Database'}")
            print("-" * 80)

            # Truncate fields
            new_title = (job['title'][:22] + '...') if len(job['title']) > 25 else job['title']
            new_company = (job['company'][:22] + '...') if len(job['company']) > 25 else job['company']
            new_desc = (job['description'][:22] + '...') if len(job['description']) > 25 else job['description']

            existing_title = (duplicate_job[1][:22] + '...') if len(duplicate_job[1]) > 25 else duplicate_job[1]
            existing_company = (duplicate_job[2][:22] + '...') if len(duplicate_job[2]) > 25 else duplicate_job[2]
            existing_desc = (duplicate_job[3][:22] + '...') if len(duplicate_job[3]) > 25 else duplicate_job[3]

            # Print job attributes side by side with fixed width alignment
            print(f"Title:       {new_title:<25} | Title:       {existing_title:<25}")
            print(f"Company:     {new_company:<25} | Company:     {existing_company:<25}")
            # print(f"Description: {new_desc:<25} | Description: {existing_desc:<25}")
            print("=" * 80)
            duplicate_count += 1

            return False

        insert_job_data(connection, job)
        return True

    except Exception as e:
        log_to_file(f"Error inserting job data: {e} | Job: {job}", logfile)
        return False


def log_to_file(message, logfile):
    """Function to log messages to the log file with a timestamp."""
    with open(logfile, "a") as file:
        file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")


def save_offsets(offsets, offset_file="offsets.json"):
    """Save current offsets to file."""
    with open(offset_file, "w") as f:
        json.dump(offsets, f, indent=4)

def load_offsets(offset_file, search_terms, countries_indeed_glassdoor, countries_indeed_only):
    """Load offsets from file or initialize them if file doesn't exist."""
    if os.path.exists(offset_file):
        with open(offset_file, 'r') as f:
            return json.load(f)
    return {
        country: {"indeed": {term: {"offset": 0, "total_count": 0, "no_results_count": 0} for term in search_terms},
                  "glassdoor": {term: {"offset": 0, "total_count": 0, "no_results_count": 0} for term in search_terms}}
        for country in countries_indeed_glassdoor
    } | {
        country: {"indeed": {term: {"offset": 0, "total_count": 0, "no_results_count": 0} for term in search_terms}}
        for country in countries_indeed_only
    }

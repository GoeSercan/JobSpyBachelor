import requests
import os

proxieslist = []  # Global list for dynamic proxy use


def test_proxy(proxy_url, ca_cert_path):
    """Check if proxy is working."""
    test_url = "https://geo.brdtest.com/welcome.txt"
    print(f"Testing proxy with URL: {proxy_url}")
    print(f"Using CA certificate path: {ca_cert_path}")

    try:
        # Ensure CA certificate path exists
        if not os.path.exists(ca_cert_path):
            raise FileNotFoundError(f"CA certificate not found at: {ca_cert_path}")

        # Test the proxy connection
        response = requests.get(test_url, proxies={"http": proxy_url, "https": proxy_url}, verify=ca_cert_path)

        # Check if the response is successful
        if response.status_code == 200:
            print("Proxy is working. Response received.")
            # Optional: Add to global proxies list if needed
            proxieslist.extend([{"http": proxy_url, "https": proxy_url}])
            return True
        else:
            print(f"Proxy test failed with status code: {response.status_code}")
            return False

    except FileNotFoundError as fe:
        print(f"File error: {fe}")
    except requests.exceptions.RequestException as re:
        print(f"Request error: {re}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    return False


def get_proxy_url(user, zone, passwd, superproxy):
    """Construct the proxy URL."""
    return f"http://brd-customer-{user}-zone-{zone}:{passwd}@{superproxy}"

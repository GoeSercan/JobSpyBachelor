import requests

def test_proxy(proxy_url, ca_cert_path):
    """Check if proxy is working."""
    test_url = "https://geo.brdtest.com/welcome.txt"
    try:
        response = requests.get(test_url, proxies={"http": proxy_url, "https": proxy_url}, verify=ca_cert_path)
        return response.status_code == 200
    except Exception as e:
        return False

def get_proxy_url(user, zone, passwd, superproxy):
    return f"http://brd-customer-{user}-zone-{zone}:{passwd}@{superproxy}"

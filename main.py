import sys
import requests
from bs4 import BeautifulSoup
import tldextract
from collections import Counter
from datetime import datetime, timedelta
import urllib.parse

def get_first_index_date(domain):
    url = f"http://web.archive.org/cdx/search/cdx?url={domain}&output=json&fl=timestamp&collapse=digest&limit=1&filter=statuscode:200"
    response = requests.get(url)
    data = response.json()
    
    if len(data) > 1 and len(data[1]) > 0:
        first_snapshot = data[1][0]
        first_date = datetime.strptime(first_snapshot, '%Y%m%d%H%M%S')
        return first_date
    return None

def google_search(query, start_date, end_date):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
    }
    url = f"https://www.google.com/search?q={urllib.parse.quote(query)}&tbs=cdr:1,cd_min:{start_date.strftime('%m/%d/%Y')},cd_max:{end_date.strftime('%m/%d/%Y')}"
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    search_results = []

    for a in soup.find_all('a', href=True):
        href = a['href']
        if "/url?q=" in href:
            link = href.split("/url?q=")[1].split("&sa=U")[0]
            search_results.append(link)
    
    return search_results

def count_distinct_domains(urls):
    domains = [tldextract.extract(url).registered_domain for url in urls]
    domain_counts = Counter(domains)
    return domain_counts

def main(domain):
    first_index_date = get_first_index_date(domain)
    if not first_index_date:
        print(f"No index date found for {domain}")
        with open('results.txt', 'w') as f:
            f.write("No index date found for the domain.")
        return
    
    end_date = first_index_date + timedelta(days=30)

    query = f"intext:{domain}"
    urls = google_search(query, first_index_date, end_date)
    domain_counts = count_distinct_domains(urls)

    with open('results.txt', 'w') as f:
        f.write("Search Results URLs:\n")
        for url in urls:
            f.write(f"{url}\n")

        f.write("\nDistinct Domain Count:\n")
        for domain, count in domain_counts.items():
            f.write(f"{domain}: {count}\n")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python domain_analysis.py <domain>")
        sys.exit(1)
    
    domain = sys.argv[1]
    main(domain)

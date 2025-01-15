import httpx
from bs4 import BeautifulSoup
from urllib.parse import urlencode
import re
import asyncio

def parse_total_results(stats_text):
    """Extract the total number of results from the result stats text."""
    match = re.search(r'About ([\d,]+) results', stats_text)
    if match:
        return int(match.group(1).replace(',', ''))
    return 0

async def fetch_backlinks_for_domain(client, domain, limit=100, oldest=True):
    """Fetch a specified number of unique backlinks for a single domain asynchronously."""
    base_url = 'https://www.google.com/search'
    query = f'link:{domain}'
    sort_order = 'sort=date' if oldest else 'sort=date_desc'
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
    }
    
    backlinks = set()
    start = 0
    total_results = 0
    first_page = True

    while len(backlinks) < limit:
        params = {
            'q': query,
            'start': start,
            'filter': '0',
            'hl': 'en',
            sort_order: ''
        }
        url = f"{base_url}?{urlencode(params)}"

        try:
            response = await client.get(url, headers=headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                if first_page:
                    result_stats = soup.select_one('#result-stats')
                    if result_stats:
                        total_results = parse_total_results(result_stats.get_text())
                        print(f"[{domain}] Total results found: {total_results}")
                        
                        if total_results < limit:
                            limit = total_results
                    first_page = False

                results = soup.select('.tF2Cxc')

                if not results:
                    print(f"[{domain}] No more results found.")
                    break

                for result in results:
                    link = result.select_one('.yuRUbf a')['href']
                    backlinks.add(link)
                    if len(backlinks) >= limit:
                        break

                start += 10
            else:
                print(f"[{domain}] Failed to fetch results. Status code: {response.status_code}")
                break
        except httpx.RequestError as e:
            print(f"[{domain}] An error occurred: {str(e)}")
            break

    print(f"[{domain}] Collected {len(backlinks)} unique backlinks:")
    for backlink in backlinks:
        print(backlink)
    
    return backlinks

async def main(domains, limit=100, oldest=True):
    async with httpx.AsyncClient() as client:
        tasks = [fetch_backlinks_for_domain(client, domain, limit, oldest) for domain in domains]
        results = await asyncio.gather(*tasks)

    for domain, backlinks in zip(domains, results):
        print(f"\nBacklinks for {domain}:")
        for backlink in backlinks:
            print(backlink)

# Example usage with multiple domains
domains_to_check = ['example.com', 'example.org', 'example.net']
asyncio.run(main(domains_to_check, limit=100, oldest=True))

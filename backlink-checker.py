import os
import requests
import asyncio
from aiohttp import ClientSession, ClientTimeout
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dotenv import load_dotenv
import re
import aiohttp
import json
import os
from collect_data_wayback import collect_data_wayback,exact_url_timestamp
from waybackpy import WaybackMachineCDXServerAPI
import cdx_toolkit
from domainMonitor import DomainMonitor
from get_app_detail import bulk_scrape_and_save_app_urls 
# Load environment variables
load_dotenv()

D1_DATABASE_ID = os.getenv('CLOUDFLARE_D1_DATABASE_ID')
CLOUDFLARE_ACCOUNT_ID = os.getenv('CLOUDFLARE_ACCOUNT_ID')
CLOUDFLARE_API_TOKEN = os.getenv('CLOUDFLARE_API_TOKEN')

# Constants
CLOUDFLARE_BASE_URL = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/d1/database/{D1_DATABASE_ID}"
HEADERS = {
    "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
    "Content-Type": "application/json",
}
ccisopen=False

# Concurrency limit
SEM_LIMIT = 20

# Helper: Parse a sitemap and return all <loc> URLs
async def parse_sitemap(session, url):
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            soup = BeautifulSoup(await response.text(), "xml")
            return [loc.text for loc in soup.find_all("loc")]
    except Exception as e:
        print(f"[ERROR] Failed to fetch sitemap {url}: {e}")
        return []

# Helper: Fetch app page and extract run count
async def get_app_runs(session, item):
    try:
        url=item.get('url')
        # https://huggingface.co/spaces/AP123/IllusionDiffusion/discussions/94
        async with session.get(url) as response:
            response.raise_for_status()
            soup = BeautifulSoup(await response.text(), "html.parser")
            run_span = soup.find("button", class_="flex items-center border-l px-1.5 py-1 text-gray-400 hover:bg-gray-50 focus:bg-gray-100 focus:outline-none dark:hover:bg-gray-900 dark:focus:bg-gray-800")
            if run_span:
                t = run_span.get_text(strip=True).lower()
                if 'k' in t:
                    t = int(float(t.replace('k', '')) * 1000)
                elif 'm' in t:
                    t = int(float(t.replace('m', '')) * 1000000)
                t = re.search(r'\d+', str(t)).group(0)
                item['run_count']=t
                return item
            else:
                print(f"[WARNING] No run count found on page: {url}")
                item['run_count']=0
                
                return item
    except Exception as e:
        print(f"[ERROR] Failed to fetch app page {url}: {e}")
        item['run_count']=0
        
        return item

# Helper: Create table in the database
async def create_table_if_not_exists(session):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS ios_new_apps (
        id SERIAL PRIMARY KEY,
        url TEXT UNIQUE,
        google_indexAt TEXT,
        wayback_createAt TEXT,
        cc_createAt TEXT,
        sitemap_createAt TEXT,
        updateAt TEXT
    );
    """
    payload = {"sql": create_table_sql}
    url = f"{CLOUDFLARE_BASE_URL}/query"
    async with session.post(url, headers=HEADERS, json=payload) as response:
        response.raise_for_status()
        result = await response.json()
        
        if result.get("success"):
            print("[INFO] Table ios_new_apps checked/created successfully.")
            return True  # Assuming table creation was successful
        return False  # Assuming table already existed



async def get_existing_app_data():
    payload = {
        "sql": "SELECT * FROM ios_new_apps;"    }
    url = f"{CLOUDFLARE_BASE_URL}/query"

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=HEADERS, json=payload) as response:
            if response.status != 200:
                print(f"Error: Received HTTP {response.status}")
                return []

            response_data = await response.json()
            print('quety===',response_data)
            if not response_data.get('success'):
                print(f"API Error: {response_data.get('errors')}")
                return []

            result = response_data.get('result')[0].get('results')
            if not result:
                print("No result found.")
                return []

            # Return all rows of data
            return result


# Helper: Check if there is any data in the table
async def is_table_populated(session):
    check_data_sql = "SELECT COUNT(*) AS count FROM ios_new_apps;"
    payload = {"sql": check_data_sql}
    url = f"{CLOUDFLARE_BASE_URL}/query"

    try:
        async with session.post(url, headers=HEADERS, json=payload) as response:
            response.raise_for_status()
            result = await response.json()
            if result.get("success"):
                count = result.get("result")[0].get("count")
                if count:
                    return count > 0
            return False
    except aiohttp.ClientError as e:
        print(f"[ERROR] Failed to check table data: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Unexpected error while checking table data: {e}")
        return False


# Helper: Insert or update app data with retry and exception handling
async def get_domain_born_date(session, url, max_retries=3, retry_delay=5):
    current_time = datetime.utcnow().isoformat()
    print('Try to find first index date of', url)
    user_agent = "check huggingface app's user agent"
    wayback_createAt = None
    cc_createAt = None    

    try:
        cdx_api = WaybackMachineCDXServerAPI(url, user_agent)
        oldest = cdx_api.oldest()
        if oldest.datetime_timestamp:
            wayback_createAt = oldest.datetime_timestamp.isoformat()
        print('==WaybackMachineCDXServerAPI=', wayback_createAt)
    except Exception as e:
        print('WaybackMachineCDXServerAPI failed:', e)
    return wayback_createAt
async def upsert_app_data(session, item, max_retries=3, retry_delay=5):
    current_time = datetime.utcnow().isoformat()

    url=item.get('url')
    google_indexAt=item.get('google_indexAt',None)
    wayback_createAt=item.get('wayback_createAt',None)
    cc_createAt=item.get('cc_createAt',None)
    sitemap_createAt=item.get('sitemap_createAt',None)

    sql = f"""
    INSERT INTO ios_new_apps (url, google_indexAt, wayback_createAt, cc_createAt, sitemap_createAt, updateAt)
    VALUES ('{url}',
            {f"'{google_indexAt}'" if google_indexAt else 'NULL'},
            {f"'{wayback_createAt}'" if wayback_createAt else 'NULL'},
            {f"'{cc_createAt}'" if cc_createAt else 'NULL'},
            {f"'{sitemap_createAt}'" if sitemap_createAt else 'NULL'},
            '{current_time}')
    ON CONFLICT (url) DO UPDATE
    SET
        -- update the updateAt time to current
        updateAt = '{current_time}',
        -- If a new google_indexAt is available use that else keep existing value
        google_indexAt = COALESCE(ios_new_apps.google_indexAt, EXCLUDED.google_indexAt),
        -- If a new wayback_createAt is available use that else keep existing value
        wayback_createAt = COALESCE(ios_new_apps.wayback_createAt, EXCLUDED.wayback_createAt),
        -- If a new cc_createAt is available use that else keep existing value
        cc_createAt = COALESCE(ios_new_apps.cc_createAt, EXCLUDED.cc_createAt),
        -- If a new sitemap_createAt is available use that else keep existing value
         sitemap_createAt = COALESCE(ios_new_apps.sitemap_createAt, EXCLUDED.sitemap_createAt);
    """

    payload = {"sql": sql}
    query_url = f"{CLOUDFLARE_BASE_URL}/query"

    print(f"[DEBUG] SQL query: {sql}")  # Log the SQL query
    print(f"[DEBUG] Payload: {payload}")  # Log the payload


    for attempt in range(max_retries):
        try:
            async with session.post(query_url, headers=HEADERS, json=payload) as response:
                response.raise_for_status()
                print(f"[INFO] Data upserted for {url}.")
                return
        except aiohttp.ClientError as e:
            print(f"[ERROR] Attempt {attempt + 1} failed: {e!r}")
            if attempt < max_retries - 1:
                 try:
                     response_body = await response.json()
                     print(f"[ERROR] Response body: {response_body}")
                 except:
                     pass
                 print(f"[INFO] Retrying in {retry_delay} seconds...")

                 await asyncio.sleep(retry_delay)

        except Exception as e:
            print(f"[ERROR] Unexpected error on attempt {attempt + 1}: {e!r}")
            if attempt < max_retries - 1:
                print(f"[INFO] Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
    print(f"[ERROR] Failed to upsert data for {url} after {max_retries} attempts.")
async def upsert_app_data1(session,item, max_retries=3, retry_delay=5):
    current_time = datetime.utcnow().isoformat()

    url=item.get('url')
    google_indexAt=item.get('google_indexAt',None)
    wayback_createAt=item.get('wayback_createAt',None)
    cc_createAt=item.get('cc_createAt',None)
    sitemap_createAt=item.get('sitemap_createAt',None)
    
    sql = f"""
    INSERT INTO ios_new_apps (url, google_indexAt,wayback_createAt, cc_createAt, sitemap_createAt,updateAt)
    VALUES ('{url}',  
            {f"'{google_indexAt}'" if google_indexAt else 'NULL'}, 
            {f"'{wayback_createAt}'" if wayback_createAt else 'NULL'}, 
            {f"'{cc_createAt}'" if cc_createAt else 'NULL'}, 
            {f"'{sitemap_createAt}'" if sitemap_createAt else 'NULL'}, 
            '{current_time}')
    ON CONFLICT (url) DO UPDATE
    SET updateAt = '{current_time}',
        google_indexAt = COALESCE(ios_new_apps.google_indexAt, EXCLUDED.google_indexAt),
        wayback_createAt = COALESCE(ios_new_apps.wayback_createAt, EXCLUDED.wayback_createAt),
        cc_createAt = COALESCE(ios_new_apps.cc_createAt, EXCLUDED.cc_createAt);
        sitemap_createAt = COALESCE(ios_new_apps.sitemap_createAt, EXCLUDED.sitemap_createAt);
        
    """
    payload = {"sql": sql}
    url = f"{CLOUDFLARE_BASE_URL}/query"

    for attempt in range(max_retries):
        try:
            async with session.post(url, headers=HEADERS, json=payload) as response:
                response.raise_for_status()
                print(f"[INFO] Data upserted for {url}.")
                return
        except aiohttp.ClientError as e:
            print(f"[ERROR] Attempt {attempt + 1} failed: {e}:{response.json()}")
            if attempt < max_retries - 1:
                print(f"[INFO] Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
        except Exception as e:
            print(f"[ERROR] Unexpected error on attempt {attempt + 1}: {e}:{response.json()}")
            if attempt < max_retries - 1:
                print(f"[INFO] Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
    print(f"[ERROR] Failed to upsert data for {url} after {max_retries} attempts.")

# Process a single app URL
async def process_url(semaphore, session, item):
    async with semaphore:
        url=item.get("url")
        print(f"[INFO] Processing app: {url}")
        item = await get_app_runs(session, item)
        # get app name intitle google search count 
        print(f"[INFO] save statics: {item}")
        
        if item is not None:
            await upsert_app_data(session, item)
async def process_new_app(semaphore, session, item):
    async with semaphore:
        await upsert_app_data(session, item)

# Main function
async def main():
    semaphore = asyncio.Semaphore(SEM_LIMIT)
    timeout = ClientTimeout(total=60)
    supportwayback=True
    supportgooglesearch=True
    domainlist=os.getenv('url','')
    if domainlist=='':
        return 
    if ',' in domainlist:
        domainlist=domainlist.split(',')
    else:
        domainlist=[domainlist]
    domainlist=['https://toolify.ai']
    for url in domainlist:
        url=url.strip()
        appurls=[]
        borndate=get_domain_born_date(url)    
        if supportgooglesearch:
            d=DomainMonitor()
            search_urls=[]
            expression=os.getenv('expression',f'link:{url}')
            if 'https://' in url:
                url=url.replace('https://','')
            if 'www.' in url:
                url=url.replace('www.','')
            sites=[
      url,
    'www.'+url
            ]
            d.sites=sites
            time_ranges=[]

            advanced_queries = {        
                    'apps.apple.com': f'{expression} -site:{url}',
                    # 'play.google.com': f'{expression} site:play.google.com'
                
                                   }

            results=d.monitor_all_sites(advanced_queries=advanced_queries)
            print('==',results)
            print("[INFO] google search check  complete.")
            new_apps_urls=[]
            new_items=[]
            # results=results[:10]
            
            if  not results.empty and results.shape[0] > 1:
                gindex=int(datetime.now().strftime('%Y%m%d'))
                items=[]
                for index, row in results.iterrows():
                    item={}
                    url=row.get('url')
                    if '?' in url:
                        url=url.split('?')[0]
                    appname=url.replace(baseUrl,'').split('/')
                    if len(appname)<3:
                        continue
                    if '/developer/' in url:
                        continue
                    # url=baseUrl+appname[0]+'/'+appname[1]+'/'+appname[2]
                    if url in appurls:
                        continue
                    item['url']=url

                    item['google_indexAt']=gindex
                    if not url in appurls:
                        print('check url is existing')
                        existing_apps.append(item)
                        new_apps_urls.append(url)
                        new_items.append(item)
            print('clean google search url item',len(new_apps_urls),new_apps_urls)
            
            
            await asyncio.gather(*(process_new_app(semaphore, session, item) for item in new_items))
        print("[INFO] url detect complete.")
        print("[INFO] update popular space count.")
        bulk_scrape_and_save_app_urls(new_apps_urls)




if __name__ == "__main__":
    asyncio.run(main())

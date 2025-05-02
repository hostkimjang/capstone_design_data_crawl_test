import pprint
import requests
from bs4 import BeautifulSoup
import lxml
import re
import json

useragent = f'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0/MUlvxiFHQPva'


def request():
    #url = f"https://m.place.naver.com/restaurant/1753946312/home"
    url = f'https://m.place.naver.com/restaurant/1753946312/menu/list'
    #url = f'https://m.map.naver.com/search?query=%EC%84%9C%EC%9A%B8%ED%8A%B9%EB%B3%84%EC%8B%9C%20%EC%84%B1%EB%8F%99%EA%B5%AC%20%ED%99%8D%EC%9D%B5%EB%8F%99%20334'
    headers = {
        'User-Agent': useragent,
    }

    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, 'lxml')
    scripts = soup.find_all('script')
    apollo_raw = None
    regex_pattern = re.compile(r'window\.__APOLLO_STATE__\s*=\s*({.*?});', re.DOTALL)

    for script in scripts:
        if script.string and 'window.__APOLLO_STATE__' in script.string:
            match = regex_pattern.search(script.string)
            if match:
                apollo_raw = match.group(1)
                # print("✅ Found APOLLO_STATE script section.") # Debug print
                break # Found it, no need to check other scripts

    if not apollo_raw:
        print("❌ Apollo state data (__APOLLO_STATE__) not found in any script tag.")
        # Optional: Print page content for debugging
        # print(page.text[:2000])
        return None

    # --- JSON Parsing and Data Extraction ---
    try:
        # print("--- Raw Apollo State ---") # Debug print
        # print(apollo_raw[:500] + "...") # Print beginning of raw data for inspection
        # print("--- End Raw Apollo State ---") # Debug print

        apollo_state = json.loads(apollo_raw)
        print("✅ Successfully parsed APOLLO_STATE JSON.")
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse JSON from Apollo state: {e}")
        # Optional: Print problematic part of the raw string
        # error_context = apollo_raw[max(0, e.pos - 50):min(len(apollo_raw), e.pos + 50)]
        # print(f"    Context around error position {e.pos}: ...{error_context}...")
        return None

    print(apollo_state)

if __name__ == "__main__":
    request()
import pprint
import time

import requests
from bs4 import BeautifulSoup
import lxml
import re
import json

useragent = f'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'

def extract_apollo_place_items(html_text: str):
    parser = BeautifulSoup(html_text, "lxml")
    scripts = parser.find_all("script")

    apollo_data_raw = None
    for script in scripts:
        if script.string and "window.__APOLLO_STATE__" in script.string:
            match = re.search(r"window\.__APOLLO_STATE__\s*=\s*({.*?});\s*$", script.string, re.DOTALL)
            if match:
                apollo_data_raw = match.group(1)
                break

    if not apollo_data_raw:
        print("🟡 APOLLO_STATE 데이터를 찾지 못했습니다.")
        return []

    try:
        apollo_json = json.loads(apollo_data_raw)
    except json.JSONDecodeError as e:
        print(f"❌ JSON 파싱 실패: {e}")
        return []

    place_items = []
    for key, value in apollo_json.items():
        if key.startswith("PlaceSummary:") and isinstance(value, dict):
            place_items.append(value)

    print(f"✅ APOLLO_STATE 내 PlaceSummary 항목 {len(place_items)}개 추출 완료")
    return place_items


def extract_menu_items_from_apollo(apollo_json):
    menu_items = []

    for key, value in apollo_json.items():
        if key.startswith("Menu:") and isinstance(value, dict):
            menu_data = {
                "name": value.get("name", "").strip(),
                "price": value.get("price", "").strip(),
                "description": value.get("description", "").strip(),
                "images": value.get("images", [])
            }
            menu_items.append(menu_data)

    return menu_items


def request():
    #url = f"https://m.place.naver.com/restaurant/1753946312/home"
    #url = f'https://m.place.naver.com/restaurant/1753946312/menu/list'
    #url = f'https://m.place.naver.com/place/searchByAddress/addressPlace?query=부산광역시 금정구 금강로 308, 1층 (장전동)&x=126&y=37'
    url = f'https://m.place.naver.com/restaurant/1684777339/menu'
    #url = f'https://m.map.naver.com/search?query=%EC%84%9C%EC%9A%B8%ED%8A%B9%EB%B3%84%EC%8B%9C%20%EC%84%B1%EB%8F%99%EA%B5%AC%20%ED%99%8D%EC%9D%B5%EB%8F%99%20334'
    headers = {
        'User-Agent': useragent,
    }

    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, 'lxml')
    pprint.pprint(soup.prettify())
    scripts = soup.find_all('script')
    apollo_data_raw = None
    for script in scripts:
        # Check if the script content exists and contains the target string
        if script.string and "window.__APOLLO_STATE__" in script.string:
            # --- Corrected Regex ---
            # Removed the '$' anchor to match even if other JS code follows the APOLLO_STATE assignment.
            # Added non-greedy match {.*?} just in case of nested structures, though {.*} often works too.
            match = re.search(r"window\.__APOLLO_STATE__\s*=\s*({.*?});", script.string, re.DOTALL)
            if match:
                apollo_data_raw = match.group(1)
                print("✅ APOLLO_STATE 스크립트 블록 찾음.")
                break # Exit the loop once found

    if not apollo_data_raw:
        print("🟡 APOLLO_STATE 데이터를 포함하는 스크립트를 찾지 못했습니다.")
        return []

    try:
        # Attempt to load the extracted string as JSON
        apollo_json = json.loads(apollo_data_raw)
        print("✅ APOLLO_STATE JSON 파싱 성공.")
    except json.JSONDecodeError as e:
        print(f"❌ JSON 파싱 실패: {e}")
        # Optional: Print a snippet of the raw data for debugging JSON errors
        # print("--- 파싱 시도한 데이터 (일부) ---")
        # print(apollo_data_raw[:500] + "..." if apollo_data_raw else "N/A")
        # print("-----------------------------")
        return []

    menu_items = extract_menu_items_from_apollo(apollo_json)

    if menu_items:
        print(f"✅ 메뉴 {len(menu_items)}개 추출 완료!")
        print(json.dumps(menu_items, indent=2, ensure_ascii=False))
    else:
        print("ℹ️ 메뉴 항목이 없습니다.")

if __name__ == "__main__":
    request()
import asyncio
import json
import random
import time
from asyncio import wait_for
from multiprocessing.managers import PoolProxy
from re import search
from time import sleep
import lxml
import platform
from bs4 import BeautifulSoup
import chardet
import pandas as pd
import pprint
import sqlite3
import zendriver as zd
import urllib
import re
from re import search, sub, compile as re_compile # compile 추가
import os
import sys

headless = False

def store_first_db():

    file_path = 'fulldata_07_24_04_P_일반음식점.csv'

    with open(file_path, 'rb') as f:
        sample = f.read(1024 * 1024)  # 1MB

    # 샘플 데이터로 인코딩 감지
    encoding_data = chardet.detect(sample)
    detected_encoding = encoding_data['encoding']
    print(f"Detected encoding: {detected_encoding}")

    data = pd.read_csv(file_path, encoding=detected_encoding, low_memory=False)
    pprint.pprint(data.head())
    print("Data loaded successfully.")

    # for column in data.columns:
    #     unique_values = data[column].dropna().unique()
    #     print(f"📌 컬럼: {column}")
    #     print(f"   ▶ 고유값 {len(unique_values)}개")
    #     print(f"   ▶ 샘플: {unique_values[:10]}")  # 처음 10개만 보여줌
    #     print("-" * 50)

    #check data length
    print(f"Total records: {len(data)}")

    # 폐업 제외
    if '영업상태명' in data.columns:
        print("🛠️ '영업상태명' 컬럼을 기준으로 폐업 데이터 걸러내는 중...")
        data = data[data['영업상태명'] != '폐업']
    else:
        print("⚠️ '영업상태명' 컬럼이 없습니다. 폐업 데이터 걸러내지 않습니다.")

    # SQLite 데이터베이스 연결
    conn = sqlite3.connect('food_data.db')

    # DataFrame을 SQLite 테이블로 저장
    data.to_sql('restaurants', conn, if_exists='replace', index=False)

    # 연결 끊기
    conn.close()

    print(f"✅ 폐업 제외 후 {len(data)}건 저장 완료! (DB: food_data.db, Table: restaurants)")

def load_10_restaurant_names_and_addresses():
    conn = sqlite3.connect('food_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT 번호, 사업장명, 도로명전체주소 FROM restaurants where CRAWL = 0 LIMIT 200;")
    rows = cursor.fetchall()

    conn.close()

    restaurant_infos = []
    for row in rows:
        id, business_name, road_address = row
        if id and business_name and road_address:
            restaurant_infos.append((id, business_name, road_address))
    return restaurant_infos

def load_restaurant_subset(start, end):
    conn = sqlite3.connect("/app/food_data.db")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 번호, 사업장명, 도로명전체주소 FROM restaurants LIMIT ? OFFSET ?",
        (end - start, start),
    )
    rows = cursor.fetchall()
    conn.close()
    return [(id, name, addr) for id, name, addr in rows if name and addr]

def make_search_query(business_name, road_address):
    # 도로명 주소 앞 3단계까지만
    parts = road_address.split()
    if len(parts) >= 3:
        filtered_address = ' '.join(parts[:3])
    else:
        filtered_address = road_address

    query = f"{business_name} {filtered_address}"
    # pprint.pprint(f"Search query: {query}")
    return query

def sanitize_filename(name):
    name = re.sub(r'[\\/*?:"<>|]', "", name).replace(" ", "_")
    return name[:100] if len(name) > 100 else name

async def load_page_with_wait(browser, url):
    page = await browser.get(url)

    tmp_parser = BeautifulSoup(await page.get_content(), "lxml")
    if any(err in tmp_parser.get_text().lower() for err in [
        "500", "internal server error", "proxy error", "nginx", "html error", "bad gateway"
    ]):
        time.sleep(3)
        raise Exception("❌ 페이지 로드 실패: 서버 오류")
    await page.wait_for("div.place_business_list_wrapper", timeout=10)
    return page


async def load_page_with_wait02(browser, url):
    page = await browser.get(url)
    tmp_parser = BeautifulSoup(await page.get_content(), "lxml")
    if any(err in tmp_parser.get_text().lower() for err in [
        "500", "internal server error", "proxy error", "nginx", "html error", "bad gateway"
    ]):
        time.sleep(3)
        raise Exception("❌ 페이지 로드 실패: 서버 오류")
    await page.wait_for("div.place_fixed_maintab", timeout=10)
    return page


async def with_retry(func, retries=30, delay=1):
    for attempt in range(retries):
        try:
            return await func()
        except Exception as e:
            print(f"⚠️ 재시도 {attempt+1}/{retries} 실패: {e}")
            await asyncio.sleep(delay)
    raise Exception("❌ 모든 재시도 실패")


async def wait_for_selector_with_retry(page, selector, timeout=10, interval=1):
    max_attempts = int(timeout / interval)
    for attempt in range(max_attempts):
        try:
            node = await page.select_all(selector)
            if node:
                return node
        except Exception:
            pass
        await asyncio.sleep(interval)
    raise TimeoutError(f"❌ '{selector}' 로딩 실패 (timeout={timeout}s)")


def extract_dynamic_place_info(soup: BeautifulSoup):
    place_info = {}
    title_block = soup.select("div.zD5Nm > div#_title")
    info_blocks = soup.select("div.PIbes > div.O8qbU")
    if title_block:
        title = title_block[0].get_text(strip=True)
        place_info["title"] = title

    for block in info_blocks:
        key_elem = block.select_one("strong > span.place_blind")
        value_block = block.select_one("div.vV_z_")
        if not key_elem or not value_block:
            continue
        key = key_elem.get_text(strip=True)
        if key == "주소":
            addr = value_block.select_one("span.LDgIH")
            place_info["주소"] = addr.text.strip() if addr else None
        elif key == "전화번호":
            phone = value_block.select_one("span.xlx7Q")
            place_info["전화번호"] = phone.text.strip() if phone else None
        elif key == "영업시간":
            status = value_block.select_one("em")
            hours = value_block.select_one("time")
            place_info["영업상태"] = status.text.strip() if status else None
            place_info["영업시간"] = hours.text.strip() if hours else None
        elif key == "홈페이지":
            links = value_block.select("a.place_bluelink")
            place_info["홈페이지들"] = [a["href"] for a in links if a.get("href")]
        else:
            place_info[key] = value_block.get_text(strip=True)
    return place_info

def append_to_json_file(data, filepath):
    # 파일이 있으면 기존 데이터 로드
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []
    else:
        existing = []

    # 중복 방지 (title 기준)
    titles = {entry.get("title") for entry in existing}
    if data.get("title") not in titles:
        existing.append(data)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(existing, f, ensure_ascii=False, indent=2)
        print(f"📦 저장 완료: {data['title']}")
    else:
        print(f"⚠️ 중복으로 저장 건너뜀: {data['title']}")


def log_error_json(error_info, filepath):
    error_info["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(error_info, ensure_ascii=False) + "\n")
    print(f"❌ 오류 기록 완료: {error_info['title'] if 'title' in error_info else '알 수 없는 오류'}")


async def with_browser_retry(browser_ref, executable, browser_args, coro_fn, retries=30, delay=2):
    for attempt in range(retries):
        try:
            result = await coro_fn(browser_ref[0])
            html = await result.get_content()
            soup = BeautifulSoup(html, "lxml")
            page_text = soup.get_text().lower()
            for err in [
                "500", "internal server error", "proxy error", "nginx",
                "sigkill", "sigtrap", "aw snap", "페이지를 표시하는 도중 문제"
            ]:
                if err in page_text:
                    print(f"❌ 페이지 로드 실패: 에러 탐지됨 → '{err}'")
                    raise Exception(f"🛑 HTML 내 에러 페이지 탐지됨: '{err}'")
            return result
        except Exception as e:
            print(f"⚠️ 브라우저 작업 실패 {attempt+1}/{retries}: {e}")
            try:
                await browser_ref[0].stop()
            except:
                pass
            print("🔄 브라우저 재시작 중...")
            browser_ref = [await start_browser(executable)]
            await asyncio.sleep(delay)
    raise Exception("❌ 브라우저 재시도 모두 실패")


async def start_browser(executable):
    return await zd.start(
        headless=headless,
        browser_executable_path=executable,
        browser_args=browser_args
    )

async def wait_for_browser_ready(port, timeout=10):
    import socket
    for _ in range(timeout):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except:
            await asyncio.sleep(1)
    raise Exception(f"🚫 브라우저 포트 {port} 연결 실패")


async def with_browser_get(url, browser_ref, executable, retries=30, delay=2):
    for attempt in range(retries):
        try:
            print(f"📡 시도 {attempt+1}/{retries}: {url}")
            page = await browser_ref[0].get(url)
            html = await page.get_content()
            soup = BeautifulSoup(html, "lxml")
            if any(err in soup.get_text().lower() for err in [
                "500", "internal server error", "proxy error", "nginx", "html error", "bad gateway"
            ]):
                raise Exception("🛑 HTML 내 에러 페이지 탐지됨")
            return page
        except Exception as e:
            print(f"⚠️ 브라우저 작업 실패 {attempt+1}/{retries}: {e}")
            print("🔄 브라우저 재시작 중...")
            try:
                await browser_ref[0].stop()
            except:
                pass
            await asyncio.sleep(delay)
            browser_ref[0] = await start_browser(executable)
    raise Exception("❌ 브라우저 재시도 모두 실패")

def normalize(text: str) -> str:
    if not text:
        return ''
    text = re.sub(r'\s+', '', text)                 # 모든 공백 제거
    text = re.sub(r'[^\w가-힣]', '', text)           # 특수문자 제거
    text = re.sub(r'[\u200b\u200c\u200d\ufeff\xa0]', '', text)  # 비가시 문자 제거
    return text.lower()

def normalize_address_for_comparison(addr: str) -> str:
    """Specific normalization for address comparison."""
    if not addr:
        return ''
    # Remove floor info, parenthesized details, and all whitespace
    addr = re.sub(r'(?:지하|지상)?\s?\d+층', '', addr.strip()).strip()
    addr = re.sub(r'\b\d+호\b', '', addr.strip()).strip() # Remove building unit number like 201호
    addr = re.sub(r'\(.*?\)', '', addr.strip()).strip()
    addr = re.sub(r'\s+', '', addr)
    # Keep essential address characters (Hangul, numbers, basic separators if needed later, but remove for now)
    # Keep commas and hyphens which might be part of address numbers (e.g., 232-9)
    addr = re.sub(r'[^\w가-힣,-]', '', addr)
    return addr.lower()

def extract_space_items(html_text: str):
    """
    PC 페이지 HTML에서 class="space_title" 요소를 모두 추출하여
    장소 이름 리스트 반환
    """
    parser = BeautifulSoup(html_text, "lxml")
    title_tags = parser.select("strong.space_title")

    place_names = []
    for tag in title_tags:
        name = tag.get_text(strip=True)
        if name:
            place_names.append(name)

    if not place_names:
        print("🟡 장소 이름을 찾지 못했습니다.")
    else:
        print(f"✅ 장소 이름 {len(place_names)}개 추출 완료.")
    return place_names

def extract_apollo_place_items(html_text: str):
    """
    Extracts place summary items from the __APOLLO_STATE__ JSON in the HTML.
    """
    parser = BeautifulSoup(html_text, "lxml")
    scripts = parser.find_all("script")

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

    place_items = []
    # Iterate through the parsed JSON dictionary
    for key, value in apollo_json.items():
        # Check if the key is a string, starts with "PlaceSummary:", and the value is a dictionary
        if isinstance(key, str) and key.startswith("PlaceSummary:") and isinstance(value, dict):
             # Optionally add more specific checks, e.g., if value must contain '__typename'
             # if '__typename' in value and value['__typename'] == 'PlaceSummary':
            place_items.append(value)

    print(f"✅ APOLLO_STATE 내 PlaceSummary 항목 {len(place_items)}개 추출 완료")
    return place_items



def extract_rq_items(html_text: str):
    """
    window.__RQ_STREAMING_STATE__.push({...}); 블록 안의 JSON을 추출해서
    items 리스트를 전부 반환한다.
    """
    parser = BeautifulSoup(html_text, "lxml")
    scripts = parser.find_all("script")
    # pprint.pprint(scripts) # 디버깅용

    # window.__RQ_STREAMING_STATE__.push(...) 를 찾는 정규식
    # JSON 객체가 복잡하고 여러 줄일 수 있으므로 관대한 패턴 사용
    push_regex = re_compile(
        r'window\.__RQ_STREAMING_STATE__\.push\((.*?)\)\s*;?\s*$', # 끝부분 공백/세미콜론 허용
        re.DOTALL | re.MULTILINE # 여러 줄에 걸쳐 매칭
    )

    all_items = []
    found_pushes = 0

    for script in scripts:
        if not script.string:
            continue

        # 정규식으로 push 호출 부분 찾기
        matches = push_regex.findall(script.string.strip()) # 스크립트 내용 앞뒤 공백 제거
        # pprint.pprint(matches) # 디버깅용
        for match in matches:
            found_pushes += 1
            # print(f"DEBUG: Found push content: {match[:200]}...") # 디버깅용
            try:
                # JSON 파싱 시도
                parsed = json.loads(match)

                # 'queries' 키 확인 및 순회
                queries = parsed.get("queries", [])
                if not isinstance(queries, list):
                    # print("DEBUG: 'queries' is not a list.")
                    continue

                for q_index, q in enumerate(queries):
                    # 'state', 'data', 'items' 경로 확인
                    items = q.get("state", {}).get("data", {}).get("items", [])

                    # items가 리스트이고 내용이 있는지 확인
                    if isinstance(items, list) and items:
                        print(f"✅ Script 내에서 {len(items)}개의 items 발견 (query index: {q_index})")
                        all_items.extend(items) # 찾은 아이템 추가

            except json.JSONDecodeError as e:
                # print(f"⚠️ JSON 파싱 실패: {e}. Content: {match[:300]}...") # 디버깅용
                continue # 파싱 실패 시 다음 매치 또는 스크립트로
            except Exception as e:
                print(f"⚠️ 데이터 추출 중 예상치 못한 오류: {e}")
                continue

    if found_pushes == 0:
         print("🟡 RQ_STREAMING_STATE push 호출을 찾지 못했습니다.")
    elif not all_items:
        print("🟡 push 호출은 찾았으나, 유효한 'items' 데이터를 포함한 호출이 없었습니다.")
    else:
         print(f"✅ 최종 추출된 items: {len(all_items)}개")

    return all_items


async def crawler():
    restaurant_infos = load_10_restaurant_names_and_addresses()

    if not restaurant_infos:
        print("❌ 데이터베이스에서 가게 정보를 불러오지 못했습니다.")
        return

    print(f"ℹ️ {len(restaurant_infos)}개 가게에 대한 크롤러를 시작합니다...")

    system = platform.platform()
    arch = platform.machine()
    executable = None

    print("시스템 정보 확인")
    print(f"시스템: {system}")
    print(f"아키텍처: {arch}")

    if system != "mac" and arch in ("aarch64", "arm64"):
        print("ARM64 환경 감지")
        if os.path.exists("/usr/bin/ungoogled-chromium"):
            executable = "/usr/bin/ungoogled-chromium"
        elif os.path.exists("/usr/bin/chromium"):
            executable = "/usr/bin/chromium"

    try:
        browser_ref = [await start_browser(executable)]

        print("✅ Zendriver 시작 완료.")
        success, fail, need_check = 0, 0, 0

        for index, (id, business_name, road_address) in enumerate(restaurant_infos):
            if (index + 1) % 10 == 0:
                await browser_ref[0].stop()
                print("🔄 메모리 유출 방지 브라우저 재시작 중...")
                browser_ref = [await start_browser(executable)]
                print("✅ 메모리 유출 방지 브라우저 재시작 완료.")

            try:
                #search_query = make_search_query(business_name, road_address)
                search_query = road_address
                encoded_query = urllib.parse.quote(search_query)
                mob_url = f"https://m.place.naver.com/place/searchByAddress/addressPlace?query={encoded_query}&x=126&y=37"
                print(f"🔗 [{index + 1} | {len(restaurant_infos)}] {business_name}")
                print(f"🔗 [{index + 1} | {len(restaurant_infos)}] {search_query} URL: {mob_url}")

                page = await with_browser_get(mob_url, browser_ref, executable, retries=30, delay=3)
                await page.wait_for("header", timeout=10)
                html_src = await page.get_content()
                # Extract potential matches from Apollo state
                items = extract_apollo_place_items(html_src)

                if not items:
                    print(f"⚠️ [{index + 1} | {len(restaurant_infos)}] Apollo items 추출 실패 또는 없음: {business_name}")
                    need_check += 1
                    log_error_json({"id": id, "title": business_name, "address": road_address, "url": mob_url, "error": "No Apollo items found"}, os.path.join(ERROR_DIR, f"error_log_{start_index}.json"))
                    continue

                # --- Matching Logic ---
                normalized_db_name = normalize(business_name)
                normalized_db_addr = normalize_address_for_comparison(road_address)
                #print(f"   정규화된 DB 이름: '{normalized_db_name}'")
                #print(f"   정규화된 DB 주소: '{normalized_db_addr}'")

                name_matches = []
                for item in items:
                    item_name = item.get("name", "")
                    normalized_item_name = normalize(item_name)
                    #print(f"   - 검사중인 이름: '{item_name}' (정규화: '{normalized_item_name}')") # Debug print
                    if normalized_item_name == normalized_db_name:
                        name_matches.append(item)
                        print(f"     ✔️ 이름 일치!")

                if not name_matches:
                    print(f"🟡 [{index + 1} | {len(restaurant_infos)}] 이름 일치 항목 없음: '{business_name}'")
                    need_check += 1
                    log_error_json({"id": id, "title": business_name, "address": road_address, "url": mob_url, "error": "No name match in Apollo items", "found_names": [i.get('name') for i in items]}, os.path.join(ERROR_DIR, f"error_log_{start_index}.json"))
                    continue

                elif len(name_matches) == 1:
                    best_match = name_matches[0]
                    print(f"✅ [{index + 1} | {len(restaurant_infos)}] 이름 유일 매칭 성공: '{best_match.get('name')}' (ID: {best_match.get('id')})")

                else: # Multiple name matches, use address to disambiguate
                    print(f"⚠️ [{index + 1} | {len(restaurant_infos)}] '{business_name}' 이름 일치 항목 {len(name_matches)}개 발견. 주소 비교 시도...")
                    found_address_match = False
                    for match in name_matches:
                        addr_to_check = match.get("roadAddress") or match.get("address") # Prefer road address
                        normalized_item_addr = normalize_address_for_comparison(addr_to_check)

                        print(f"   - 비교 대상 주소: '{addr_to_check}' (정규화: '{normalized_item_addr}')")

                        # Check if normalized DB address is contained within the normalized item address
                        # This is slightly more flexible than exact match
                        if normalized_db_addr in normalized_item_addr:
                            best_match = match
                            print(f"✅ [{index + 1} | {len(restaurant_infos)}] 주소 포함 확인: '{best_match.get('name')}' (ID: {best_match.get('id')})")
                            found_address_match = True
                            break # Use the first address match found

                    if not found_address_match:
                        best_match = name_matches[0] # Fallback to the first name match if no address matches
                        print(f"🟡 [{index + 1} | {len(restaurant_infos)}] 주소 일치/포함 없음. 첫 번째 이름 일치 항목 사용: '{best_match.get('name')}' (ID: {best_match.get('id')})")
                # --- End Matching Logic ---

                if best_match and best_match.get("id"):
                    best = best_match
                    business_name = best.get("name")
                    print(f"✅ [{index + 1} | {len(restaurant_infos)}] 최종 매칭 성공: '{business_name}' (ID: {best['id']})")

                await with_browser_retry(
                    browser_ref, executable, browser_args,
                    lambda b: b.get(f"https://m.place.naver.com/place/{best['id']}")
                )
                print(f"🔗 [{index + 1} | {len(restaurant_infos)}] {f"https://m.place.naver.com/place/{best['id']}"} 로딩 완료")
                print(f"🔗[{index + 1} | {len(restaurant_infos)}] {search_query} 2차 URL: https://m.place.naver.com/place/{best['id']}")
                await with_retry(lambda: page.wait_for("div.place_fixed_maintab", timeout=10))

                parser = BeautifulSoup(await page.get_content(), "lxml")
                main_tab = parser.select_one('div[class="place_fixed_maintab"]')
                if main_tab:
                    href_list = [
                        a['href']
                        for a in main_tab.select('a[href]')
                        if a['href'].strip() and not a['href'].strip().startswith('#')
                    ]
                    print(f"🍽️ [{index + 1} | {len(restaurant_infos)}] {search_query} 유효한 링크 개수: {len(href_list)}")
                    print(f"🍽️ [{index + 1} | {len(restaurant_infos)}] {search_query} 링크: {href_list}")
                else:
                    print(f"❌ [{index + 1} | {len(restaurant_infos)}] {search_query} place_fixed_maintab not found.")

                place_info = extract_dynamic_place_info(parser)

                data = {
                    "id": id,
                    "query": search_query,
                    "title": business_name,
                    "place_info": place_info,
                    "unique_links": f'/place/{best['id']}',
                    "tab_list": href_list,
                    "url": mob_url
                }
                print(data)

                append_to_json_file(data, output_path)
                success += 1

            except Exception as e:
                print(f"❌ [{index + 1} | {len(restaurant_infos)}] JSON 매칭 실패: {e}")
                fail += 1
                continue


    finally:
        await browser_ref[0].stop()
        print("🛑 Zendriver 종료 완료")
        print("🛑 크롤러 종료 완료")
        print(f"\n✅ 완료: {success} / ❌ 실패: {fail} / ⚠️ 확인 필요: {need_check}")


if __name__ == "__main__":
    #store_first_db()
    if not os.path.exists("screenshots"):
        os.makedirs("screenshots")

    if not os.path.exists("web_data"):
        os.makedirs("web_data")

    if not os.path.exists("error_logs"):
        os.makedirs("error_logs")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    SCREENSHOT_DIR = os.path.join(BASE_DIR, "screenshots")
    DATA_DIR = os.path.join(BASE_DIR, "web_data")
    ERROR_DIR = os.path.join(BASE_DIR, "error_logs")

    os.makedirs(SCREENSHOT_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(ERROR_DIR, exist_ok=True)

    print("📂 Screenshot 저장 경로:", SCREENSHOT_DIR)
    print("📂 WebData 저장 경로:", DATA_DIR)
    print("📂 ErrorLog 저장 경로:", ERROR_DIR)

    start_index = int(os.environ.get("crawl_second_START_INDEX", sys.argv[1] if len(sys.argv) > 1 else 0))
    output_path = os.path.join(DATA_DIR, f"crawl_second_output_{start_index}.json")

    browser_args = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-setuid-sandbox",
        "--disable-software-rasterizer",
        "--disable-blink-features=AutomationControlled",
        "--window-size=1280x800",  # 반드시 사이즈 지정
        "--start-maximized",  # headless에서도 최대화처럼 보이게
        "--disable-infobars",
        "--disable-extensions",
        "--disable-popup-blocking",
        "--enable-logging=stderr",
        "--log-level=1",
    ]

    asyncio.run(crawler())
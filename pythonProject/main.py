import asyncio
import random
import time
from time import sleep
import lxml
from bs4 import BeautifulSoup
import chardet
import pandas as pd
import pprint
import sqlite3
import zendriver as zd
import urllib
import re
import os

if not os.path.exists("screenshots"):
    os.makedirs("screenshots")

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

    cursor.execute("SELECT 사업장명, 도로명전체주소 FROM restaurants ;")
    rows = cursor.fetchall()

    conn.close()

    restaurant_infos = []
    for row in rows:
        business_name, road_address = row
        if business_name and road_address:
            restaurant_infos.append((business_name, road_address))
    return restaurant_infos

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
    # 파일명에 사용할 수 없는 문자 제거 (\ / * ? : " < > |)
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    # 공백을 밑줄로 변경 (선택 사항)
    name = name.replace(" ", "_")
    # 필요시 최대 길이 제한 (선택 사항)
    max_len = 100
    if len(name) > max_len:
        name = name[:max_len]
    return name


async def crawler():
    restaurant_infos = load_10_restaurant_names_and_addresses()
    if not restaurant_infos:
        print("❌ 데이터베이스에서 가게 정보를 불러오지 못했습니다.")
        return

    print(f"ℹ️ {len(restaurant_infos)}개 가게에 대한 크롤러를 시작합니다...")
    if not os.path.exists("screenshots"):
        os.makedirs("screenshots")

    try:
        print("🚀 Zendriver 시작 중...")
        browser = await zd.start(headless=False)
        print("✅ Zendriver 시작 완료. 브라우저 객체 확보.")

        results = []

        for index, (business_name, road_address) in enumerate(restaurant_infos):
            search_query = make_search_query(business_name, road_address)
            encoded_query = urllib.parse.quote(search_query) # URL 인코딩
            search_url = f"https://map.naver.com/p/search/{encoded_query}"

            print(f"\n--- {index+1}/{len(restaurant_infos)} 처리 중: {search_query} ---")
            print(f"🔗 URL: {search_url}")

            try:
                # 1. browser.get()으로 이동하고, 반환된 page/tab 객체를 사용
                page = await browser.get(search_url)
                print("🌐 페이지 이동 완료. 콘텐츠 로딩 대기 중...") # browser.get이 완료될 때까지 기다린다고 가정

                # --- 이제 'page' (Tab 객체)를 사용하여 요소 찾기 ---
                search_iframe_selector = "#searchIframe"
                entry_iframe_selector = "#entryIframe"

                pprint.pprint("🔄 searchIframe 로딩 대기중...")
                await page.wait_for(search_iframe_selector, timeout=10000)
                pprint.pprint("✅ searchIframe 로딩 완료.")

                # tmp_content = await page.get_content()
                pprint.pprint('SearchIframe url 확인중')
                iframe_elements_str_list = await page.select_all("#searchIframe")
                iframe_string = str(iframe_elements_str_list[0])
                match = re.search(r'src="([^"]*)"', iframe_string)
                pprint.pprint(f"iframe URL: {match.group(1)}")

                await page.get(match.group(1))
                tmp_content = await page.get_content()
                tmp_parse = BeautifulSoup(tmp_content, "lxml")

                if tmp_parse.select("div[class='FYvSc']"):
                    pprint.pprint("❌ 검색 결과 없음.")
                    continue

                await page.get(search_url)
                pprint.pprint("🔄 entryIframe 로딩 대기중...")
                await page.wait_for(search_iframe_selector, timeout=10000)
                await page.wait_for(entry_iframe_selector, timeout=10000)
                pprint.pprint("✅ entryIframe 로딩 완료.")
                iframe_elements_str_list = await page.select_all("#entryIframe")
                iframe_string = str(iframe_elements_str_list[0])
                match = re.search(r'src="([^"]*)"', iframe_string)
                pprint.pprint(f"iframe URL: {match.group(1)}")
                await page.get(match.group(1))
                time.sleep(1)


            except Exception as e:
                print(f"❌ iframe 로딩 중 오류 발생: {e}")
                continue

        # --- 모든 루프 종료 후 ---
        print("\n🎉 크롤러 작업 완료.")

    except Exception as start_err:
        print(f"💥 Zendriver 시작 또는 전체 크롤링 과정에서 심각한 오류 발생: {start_err}")

if __name__ == "__main__":
    #store_first_db()
    asyncio.run(crawler())
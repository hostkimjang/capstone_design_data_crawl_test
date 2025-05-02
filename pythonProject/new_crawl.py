import asyncio
import json
import random
import time
from asyncio import wait_for
from time import sleep
import lxml
import platform
import requests
from bs4 import BeautifulSoup
import chardet
import pandas as pd
import pprint
import sqlite3
import zendriver as zd
import urllib
import re
import os
import sys

def load_10_restaurant_names_and_addresses():
    conn = sqlite3.connect('food_data.db')
    cursor = conn.cursor()

    cursor.execute("SELECT 사업장명, 도로명전체주소 FROM restaurants LIMIT 200;")
    rows = cursor.fetchall()

    conn.close()

    restaurant_infos = []
    for row in rows:
        business_name, road_address = row
        if business_name and road_address:
            restaurant_infos.append((business_name, road_address))
    return restaurant_infos

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


def crawl(url, useragent):
    restaurant_infos = load_10_restaurant_names_and_addresses()

    if not restaurant_infos:
        print("❌ 데이터베이스에서 가게 정보를 불러오지 못했습니다.")
        return

    print(f"ℹ️ {len(restaurant_infos)}개 가게에 대한 크롤러를 시작합니다...")

    headers = {
        'User-Agent': useragent
    }

    need_check = 0

    try:
        for index, (business_name, road_address) in enumerate(restaurant_infos):
            try:
                search_query = make_search_query(business_name, road_address)
                encoded_query = urllib.parse.quote(search_query)
                mob_url = f"https://m.place.naver.com/restaurant/list?query={encoded_query}&x=126&y=37"
                print(f"🔗 [{index+1}] {search_query}")
                print(f"🔗 [{index+1}] {search_query} URL: {mob_url}")

                page = requests.get(url=mob_url, headers=headers)
                soup = BeautifulSoup(page.content, 'lxml')
                # print(soup.prettify())

                if soup.select("div[class='FYvSc']") or "조건에 맞는 업체가 없습니다" in soup.get_text():
                    print(f"❌ [{index + 1}] {search_query} 검색 결과 없음")
                    log_error_json({
                        "query": search_query,
                        "title": business_name,
                        "address": road_address,
                        "url": mob_url,
                        "type": "no_store",
                        "reason": "검색 결과 없음"
                    }, os.path.join(ERROR_DIR, f"error_log_{start_index}.jsonl"))

                    need_check += 1

                a_tags = soup.select("div.place_business_list_wrapper > ul > li a[href]")
                href_list = [a['href'] for a in a_tags]
                valid_links = list(set(
                    re.match(r"^/restaurant/\d+", href).group(0)
                    for href in href_list if re.match(r"^/restaurant/\d+", href)
                ))

                if not valid_links:
                    print(f"⚠️ [{index + 1}] {search_query} 링크 없음")
                    print(soup.prettify())
                    need_check += 1
                    time.sleep(10)

                print(f"🔗 [{index + 1}] {search_query} {len(valid_links)}개 유효 링크 발견")
                print(f"🔗 [{index + 1}] {search_query} {valid_links[:len(valid_links)]}")
                unique_links = list(set(valid_links))

                if len(unique_links) > 1:
                    print(f"⚠️ [{index + 1}] {search_query} 1차 검색 유사도 다중 상점 발견: {unique_links}")

                data_2 = requests.get(url=f'https://m.place.naver.com{valid_links[0]}', headers=headers)

                parser = BeautifulSoup(data_2.content.decode('utf-8'), "lxml")
                main_tab = parser.select_one('div[class="place_fixed_maintab"]')
                if main_tab:
                    href_list = [
                        a['href']
                        for a in main_tab.select('a[href]')
                        if a['href'].strip() and not a['href'].strip().startswith('#')
                    ]
                    print(f"🍽️ [{index + 1}] {search_query} 유효한 링크 개수: {len(href_list)}")
                    print(f"🍽️ [{index + 1}] {search_query} 링크: {href_list}")
                else:
                    print(f"❌ [{index + 1}] {search_query} place_fixed_maintab not found.")

                place_info = extract_dynamic_place_info(parser)

                store_data = {
                    "place_info": place_info,
                    "unique_links": unique_links,
                    "tab_list": href_list,
                }

                pprint.pprint(store_data)
            except Exception as e:
                print(e)
    except Exception as error:
        print(error)

    finally:
        print("끝")


if __name__ == "__main__":
    useragent = f'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0/MUlvxiFHQPva'
    url = f'https://m.place.naver.com/restaurant/list?query=%EC%A0%95%EB%8B%B4%EC%86%90%EB%A7%8C%EB%91%90%20%EB%8C%80%EC%A0%84%EA%B4%91%EC%97%AD%EC%8B%9C%20%EC%A4%91%EA%B5%AC%20%EB%B3%B4%EB%AC%B8%EB%A1%9C&x=126&y=37'
    print(url)
    crawl(url, useragent)
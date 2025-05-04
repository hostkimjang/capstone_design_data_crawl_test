import sqlite3
import json
import os
import re
import time
import csv


def normalize(text: str) -> str:
    if not text:
        return ''
    text = re.sub(r'\(.*?\)', '', text)
    text = re.sub(r'\s+', '', text)
    text = re.sub(r'[^ㄱ-ㅣ가-힣\w]', '', text)
    return text.lower()


def extract_address_prefix(address: str, parts: int = 3) -> str:
    if not address:
        return ''
    tokens = address.split()
    return ' '.join(tokens[:parts]) if len(tokens) >= parts else address


def load_all_json_data(web_dir):
    json_files = sorted([
        os.path.join(web_dir, f) for f in os.listdir(web_dir) if f.endswith('.json')
    ])
    all_data = []
    for path in json_files:
        with open(path, 'r', encoding='utf-8') as f:
            all_data.extend(json.load(f))
    return all_data


def build_db_index_map():
    print("🔍 DB 레코드 인덱싱 중...")
    conn = sqlite3.connect('food_data.db')
    cursor = conn.cursor()

    cursor.execute("""
        SELECT 번호, 사업장명, 도로명전체주소 FROM restaurants
    """)
    mapping = {}
    for row in cursor.fetchall():
        name_key = normalize(row[1])
        road_key = normalize(extract_address_prefix(row[2]))
        mapping[(name_key, road_key)] = row[0]  # 번호

    conn.close()
    print(f"✅ DB 인덱싱 완료: {len(mapping)}건")
    return mapping


def enrich_json_with_ids(all_data, id_map):
    unmatched = []
    enriched = []

    for item in all_data:
        title = item.get("title", "").strip()
        query = item.get("query", "").strip()
        road_address = query[len(title):].strip() if query.startswith(title) else query
        road_prefix = extract_address_prefix(road_address)

        norm_key = (normalize(title), normalize(road_prefix))
        matched_id = id_map.get(norm_key)

        if matched_id:
            item["번호"] = matched_id
            enriched.append(item)
        else:
            unmatched.append({"title": title, "query": query})

    return enriched, unmatched


def save_enriched_json(enriched, out_path='web_data_enriched.json'):
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)
    print(f"📦 매칭된 {len(enriched)}건 저장 완료 → {out_path}")


def save_unmatched(unmatched, out_path='unmatched_log.csv'):
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['title', 'query'])
        writer.writeheader()
        writer.writerows(unmatched)
    print(f"⚠️ 매칭 실패 {len(unmatched)}건 → {out_path}")


def insert_enriched_data(json_path='web_data_enriched.json', db_path='food_merged_final.db'):
    with open(json_path, 'r', encoding='utf-8') as f:
        enriched = json.load(f)

    print("🔄 원본 DB 전체 레코드 로딩 중...")
    source_conn = sqlite3.connect('food_data.db')
    source_cursor = source_conn.cursor()
    source_cursor.execute("""
    SELECT 번호, 사업장명, 인허가일자, 영업상태명, 상세영업상태명,
           소재지전체주소, 도로명전체주소, 도로명우편번호,
           최종수정시점, 데이터갱신일자, 업태구분명
    FROM restaurants
    """)
    db_row_map = {row[0]: row[1:] for row in source_cursor.fetchall()}
    source_conn.close()

    print("🧱 최종 DB 테이블 준비 중...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS restaurant_merged (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        사업장명 TEXT,
        인허가일자 TEXT,
        영업상태명 TEXT,
        상세영업상태명 TEXT,
        소재지전체주소 TEXT,
        도로명전체주소 TEXT,
        도로명우편번호 REAL,
        최종수정시점 TEXT,
        데이터갱신일자 TEXT,
        업태구분명 TEXT,
        네이버_상호명 TEXT,
        네이버_주소 TEXT,
        네이버_전화번호 TEXT,
        네이버_URL TEXT,
        네이버_PLACE_ID_URL TEXT,
        네이버_place_info TEXT,
        네이버_tab_list TEXT
    )
    """)

    records_to_insert = []

    for item in enriched:
        print(f"진행중 {len(records_to_insert)}/{len(enriched)}건")
        no = item.get("번호")
        if no not in db_row_map:
            continue

        row = db_row_map[no]
        place = item.get("place_info", {})
        merged =  row + (
            place.get("title", ""),
            place.get("주소", ""),
            place.get("전화번호", ""),
            item.get("url", ""),
            item.get("unique_links", [""])[0],
            json.dumps(place, ensure_ascii=False),
            json.dumps(item.get("tab_list", []), ensure_ascii=False)
        )
        records_to_insert.append(merged)

    print(f"💾 최종 삽입 대상: {len(records_to_insert)}건")
    cursor.executemany("""
    INSERT INTO restaurant_merged (
        사업장명, 인허가일자, 영업상태명, 상세영업상태명,
        소재지전체주소, 도로명전체주소, 도로명우편번호,
        최종수정시점, 데이터갱신일자, 업태구분명,
        네이버_상호명, 네이버_주소, 네이버_전화번호,
        네이버_URL, 네이버_PLACE_ID_URL,
        네이버_place_info, 네이버_tab_list
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records_to_insert)
    conn.commit()
    conn.close()
    print("✅ 삽입 완료")

def db_crawl_process(json_path='web_data_enriched.json'):
    with open(json_path, 'r', encoding='utf-8') as f:
        enriched = json.load(f)

    print("🔄 원본 DB 전체 레코드 로딩 중...")
    source_conn = sqlite3.connect('food_data.db')
    source_cursor = source_conn.cursor()
    source_cursor.execute("""
    SELECT 번호 FROM restaurants
    """)
    existing_ids = {row[0] for row in source_cursor.fetchall()}

    crawl_ids = {item["번호"] for item in enriched if "번호" in item and item["번호"] in existing_ids}

    print("🧱 최종 DB 테이블 준비 중...")
    print(f"ID값이 있는 경우 CRAWL 값 업데이트 ({len(crawl_ids)}건)")

    batch_size = 900
    for i in range(0, len(crawl_ids), batch_size):
        batch = list(crawl_ids)[i:i + batch_size]
        qmarks = ','.join('?' for _ in batch)
        sql = f"UPDATE restaurants SET crawl = 1 WHERE 번호 IN ({qmarks})"
        source_cursor.execute(sql, batch)
        print(f"🔁 업데이트 진행 중: {i + len(batch)} / {len(crawl_ids)}")

    source_conn.commit()
    source_conn.close()
    print("✅ 전체 crawl 플래그 업데이트 완료.")


if __name__ == "__main__":
    start = time.time()

    print("📥 JSON 데이터 로드 중...")
    all_data = load_all_json_data("./tmp/web_data")

    print("🧠 ID 매칭 인덱스 준비...")
    id_map = build_db_index_map()
    enriched, unmatched = enrich_json_with_ids(all_data, id_map)

    save_enriched_json(enriched)
    save_unmatched(unmatched)

    print("📊 DB 삽입 시작...")
    insert_enriched_data()
    print("✅ DB 삽입 완료")

    # print("원본 DB CRAWL 값 업데이트 시작...")
    # db_crawl_process()
    # print("✅ 원본 DB CRAWL 값 업데이트 완료")

    end = time.time()
    print(f"⏰ 전체 소요 시간: {end - start:.2f}초")

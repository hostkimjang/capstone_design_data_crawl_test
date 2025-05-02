import pprint
import sqlite3
import json
import time
import os


def connect_merged_db() -> sqlite3.Connection:
    """
    Connect to the merged database and attach the original food_data.db for read access.
    """
    conn = sqlite3.connect('food_merged.db')
    conn.execute("ATTACH DATABASE 'food_data.db' AS orig;")  # 원본 DB attach
    return conn

def crawl_data_json_load(datapath):
    with open(datapath, 'r', encoding='utf-8') as f:
        return json.load(f)

def error_data_json_load(error_data_path):
    with open(error_data_path, 'r', encoding='utf-8') as f:
        return [json.loads(line.strip()) for line in f if line.strip()]

def create_merged_table(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS restaurant_merged (
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
    );
    """)


def crawl_data_process(datapath, error_data_path):
    conn = connect_merged_db()
    cursor = conn.cursor()
    create_merged_table(cursor)

    data = crawl_data_json_load(datapath)

    for idx, item in enumerate(data, 1):
        title = item.get("title", "").strip()
        query = item.get("query", "").strip()
        road_address = query[len(title):].strip()

        sql = f"""
        SELECT 사업장명, 인허가일자, 영업상태명, 상세영업상태명,
               소재지전체주소, 도로명전체주소, 도로명우편번호,
               최종수정시점, 데이터갱신일자, 업태구분명
        FROM orig.restaurants  -- ✅ 원본 DB에서 조회
        WHERE 사업장명 LIKE '{title}%'
          AND 도로명전체주소 LIKE '{road_address}%'
        LIMIT 1;
        """

        cursor.execute(sql)
        result = cursor.fetchone()
        if not result:
            print(f"[{idx}] ❌ No match for {title}")
            continue

        update_sql = f"""
        UPDATE orig.restaurants
        SET CRAWL = 1
        WHERE 사업장명 LIKE ? AND 도로명전체주소 LIKE ?
        """
        cursor.execute(update_sql, (f'{title}%', f'{road_address}%'))

        # 네이버 크롤링 데이터
        place = item.get("place_info", {})
        naver_name = place.get("title", "")
        naver_addr = place.get("주소", "")
        naver_tel = place.get("전화번호", "")
        naver_url = item.get("url", "")
        unique_links  = item.get("unique_links", [])
        place_info_json = json.dumps(item.get("place_info", {}), ensure_ascii=False)
        tab_list_json = json.dumps(item.get("tab_list", []), ensure_ascii=False)
        naver_place_id_url = unique_links[0] if unique_links else ""

        cursor.execute("""
        INSERT INTO restaurant_merged (
            사업장명, 인허가일자, 영업상태명, 상세영업상태명,
            소재지전체주소, 도로명전체주소, 도로명우편번호,
            최종수정시점, 데이터갱신일자, 업태구분명,
            네이버_상호명, 네이버_주소, 네이버_전화번호,
            네이버_URL, 네이버_PLACE_ID_URL,
            네이버_place_info, 네이버_tab_list
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, result + (
            naver_name, naver_addr, naver_tel,
            naver_url, naver_place_id_url,
            place_info_json, tab_list_json
        ))

    conn.commit()
    conn.close()
    print("✅ 병합 및 저장 완료")

def process_error_data(error_data_path):
    data = error_data_json_load(error_data_path)
    conn = connect_merged_db()
    cursor = conn.cursor()

    updated_count = 0
    for item in data:
        title = item.get("title", "").strip()
        query = item.get("query", "").strip()
        road_address = query[len(title):].strip()
        reason = item.get("reason", "Unknown")

        update_sql = """
        UPDATE orig.restaurants
        SET crawl_error = 1, error_reason = ?
        WHERE 사업장명 LIKE ? AND 도로명전체주소 LIKE ?
        """
        cursor.execute(update_sql, (reason, f'{title}%', f'{road_address}%'))
        updated_count += cursor.rowcount

    conn.commit()
    conn.close()
    print(f"✅ 오류 항목 {updated_count}건 업데이트 완료.")


if __name__ == "__main__":
    # web_data 디렉토리 내부 모든 .json 파일
    web_data_dir = 'web_data'
    web_files = sorted([
        os.path.join(web_data_dir, f)
        for f in os.listdir(web_data_dir)
        if f.endswith('.json')
    ])

    # error_logs 디렉토리 내부 모든 .jsonl 파일
    error_log_dir = 'error_logs'
    error_files = sorted([
        os.path.join(error_log_dir, f)
        for f in os.listdir(error_log_dir)
        if f.endswith('.jsonl')
    ])

    for datapath, error_data_path in zip(web_files, error_files):
        print(f"\n📁 처리 중: {datapath} / {error_data_path}")
        crawl_data_process(datapath, error_data_path)
        process_error_data(error_data_path)
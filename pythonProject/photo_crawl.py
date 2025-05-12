import requests
import time
import json
from datetime import datetime
import sqlite3
import re
from tqdm import tqdm
import os


GRAPHQL_URL = "https://api.place.naver.com/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
}

MAX_RETRIES = 5
RETRY_BACKOFF = 2  # seconds
MAX_PAGES = 10

DB_PATH = "./food_merged_final.db"
TABLE_NAME = "restaurant_merged"
BUSINESS_ID_COLUMN = "네이버_PLACE_ID_URL"

def log_failure(business_id, payload, status=None, error=None):
    with open("failed_requests.log", "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] ❌ {business_id} 요청 실패\n")
        f.write(f"Status: {status}\n" if status else "")
        f.write(f"Error: {error}\n" if error else "")
        f.write(f"Payload: {json.dumps(payload, ensure_ascii=False)}\n\n")

def make_payload(business_id, cursors):
    return {
        "operationName": "getPhotoViewerItems",
        "variables": {
            "input": {
                "businessId": business_id,
                "businessType": "restaurant",
                "cursors": cursors,
                "excludeAuthorIds": [],
                "excludeSection": [],
                "excludeClipIds": [],
                "dateRange": ""
            }
        },
        "query": """
        query getPhotoViewerItems($input: PhotoViewerInput) {
          photoViewer(input: $input) {
            cursors {
              id
              startIndex
              hasNext
              lastCursor
              __typename
            }
            photos {
              originalUrl
              width
              height
              desc
              author {
                nickname
              }
              video {
                videoId
                videoUrl
                trailerUrl
              }
              __typename
            }
            __typename
          }
        }
        """
    }

def safe_post_with_retry(business_id, payload):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(GRAPHQL_URL, json=payload, headers=HEADERS, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code in [429, 500, 502, 503, 504]:
                wait = RETRY_BACKOFF * attempt
                print(f"⏳ 서버 오류({response.status_code}) - {wait}s 후 재시도 ({attempt}/{MAX_RETRIES})")
                time.sleep(wait)
            else:
                print(f"❌ 요청 실패 - 상태코드: {response.status_code}")
                log_failure(business_id, payload, status=response.status_code)
                break
        except requests.RequestException as e:
            wait = RETRY_BACKOFF * attempt
            print(f"⚠️ 예외 발생: {e} - {wait}s 후 재시도 ({attempt}/{MAX_RETRIES})")
            time.sleep(wait)
    log_failure(business_id, payload, error="최대 재시도 초과")
    return None
def crawl_photos(business_id, max_pages=5):
    all_photos = []
    cursors = [
        {"id": "biz"},
        {"id": "cp0"},
        {"id": "visitorReview"},
        {"id": "clip"},
        {"id": "imgSas"}
    ]

    for page in range(max_pages):
        print(f"📄 [{business_id}] 페이지 {page+1} 요청 중...")
        payload = make_payload(business_id, cursors)
        data = safe_post_with_retry(business_id, payload)

        if not data:
            break

        viewer = data.get('data', {}).get('photoViewer', {})
        photos = viewer.get('photos') or []
        cursors_data = viewer.get('cursors', [])

        if not photos:
            print(f"\n⚠️ [{business_id}] 사진 없음, 스킵")
            break

        for photo in photos:
            if not photo:
                continue
            author = photo.get("author") or {}
            all_photos.append({
                "url": photo.get("originalUrl"),
                "desc": photo.get("desc"),
                "author": author.get("nickname"),
                "video": photo.get("video")
            })

        # 다음 커서 확인
        next_cursor = None
        for cursor in cursors_data:
            if cursor["id"] == "biz" and cursor.get("hasNext") and cursor.get("lastCursor"):
                next_cursor = cursor["lastCursor"]
        if not next_cursor:
            print(f"\n✅ [{business_id}] 다음 커서 없음, 종료")
            break

        cursors[0] = {"id": "biz", "lastCursor": next_cursor}
        time.sleep(1)

    return all_photos

def save_jsonl(business_id, photos):
    os.makedirs("crawl_photo", exist_ok=True)  # 디렉토리 없으면 생성
    filename = os.path.join("crawl_photo", f"photo_{business_id}.jsonl")
    with open(filename, "w", encoding="utf-8") as f:
        for p in photos:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")

def load_business_ids(limit=10):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT DISTINCT {BUSINESS_ID_COLUMN}
        FROM {TABLE_NAME}
        WHERE {BUSINESS_ID_COLUMN} IS NOT NULL
        LIMIT ?
    """, (limit,))
    rows = cursor.fetchall()
    conn.close()

    # 정규식으로 숫자만 추출
    ids = []
    for row in rows:
        match = re.search(r'/restaurant/(\d+)', row[0])
        if match:
            ids.append(match.group(1))
    return ids


def main():
    business_ids = load_business_ids(limit=100)

    total = len(business_ids)
    success_count = 0
    skip_count = 0
    error_count = 0

    for bid in tqdm(business_ids, desc="📦 전체 업체 처리"):
        print(f"\n🚀 크롤링 시작: {bid}")
        try:
            photos = crawl_photos(bid, max_pages=MAX_PAGES)
            if photos:
                save_jsonl(bid, photos)
                print(f"✅ 저장 완료: photo_{bid}.jsonl ({len(photos)}장)")
                success_count += 1
            else:
                print(f"⚠️ 사진 없음 또는 실패: {bid}")
                skip_count += 1
        except Exception as e:
            print(f"❌ 예외로 건너뜀: {bid} → {e}")
            error_count += 1
            continue

    # 📊 결과 요약 출력
    print("\n📊 크롤링 요약")
    print(f"🔢 전체 업체 수: {total}")
    print(f"✅ 성공 (사진 존재): {success_count}")
    print(f"⚠️ 없음 (사진 없음): {skip_count}")
    print(f"❌ 실패 (에러 건너뜀): {error_count}")


if __name__ == "__main__":
    main()


import asyncio
import json
import os
import random
import re
import sqlite3
import time
import requests
from datetime import datetime
from tqdm import tqdm
from playwright.async_api import async_playwright

DB_PATH = "/app/food_merged_final.db"  # Docker 환경의 DB 경로
TABLE_NAME = "restaurant_merged"
BUSINESS_ID_COLUMN = "네이버_PLACE_ID_URL"
MAX_PAGES = 10
MAX_RETRIES = 3
MIN_DELAY = 0.5  # 분산처리를 위해 딜레이 최소화
MAX_DELAY = 2
GRAPHQL_URL = "https://api.place.naver.com/graphql"

# GraphQL 쿼리 데이터
PHOTO_QUERY = """
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
      viewId
      originalUrl
      originalDate
      width
      height
      title
      text
      desc
      link
      date
      photoType
      mediaType
      option {
        channelName
        dateString
        playCount
        likeCount
        __typename
      }
      to
      relation
      logId
      author {
        id
        nickname
        from
        imageUrl
        objectId
        url
        borderImageUrl
        __typename
      }
      votedKeywords {
        code
        iconUrl
        iconCode
        name
        __typename
      }
      visitCount
      originType
      isFollowing
      businessName
      rating
      externalLink {
        title
        url
        __typename
      }
      sourceTitle
      moment {
        channelId
        contentId
        momentId
        gdid
        blogRelation
        statAllowYn
        category
        docNo
        __typename
      }
      video {
        videoId
        videoUrl
        trailerUrl
        __typename
      }
      music {
        artists
        title
        __typename
      }
      clip {
        serviceType
        createdAt
        __typename
      }
      __typename
    }
    __typename
  }
}
"""

def log_failure(business_id, payload, error=None):
    with open("failed_requests.log", "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] ❌ {business_id} 요청 실패\n")
        if error:
            f.write(f"Error: {error}\n")
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
        "query": PHOTO_QUERY
    }

def save_jsonl(filename, photos, output_path):
    filepath = os.path.join(output_path, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        for p in photos:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")

def load_business_ids_range(start=0, end=100):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT id, {BUSINESS_ID_COLUMN}
            FROM {TABLE_NAME}
            WHERE {BUSINESS_ID_COLUMN} IS NOT NULL
            LIMIT ? OFFSET ?
        """, (end - start, start))
        rows = cursor.fetchall()
        conn.close()
        ids = []
        for db_id, url in rows:
            match = re.search(r'/restaurant/(\d+)', url)
            if match:
                business_id = match.group(1)
                ids.append((db_id, business_id))
        return ids
    except Exception as e:
        print(f"❌ DB 접근 오류: {e}")
        return []

def random_delay():
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    time.sleep(delay)
    return delay

def make_request_with_cookies(business_id, cursors, browser_data, retry_count=0):
    """브라우저에서 획득한 쿠키를 사용하여 requests로 요청"""
    try:
        payload = make_payload(business_id, cursors)
        
        # 헤더 설정
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': browser_data['user_agent'],
            'Referer': f'https://m.place.naver.com/restaurant/{business_id}/photo',
            'Origin': 'https://m.place.naver.com',
            'Accept-Language': 'ko-KR,ko;q=0.9',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Sec-Ch-Ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"macOS"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site'
        }
        
        # 요청 보내기
        response = requests.post(
            GRAPHQL_URL,
            json=payload,
            headers=headers,
            cookies=browser_data['cookies'],
            timeout=10
        )
        
        # 응답 확인
        if response.status_code != 200:
            print(f"⚠️ API 응답 오류: {response.status_code}")
            if retry_count < MAX_RETRIES:
                retry_delay = min(2 * (retry_count + 1), 10)  # 최대 10초까지 대기
                print(f"🔄 {retry_delay:.1f}초 후 재시도 ({retry_count + 1}/{MAX_RETRIES})...")
                time.sleep(retry_delay)
                return make_request_with_cookies(business_id, cursors, browser_data, retry_count + 1)
            return None
        
        data = response.json()
        return data
        
    except Exception as e:
        print(f"❌ 요청 예외: {e}")
        if retry_count < MAX_RETRIES:
            retry_delay = min(2 * (retry_count + 1), 10)
            print(f"🔄 {retry_delay:.1f}초 후 재시도 ({retry_count + 1}/{MAX_RETRIES})...")
            time.sleep(retry_delay)
            return make_request_with_cookies(business_id, cursors, browser_data, retry_count + 1)
        return None

async def extract_cookies_from_browser(page, business_id=None):
    """Playwright를 사용하여 브라우저에서 쿠키를 추출하는 함수"""
    try:
        # 유효한 URL 설정
        url = f"https://m.place.naver.com/restaurant/{business_id}/home" if business_id else "https://m.place.naver.com/restaurant/list"
        
        # 페이지 이동
        await page.goto(url, wait_until="networkidle", timeout=30000)
        print(f"🔄 세션 초기화 중... (URL: {url})")
        
        # 페이지와 상호작용하여 더 많은 쿠키 생성
        await page.evaluate("""
            () => {
                window.scrollTo(0, 200);
                setTimeout(() => window.scrollTo(0, 400), 300);
                setTimeout(() => window.scrollTo(0, 100), 600);
            }
        """)
        await asyncio.sleep(1)
        
        # Playwright에서 쿠키 가져오기
        cookies = await page.context.cookies()
        
        # 쿠키를 딕셔너리로 변환
        cookie_dict = {}
        for cookie in cookies:
            if '.naver.com' in cookie.get('domain', ''):
                cookie_dict[cookie['name']] = cookie['value']
        
        # 쿠키가 비어있으면 기본값 설정
        if not cookie_dict:
            print("⚠️ 쿠키를 가져올 수 없습니다. 기본 쿠키 사용...")
            cookie_dict = {
                "NNB": "XLXNYI5U5MBTA",
                "PLACE_LANGUAGE": "ko"
            }
            
            # 기본 쿠키 설정
            await page.context.add_cookies([
                {"name": "NNB", "value": "XLXNYI5U5MBTA", "domain": ".naver.com", "path": "/"},
                {"name": "PLACE_LANGUAGE", "value": "ko", "domain": ".place.naver.com", "path": "/"}
            ])
        
        # 현재 user-agent 가져오기
        user_agent = await page.evaluate("navigator.userAgent")
        
        return {
            'cookies': cookie_dict,
            'user_agent': user_agent
        }
    except Exception as e:
        print(f"❌ 쿠키 추출 중 오류 발생: {e}")
        # 오류 발생 시 기본 쿠키 반환
        return {
            'cookies': {
                "NNB": "XLXNYI5U5MBTA",
                "PLACE_LANGUAGE": "ko"
            },
            'user_agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        }

async def refresh_browser_cookies(page, business_id):
    """브라우저 쿠키를 새로고침"""
    try:
        url = f"https://m.place.naver.com/restaurant/{business_id}/home"
        await page.goto(url, wait_until="networkidle", timeout=30000)
        print(f"🔄 브라우저 세션 갱신 중... (URL: {url})")
        
        # 상호작용
        await page.evaluate("""
            () => {
                window.scrollTo(0, 200);
                setTimeout(() => window.scrollTo(0, 400), 300);
                setTimeout(() => window.scrollTo(0, 150), 600);
            }
        """)
        await asyncio.sleep(1)
        
        # 쿠키 가져오기
        cookies = await page.context.cookies()
        
        # 쿠키를 딕셔너리로 변환
        cookie_dict = {}
        for cookie in cookies:
            if '.naver.com' in cookie.get('domain', ''):
                cookie_dict[cookie['name']] = cookie['value']
        
        # 쿠키가 비어있으면 기본값 설정
        if not cookie_dict:
            print("⚠️ 갱신 중 쿠키를 가져올 수 없습니다. 기본 쿠키 사용...")
            cookie_dict = {
                "NNB": "XLXNYI5U5MBTA",
                "PLACE_LANGUAGE": "ko"
            }
        
        user_agent = await page.evaluate("navigator.userAgent")
        
        return {
            'cookies': cookie_dict,
            'user_agent': user_agent
        }
    except Exception as e:
        print(f"❌ 쿠키 갱신 중 오류 발생: {e}")
        # 오류 발생 시 기본 쿠키 반환
        return {
            'cookies': {
                "NNB": "XLXNYI5U5MBTA",
                "PLACE_LANGUAGE": "ko"
            },
            'user_agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        }

async def main():
    # 환경 변수에서 범위 가져오기 (Docker 환경)
    start = int(os.environ.get("START_INDEX", 0))
    end = int(os.environ.get("END_INDEX", 100))
    print(f"📦 현재 컨테이너는 {start} ~ {end} 범위를 담당합니다.")

    # 비즈니스 ID 로딩
    business_ids = load_business_ids_range(start, end)
    if not business_ids:
        print("⚠️ 가져올 업체 ID가 없습니다. 종료합니다.")
        return
    
    total = len(business_ids)
    print(f"🔢 총 {total}개 업체를 처리합니다.")

    # 출력 디렉토리 설정
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(BASE_DIR, "crawl_photo")
    os.makedirs(output_path, exist_ok=True)

    # 결과 카운터 초기화
    success_count = 0
    skip_count = 0
    error_count = 0
    cookie_refresh_count = 0

    # Playwright 초기화 - Docker에서 실행 시 headless 모드 필수
    print("🌐 Playwright 초기화 중...")
    async with async_playwright() as p:
        # 브라우저 시작 (Docker 환경에서는 headless=True 필수)
        browser = await p.chromium.launch(headless=True)
        
        # 컨텍스트 생성
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        )
        
        # 첫 페이지 생성
        page = await context.new_page()
        
        # 첫 번째 업체 정보로 초기 쿠키 획득
        if business_ids:
            first_db_id, first_business_id = business_ids[0]
            print(f"🔑 첫 번째 업체({first_business_id})로 초기 쿠키 획득 중...")
            browser_data = await extract_cookies_from_browser(page, first_business_id)
            print(f"📝 쿠키 획득 완료: {len(browser_data['cookies'])} 개의 쿠키")
        else:
            print("⚠️ 업체 ID가 없어 기본 쿠키를 사용합니다.")
            browser_data = {
                'cookies': {
                    "NNB": "XLXNYI5U5MBTA",
                    "PLACE_LANGUAGE": "ko"
                },
                'user_agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
            }

        # 각 업체에 대해 크롤링 수행
        for i, (db_id, business_id) in enumerate(tqdm(business_ids, desc="📦 범위 내 업체 처리")):
            try:
                print(f"\n🚀 크롤링 시작: {business_id} (DB ID={db_id})")
                
                # 주기적으로 쿠키 리프레시 (10개 업체마다)
                if i > 0 and i % 10 == 0:
                    print("🔄 브라우저 쿠키 리프레시 중...")
                    browser_data = await refresh_browser_cookies(page, business_id)
                    cookie_refresh_count += 1
                    print(f"📝 쿠키 리프레시 완료 (#{cookie_refresh_count})")
                
                # 모든 사진을 저장할 리스트
                all_photos = []
                
                # 시작 커서 설정
                cursors = [
                    {"id": "biz"},
                    {"id": "cp0"},
                    {"id": "visitorReview"},
                    {"id": "clip"},
                    {"id": "imgSas"}
                ]

                # 페이지별로 사진 가져오기
                for page_num in range(MAX_PAGES):
                    print(f"📄 페이지 {page_num + 1} 요청 중...")
                    
                    # requests로 요청 (브라우저 쿠키 사용)
                    data = make_request_with_cookies(business_id, cursors, browser_data)
                    
                    # 요청 실패 시 쿠키 갱신 후 재시도
                    if not data:
                        print(f"⚠️ 요청 실패 → 브라우저 쿠키 갱신 후 재시도")
                        browser_data = await refresh_browser_cookies(page, business_id)
                        cookie_refresh_count += 1
                        
                        # 쿠키 갱신 후 재시도
                        data = make_request_with_cookies(business_id, cursors, browser_data)
                        if not data:
                            print(f"⚠️ 재시도 실패, 건너뜀")
                            skip_count += 1
                            break

                    # 응답에서 사진 정보 추출
                    viewer = data.get('data', {}).get('photoViewer', {})
                    photos = viewer.get('photos') or []
                    cursors_data = viewer.get('cursors', [])

                    # 사진이 없으면 종료
                    if not photos:
                        print(f"⚠️ 사진 없음, 종료")
                        break

                    # 각 사진 정보 저장
                    for photo in photos:
                        author = photo.get("author") or {}
                        all_photos.append({
                            "url": photo.get("originalUrl"),
                            "desc": photo.get("desc"),
                            "author": author.get("nickname"),
                            "video": photo.get("video"),
                            "width": photo.get("width"),
                            "height": photo.get("height"),
                            "date": photo.get("date"),
                            "viewId": photo.get("viewId")
                        })

                    # 다음 페이지 커서 확인
                    next_cursor = None
                    for cursor in cursors_data:
                        if cursor["id"] == "biz" and cursor.get("hasNext") and cursor.get("lastCursor"):
                            next_cursor = cursor["lastCursor"]
                    
                    # 다음 페이지가 없으면 종료
                    if not next_cursor:
                        print(f"✅ 다음 커서 없음, 종료")
                        break
                        
                    # 다음 페이지 커서 설정
                    cursors[0] = {"id": "biz", "lastCursor": next_cursor}
                    
                    # 페이지 간 딜레이
                    delay = random_delay()
                    print(f"⏱️ 다음 페이지 요청까지 {delay:.1f}초 대기 중...")

                # 사진 저장
                if all_photos:
                    filename = f"{db_id}_photo_{business_id}.jsonl"
                    save_jsonl(filename, all_photos, output_path)
                    print(f"✅ 저장 완료: {filename} ({len(all_photos)}장)")
                    success_count += 1
                else:
                    print(f"⚠️ 수집된 사진 없음")
                    skip_count += 1
                    
                # 업체 간 딜레이
                if db_id != business_ids[-1][0]:  # 마지막 업체가 아니면
                    delay = random.uniform(1, 3)
                    print(f"⏱️ 다음 업체 크롤링까지 {delay:.1f}초 대기 중...")
                    time.sleep(delay)

            except Exception as e:
                print(f"❌ 예외 발생: {e}")
                log_failure(business_id, {}, error=str(e))
                error_count += 1
                # 에러 후 복구 시간
                time.sleep(random.uniform(3, 5))
                continue

        # 브라우저 종료
        await browser.close()

    # 결과 요약
    print("\n📊 크롤링 요약")
    print(f"🔢 전체 업체 수: {total}")
    print(f"✅ 성공: {success_count}")
    print(f"⚠️ 스킵: {skip_count}")
    print(f"❌ 실패: {error_count}")
    print(f"🔄 쿠키 리프레시: {cookie_refresh_count}회")

if __name__ == "__main__":
    asyncio.run(main())

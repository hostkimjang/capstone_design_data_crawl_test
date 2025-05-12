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

DB_PATH = "./food_merged_final.db"
TABLE_NAME = "restaurant_merged"
BUSINESS_ID_COLUMN = "네이버_PLACE_ID_URL"
MAX_PAGES = 10
MAX_RETRIES = 3
MIN_DELAY = 1
MAX_DELAY = 3

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

def random_delay():
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    time.sleep(delay)
    return delay

async def extract_cookies_from_browser(page, business_id=None):
    """Playwright를 사용하여 브라우저에서 쿠키를 추출하는 함수"""
    try:
        # 유효한 URL 설정
        if business_id:
            url = f"https://m.place.naver.com/restaurant/{business_id}/home"
        else:
            # 레스토랑 목록 페이지
            url = "https://m.place.naver.com/restaurant/list"
        
        # 페이지 이동
        await page.goto(url, wait_until="networkidle")
        print(f"🔄 세션 초기화 중... (URL: {url})")
        
        # 페이지와 상호작용하여 더 많은 쿠키 생성
        # 스크롤
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
        
        print(f"🍪 Playwright로 {len(cookie_dict)}개 쿠키 추출됨")
        
        # 쿠키가 비어있으면 기본값 설정
        if not cookie_dict:
            print("⚠️ 쿠키를 가져올 수 없습니다. 기본 쿠키 사용...")
            cookie_dict = {
                "NNB": "XLXNYI5U5MBTA",  # 임의의 기본값
                "PLACE_LANGUAGE": "ko"
            }
            
            # 기본 쿠키 설정
            await page.context.add_cookies([
                {"name": "NNB", "value": "XLXNYI5U5MBTA", "domain": ".naver.com", "path": "/"},
                {"name": "PLACE_LANGUAGE", "value": "ko", "domain": ".place.naver.com", "path": "/"}
            ])
        
        # 로컬 스토리지에서 중요 정보 확인
        local_storage = await page.evaluate("""
            () => {
                try {
                    const storage = {};
                    for (let i = 0; i < localStorage.length; i++) {
                        const key = localStorage.key(i);
                        storage[key] = localStorage.getItem(key);
                    }
                    return storage;
                } catch (e) {
                    console.error('로컬 스토리지 접근 오류:', e);
                    return {};
                }
            }
        """)
        
        if local_storage and isinstance(local_storage, dict):
            print(f"📦 로컬 스토리지 {len(local_storage)}개 항목 확인됨")
            # 필요한 경우 로컬 스토리지에서 중요 정보 쿠키로 복사
            for key in ['NNB', 'NID_AUT', 'NID_SES', 'nx_ssl']:
                if key in local_storage and key not in cookie_dict:
                    cookie_dict[key] = local_storage[key]
                    print(f"🔑 로컬 스토리지에서 '{key}' 쿠키로 복사")
                    
                    # 쿠키로 설정
                    await page.context.add_cookies([
                        {"name": key, "value": local_storage[key], "domain": ".naver.com", "path": "/"}
                    ])
        
        # 현재 user-agent 가져오기
        user_agent = await page.evaluate("navigator.userAgent")
        
        print(f"🍪 최종 획득한 쿠키 키: {', '.join(list(cookie_dict.keys())[:5])}...")
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
        await page.goto(url, wait_until="networkidle")
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
        
        print(f"🍪 새로고침으로 {len(cookie_dict)}개 쿠키 추출됨")
        
        # 쿠키가 비어있으면 기본값 설정
        if not cookie_dict:
            print("⚠️ 갱신 중 쿠키를 가져올 수 없습니다. 기본 쿠키 사용...")
            cookie_dict = {
                "NNB": "XLXNYI5U5MBTA",
                "PLACE_LANGUAGE": "ko"
            }
        
        user_agent = await page.evaluate("navigator.userAgent")
        
        print(f"🍪 갱신된 쿠키 키: {', '.join(list(cookie_dict.keys())[:5])}...")
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
        
        # 디버깅: 요청 전 헤더와 쿠키 출력
        print("\n🔍 디버깅 정보 - API 요청")
        print(f"📋 요청 URL: https://api.place.naver.com/graphql")
        print(f"📋 비즈니스 ID: {business_id}")
        # print(f"📋 헤더 정보:")
        # for k, v in headers.items():
        #     print(f"   - {k}: {v}")
        # print(f"📋 쿠키 정보 ({len(browser_data['cookies'])}개):")
        # for k, v in browser_data['cookies'].items():
        #     # 값이 너무 길면 일부만 표시
        #     display_value = v[:30] + "..." if len(v) > 30 else v
        #     print(f"   - {k}: {display_value}")
        
        # 요청 보내기
        response = requests.post(
            "https://api.place.naver.com/graphql",
            json=payload,
            headers=headers,
            cookies=browser_data['cookies'],
            timeout=10
        )
        
        # 응답 확인
        if response.status_code != 200:
            print(f"⚠️ API 응답 오류: {response.status_code}")
            print(f"📋 응답 내용: {response.text[:200]}...")
            if retry_count < MAX_RETRIES:
                retry_delay = random.uniform(1, 3) * (retry_count + 1)
                print(f"🔄 {retry_delay:.1f}초 후 재시도 ({retry_count + 1}/{MAX_RETRIES})...")
                time.sleep(retry_delay)
                return make_request_with_cookies(business_id, cursors, browser_data, retry_count + 1)
            return None
        
        data = response.json()
        
        # 디버깅: 응답 데이터 일부 출력
        print(f"📋 응답 상태 코드: {response.status_code}")
        if data and 'data' in data and 'photoViewer' in data['data']:
            photo_count = len(data['data']['photoViewer'].get('photos', []))
            print(f"📋 응답에서 사진 개수: {photo_count}장")
        
        return data
        
    except Exception as e:
        print(f"❌ 요청 예외: {e}")
        if retry_count < MAX_RETRIES:
            retry_delay = random.uniform(1, 3) * (retry_count + 1)
            print(f"🔄 {retry_delay:.1f}초 후 재시도 ({retry_count + 1}/{MAX_RETRIES})...")
            time.sleep(retry_delay)
            return make_request_with_cookies(business_id, cursors, browser_data, retry_count + 1)
        return None

async def main():
    start = int(os.environ.get("START_INDEX", 0))
    end = int(os.environ.get("END_INDEX", 100))
    print(f"📦 현재 컨테이너는 {start} ~ {end} 범위를 담당합니다.")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(BASE_DIR, "crawl_photo")
    os.makedirs(output_path, exist_ok=True)

    business_ids = load_business_ids_range(start, end)
    if not business_ids:
        print("⚠️ 가져올 업체 ID가 없습니다. 종료합니다.")
        return

    # Playwright 초기화
    print("🌐 Playwright 초기화 중...")
    async with async_playwright() as p:
        # 브라우저 시작 - 헤드리스 모드 해제 (브라우저 화면 표시)
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        )
        
        # 브라우저 창 검사를 위한 대기시간
        print("⚠️ 브라우저가 표시됩니다. 수동으로 로그인하려면 지금 로그인하세요.")
        print("⏱️ 10초 후에 자동 크롤링이 시작됩니다...")
        await asyncio.sleep(10)  # 사용자가 브라우저를 확인하고 필요시 수동 로그인할 시간
        
        # 첫 페이지 생성
        page = await context.new_page()
        
        # 첫 번째 업체 정보로 초기 쿠키 획득
        first_db_id, first_business_id = business_ids[0]
        print(f"🔑 첫 번째 업체({first_business_id})로 초기 쿠키 획득 중...")
        browser_data = await extract_cookies_from_browser(page, first_business_id)
        
        # 디버깅: 획득한 모든 쿠키 상세 출력
        print("\n🔍 디버깅 정보 - 획득한 쿠키")
        print(f"📋 쿠키 개수: {len(browser_data['cookies'])}")
        print(f"📋 상세 쿠키 목록:")
        for k, v in browser_data['cookies'].items():
            # 값이 너무 길면 일부만 표시
            display_value = v[:30] + "..." if len(v) > 30 else v
            print(f"   - {k}: {display_value}")
        print(f"📋 User-Agent: {browser_data['user_agent']}")
        
        print(f"📝 쿠키 획득 완료: {len(browser_data['cookies'])} 개의 쿠키")

        # 계속 진행할지 확인
        print("\n⚠️ 획득한 쿠키로 크롤링을 시작합니다.")
        print("⏱️ 5초 후에 자동 크롤링이 시작됩니다...")
        await asyncio.sleep(5)  # 사용자가 쿠키 정보를 확인할 시간
        
        success_count = 0
        skip_count = 0
        error_count = 0
        cookie_refresh_count = 0

        for i, (db_id, business_id) in enumerate(tqdm(business_ids, desc="📦 범위 내 업체 처리")):
            try:
                print(f"\n🚀 크롤링 시작: {business_id} (DB ID={db_id})")
                
                # 주기적으로 쿠키 리프레시 (10개 업체마다)
                if i > 0 and i % 10 == 0:
                    print("🔄 브라우저 쿠키 리프레시 중...")
                    browser_data = await refresh_browser_cookies(page, business_id)
                    cookie_refresh_count += 1
                    
                    # 디버깅: 리프레시된 모든 쿠키 상세 출력
                    print("\n🔍 디버깅 정보 - 리프레시된 쿠키")
                    print(f"📋 쿠키 개수: {len(browser_data['cookies'])}")
                    print(f"📋 상세 쿠키 목록:")
                    for k, v in browser_data['cookies'].items():
                        # 값이 너무 길면 일부만 표시
                        display_value = v[:30] + "..." if len(v) > 30 else v
                        print(f"   - {k}: {display_value}")
                    
                    print(f"📝 쿠키 리프레시 완료 (#{cookie_refresh_count})")
                
                all_photos = []
                cursors = [
                    {"id": "biz"},
                    {"id": "cp0"},
                    {"id": "visitorReview"},
                    {"id": "clip"},
                    {"id": "imgSas"}
                ]

                for page_num in range(MAX_PAGES):
                    print(f"📄 페이지 {page_num + 1} 요청 중...")
                    
                    # requests로 요청 (브라우저 쿠키 사용)
                    data = make_request_with_cookies(business_id, cursors, browser_data)
                    
                    if not data:
                        print(f"⚠️ fetch() 실패 or 응답 없음 → 브라우저 쿠키 갱신 후 재시도")
                        browser_data = await refresh_browser_cookies(page, business_id)
                        cookie_refresh_count += 1
                        
                        # 쿠키 갱신 후 재시도
                        data = make_request_with_cookies(business_id, cursors, browser_data)
                        if not data:
                            print(f"⚠️ 재시도 실패, 건너뜀")
                            skip_count += 1
                            break

                    viewer = data.get('data', {}).get('photoViewer', {})
                    photos = viewer.get('photos') or []
                    cursors_data = viewer.get('cursors', [])

                    if not photos:
                        print(f"⚠️ 사진 없음, 종료")
                        break

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

                    next_cursor = None
                    for cursor in cursors_data:
                        if cursor["id"] == "biz" and cursor.get("hasNext") and cursor.get("lastCursor"):
                            next_cursor = cursor["lastCursor"]
                    if not next_cursor:
                        print(f"✅ 다음 커서 없음, 종료")
                        break
                    cursors[0] = {"id": "biz", "lastCursor": next_cursor}
                    
                    # 페이지 간 딜레이 랜덤화
                    delay = random_delay()
                    print(f"⏱️ 다음 페이지 요청까지 {delay:.1f}초 대기 중...")

                if all_photos:
                    filename = f"{db_id}_photo_{business_id}.jsonl"
                    save_jsonl(filename, all_photos, output_path)
                    print(f"✅ 저장 완료: {filename} ({len(all_photos)}장)")
                    success_count += 1
                else:
                    print(f"⚠️ 수집된 사진 없음")
                    skip_count += 1
                    
                # 업체 간 딜레이 랜덤화
                if db_id != business_ids[-1][0]:  # 마지막 업체가 아니면
                    delay = random.uniform(0, 1)
                    print(f"⏱️ 다음 업체 크롤링까지 {delay:.1f}초 대기 중...")
                    time.sleep(delay)

            except Exception as e:
                print(f"❌ 예외 발생: {e}")
                log_failure(business_id, {}, error=str(e))
                error_count += 1
                # 에러 후 복구 시간
                time.sleep(random.uniform(5, 10))
                continue

        print("\n📊 크롤링 요약")
        print(f"✅ 성공: {success_count}")
        print(f"⚠️ 스킵: {skip_count}")
        print(f"❌ 실패: {error_count}")
        print(f"🔄 쿠키 리프레시: {cookie_refresh_count}회")
        
        # 브라우저 종료
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())

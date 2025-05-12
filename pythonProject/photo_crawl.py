import asyncio
import json
import os
import random
import re
import sqlite3
import time
from datetime import datetime
from tqdm import tqdm
from playwright.async_api import async_playwright

DB_PATH = "food_merged_final.db"
TABLE_NAME = "restaurant_merged"
BUSINESS_ID_COLUMN = "네이버_PLACE_ID_URL"
MAX_SCROLL = 10
MIN_DELAY = 1.5
MAX_DELAY = 3

PHOTO_QUERY_KEY = "photoViewer"


def log_failure(business_id, error=None):
    with open("failed_requests.log", "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now()}] ❌ {business_id} 요청 실패\n")
        if error:
            f.write(f"Error: {error}\n\n")


def load_business_ids_range(start=0, end=100):
    try:
        import sqlite3
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


def save_jsonl(filename, items, output_path):
    filepath = os.path.join(output_path, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        for p in items:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")


def random_delay():
    delay = random.uniform(MIN_DELAY, MAX_DELAY)
    time.sleep(delay)
    return delay


async def intercept_and_save_graphql(page, business_id, output_path):
    """photoViewer GraphQL 응답을 가로채서 사진 정보만 저장 (이벤트 핸들러 내에서 즉시 처리)"""
    photo_items = []
    seen_view_ids = set()
    protocol_error_count = 0

    async def handle_response(response):
        nonlocal protocol_error_count
        try:
            if 'graphql' in response.url and response.request.method == 'POST':
                try:
                    text = await response.text()
                except Exception as e:
                    protocol_error_count += 1
                    print(f"파싱 실패(즉시): {e}")
                    return
                try:
                    data = json.loads(text)
                except Exception:
                    # 여러 JSON 오브젝트가 콤마로 이어진 경우(비표준 JSONL)
                    try:
                        data = [json.loads(line) for line in text.splitlines() if line.strip()]
                    except Exception as e:
                        print(f"JSON 파싱 실패: {e}")
                        return
                if isinstance(data, list):
                    datas = data
                else:
                    datas = [data]
                photo_found = False
                for d in datas:
                    viewer = d.get('data', {}).get(PHOTO_QUERY_KEY, {})
                    if isinstance(viewer, list):
                        viewers = viewer
                    else:
                        viewers = [viewer]
                    for v in viewers:
                        photos = v.get('photos', [])
                        if photos:
                            photo_found = True
                            for photo in photos:
                                if not isinstance(photo, dict):
                                    continue
                                view_id = photo.get("viewId")
                                if view_id and view_id not in seen_view_ids:
                                    seen_view_ids.add(view_id)
                                    author = photo.get("author") or {}
                                    photo_items.append({
                                        "url": photo.get("originalUrl"),
                                        "desc": photo.get("desc"),
                                        "author": author.get("nickname"),
                                        "video": photo.get("video"),
                                        "width": photo.get("width"),
                                        "height": photo.get("height"),
                                        "date": photo.get("date"),
                                        "viewId": view_id
                                    })
                if not photo_found:
                    print(f"⚠️ 수집된 사진 없음")
        except Exception as e:
            print(f"파싱 실패(핸들러): {e}")

    page.on('response', handle_response)
    # 스크롤 및 네트워크 응답 대기
    await asyncio.sleep(0.5)  # 첫 로딩 대기
    return photo_items


async def debug_graphql_network(page, business_id):
    async def on_request(request):
        if "graphql" in request.url:
            print(f"[REQUEST] {request.method} {request.url}")
            # print(f"Headers: {request.headers}")
            try:
                post_data = await request.post_data()
                print(f"Post Data: {post_data}")
            except Exception:
                pass

    async def on_response(response):
        if "graphql" in response.url:
            print(f"[RESPONSE] {response.status} {response.url}")
            # try:
            #     json_data = await response.json()
            #     print(f"Response JSON keys: {list(json_data.keys())}")
            # except Exception:
            #     print("응답이 JSON이 아님")
            #     try:
            #         text = await response.text()
            #         print(f"응답 본문 일부: {text[:300]}")
            #     except Exception as e:
            #         print(f"본문 출력 실패: {e}")

    page.on("request", on_request)
    page.on("response", on_response)

    url = f"https://m.place.naver.com/restaurant/{business_id}/photo"
    await page.goto(url, wait_until="networkidle")
    await asyncio.sleep(1)  # 네트워크 요청 충분히 기다림


async def block_images(context):
    async def handle_route(route, request):
        if request.resource_type == "image":
            await route.abort()
        else:
            await route.continue_()
    await context.route("**/*", handle_route)


async def main():
    start = int(os.environ.get("START_INDEX", 0))
    end = int(os.environ.get("END_INDEX", 100))
    print(f"📦 현재 컨테이너는 {start} ~ {end} 범위를 담당합니다.")

    business_ids = load_business_ids_range(start, end)
    if not business_ids:
        print("⚠️ 가져올 업체 ID가 없습니다. 종료합니다.")
        return

    total = len(business_ids)
    print(f"🔢 총 {total}개 업체를 처리합니다.")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(BASE_DIR, "crawl_photo")
    os.makedirs(output_path, exist_ok=True)

    success_count = 0
    skip_count = 0
    error_count = 0

    BROWSER_RESTART_INTERVAL = 50
    print("🌐 Playwright 초기화 중...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        )
        await block_images(context)
        page = await context.new_page()

        # 첫 업체에 대해 네트워크 graphql 감지 디버깅
        if business_ids:
            first_db_id, first_business_id = business_ids[0]
            print(f"🔎 첫 업체({first_business_id}) 네트워크 graphql 감지 테스트...")
            await debug_graphql_network(page, first_business_id)
            print("✅ 네트워크 감지 테스트 종료. 이후 코드 진행하려면 debug_graphql_network 호출을 주석 처리하세요.")

        for i, (db_id, business_id) in enumerate(tqdm(business_ids, desc="📦 범위 내 업체 처리")):
            try:
                print(f"\n🚀 크롤링 시작: {business_id} (DB ID={db_id})")
                photo_items = await intercept_and_save_graphql(page, business_id, output_path)
                url = f"https://m.place.naver.com/restaurant/{business_id}/photo"
                await page.goto(url, wait_until="networkidle")

                # 스크롤 다운 반복 (사진 더보기 로딩)
                no_new_graphql_count = 0
                last_photo_count = len(photo_items)
                for scroll_num in range(MAX_SCROLL):
                    await page.mouse.wheel(0, 1000)
                    await asyncio.sleep(0.3)
                    print(f"⏬ 스크롤 다운 {scroll_num+1}/{MAX_SCROLL}")
                    if len(photo_items) == last_photo_count:
                        no_new_graphql_count += 1
                    else:
                        no_new_graphql_count = 0
                    last_photo_count = len(photo_items)
                    if scroll_num >= 2 and no_new_graphql_count >= 2:
                        print('⏹️ 더 이상 새로운 GraphQL 응답이 없습니다. 스크롤 종료.')
                        break

                # 사진 저장
                if photo_items:
                    filename = f"{db_id}_photo_{business_id}.jsonl"
                    save_jsonl(filename, photo_items, output_path)
                    print(f"✅ 저장 완료: {filename} ({len(photo_items)}장)")
                    success_count += 1
                else:
                    print(f"⚠️ 수집된 사진 없음")
                    skip_count += 1

                # N개마다 브라우저 재시작
                if (i + 1) % BROWSER_RESTART_INTERVAL == 0:
                    await page.close()
                    await context.close()
                    await browser.close()
                    print(f'🔄 브라우저 재시작: {i+1}번째 업체까지 완료')
                    browser = await p.chromium.launch(headless=False)
                    context = await browser.new_context(
                        viewport={"width": 1280, "height": 800},
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
                    )
                    await block_images(context)
                    page = await context.new_page()

            except Exception as e:
                print(f"❌ 예외 발생: {e}")
                log_failure(business_id, error=str(e))
                error_count += 1
                time.sleep(random.uniform(3, 5))
                continue

        await browser.close()

    print("\n📊 크롤링 요약")
    print(f"🔢 전체 업체 수: {total}")
    print(f"✅ 성공: {success_count}")
    print(f"⚠️ 스킵: {skip_count}")
    print(f"❌ 실패: {error_count}")

if __name__ == "__main__":
    asyncio.run(main())

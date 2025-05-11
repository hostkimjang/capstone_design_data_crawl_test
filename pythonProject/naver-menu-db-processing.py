import os
import json
import sqlite3

# 경로 설정
json_dir = "menu_crawl"
db_path = "food_merged_final.db"

# DB 연결
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# menu 컬럼이 없으면 생성
cursor.execute("PRAGMA table_info(restaurant_merged)")
columns = [col[1] for col in cursor.fetchall()]
if "MENU" not in columns:
    cursor.execute("ALTER TABLE restaurant_merged ADD COLUMN MENU TEXT")

updated = 0

# 디렉토리 내 모든 JSON 처리
for filename in os.listdir(json_dir):
    print(f"🔍 처리 중: {filename}")
    if not filename.endswith(".json"):
        continue

    file_path = os.path.join(json_dir, filename)

    with open(file_path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except Exception as e:
            print(f"❌ JSON 로드 실패: {filename} → {e}")
            continue

    for item in data:
        row_id = item.get("id")
        menu = item.get("menu", [])

        if row_id is None or not isinstance(menu, list):
            continue

        menu_json = json.dumps(menu, ensure_ascii=False)

        cursor.execute("""
            UPDATE restaurant_merged
            SET menu = ?
            WHERE id = ?
        """, (menu_json, row_id))

        if cursor.rowcount:
            updated += 1

conn.commit()
conn.close()

print(f"✅ 총 {updated}개의 레코드가 업데이트되었습니다.")
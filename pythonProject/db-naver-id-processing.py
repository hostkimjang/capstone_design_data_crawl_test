import sqlite3
import re

# DB 연결
conn = sqlite3.connect('food_merged_final.db')  # 파일명 바꿔줘야 함
cursor = conn.cursor()

# 해당 조건의 행들 가져오기
cursor.execute("""
    SELECT rowid, `네이버_tab_list` 
    FROM restaurant_merged
    WHERE `네이버_PLACE_ID_URL` = '/'
""")
rows = cursor.fetchall()

# 정규식 추출 및 업데이트
for rowid, tab_list_raw in rows:
    match = re.search(r'/(\d+)', tab_list_raw)
    if match:
        place_id = match.group(1)
        new_url = f'{place_id}'
        cursor.execute("""
            UPDATE restaurant_merged
            SET `네이버_PLACE_ID_URL` = ?
            WHERE rowid = ?
        """, (new_url, rowid))

conn.commit()
conn.close()

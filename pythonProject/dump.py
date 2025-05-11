import sqlite3
from psycopg2 import connect
from psycopg2.extras import execute_values
from tqdm import tqdm  # ✅ pip install tqdm

# SQLite 연결
sqlite_conn = sqlite3.connect('food_merged_final.db')
sqlite_cursor = sqlite_conn.cursor()

# PostgreSQL 연결
pg_conn = connect(
    dbname='foodpick',
    user='foodpick',
    password='foodpick123',
    host='localhost',
    port='5000'
)
pg_conn.autocommit = True  # ✅ VACUUM용 필수 설정
pg_cursor = pg_conn.cursor()

# PostgreSQL 테이블 생성 (+ PostGIS geom 컬럼 포함)
pg_cursor.execute("""
    DROP TABLE IF EXISTS restaurant_merged;
    CREATE TABLE restaurant_merged (
        id SERIAL PRIMARY KEY,
        사업장명 TEXT,
        인허가일자 TEXT,
        영업상태명 TEXT,
        상세영업상태명 TEXT,
        소재지전체주소 TEXT,
        도로명전체주소 TEXT,
        도로명우편번호 TEXT,
        최종수정시점 TEXT,
        데이터갱신일자 TEXT,
        업태구분명 TEXT,
        네이버_상호명 TEXT,
        네이버_주소 TEXT,
        네이버_전화번호 TEXT,
        네이버_URL TEXT,
        네이버_PLACE_ID_URL TEXT,
        네이버_place_info TEXT,
        네이버_tab_list TEXT,
        menu TEXT,
        LATITUDE TEXT,
        LONGITUDE TEXT,
        geom geometry(Point, 4326)
    );
""")
pg_conn.commit()

print("✅ PostgreSQL 테이블 생성 완료.")

# SQLite → PostgreSQL 데이터 읽기
sqlite_cursor.execute("SELECT * FROM restaurant_merged")
rows = sqlite_cursor.fetchall()
print(f"✅ SQLite에서 {len(rows)}건 데이터 조회 완료.")
print("🚀 PostgreSQL에 데이터 이관 중...")

# ✅ BATCH INSERT
BATCH_SIZE = 1000
for i in tqdm(range(0, len(rows), BATCH_SIZE)):
    batch = rows[i:i + BATCH_SIZE]
    values = []

    for row in batch:
        data = row[1:]  # ID 제외
        lat = data[-2]
        lon = data[-1]
        try:
            if lat and lon:
                lat_f = float(lat)
                lon_f = float(lon)
                geom_wkt = f'SRID=4326;POINT({lon_f} {lat_f})'
            else:
                geom_wkt = None
        except:
            geom_wkt = None
        values.append((*data, geom_wkt))

    execute_values(pg_cursor, """
        INSERT INTO restaurant_merged (
            사업장명, 인허가일자, 영업상태명, 상세영업상태명,
            소재지전체주소, 도로명전체주소, 도로명우편번호, 최종수정시점, 데이터갱신일자,
            업태구분명, 네이버_상호명, 네이버_주소, 네이버_전화번호,
            네이버_URL, 네이버_PLACE_ID_URL, 네이버_place_info,
            네이버_tab_list, menu, LATITUDE, LONGITUDE,
            geom
        )
        VALUES %s
    """, values)

    pg_conn.commit()

print(f"🎉 전체 {len(rows)}건 데이터 이관 및 공간좌표 추가 완료.")

pg_cursor.execute("CREATE INDEX IF NOT EXISTS idx_geom ON restaurant_merged USING GIST (geom);")

# 🔧 최적화 실행
pg_cursor.execute("VACUUM ANALYZE restaurant_merged;")
print("✅ VACUUM ANALYZE 완료: 통계 최적화 반영됨")


# 연결 종료
sqlite_conn.close()
pg_conn.close()

import sys
import os
import json
import logging
import pymysql
import redis
from flask import Flask, jsonify, request
from datetime import datetime
from contextlib import contextmanager

# ----------------------------
# ✅ Logging 설정 (print() 대신 사용)
# ----------------------------
log_filename = f"uwsgi_{datetime.now().strftime('%Y-%m-%d')}.log"
log_path = os.path.join(os.getcwd(), log_filename)

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ----------------------------
# ✅ Flask 앱 초기화
# ----------------------------
app = Flask(__name__)

# ----------------------------
# ✅ 환경 변수 로드
# ----------------------------
try:
    with open('config.json', 'r') as config_file:
        db_config = json.load(config_file)
except (FileNotFoundError, json.JSONDecodeError):
    logger.error("config.json 파일을 찾을 수 없거나 JSON이 잘못되었습니다.")
    exit(1)

# ----------------------------
# ✅ Redis 연결
# ----------------------------
try:
    redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
except redis.ConnectionError:
    logger.error("Redis 연결 실패.")
    exit(1)

# ----------------------------
# ✅ MySQL 연결을 컨텍스트 매니저로 개선 (이걸 load_table_data()보다 먼저 정의해야 함!)
# ----------------------------
@contextmanager
def get_mysql_connection():
    """MySQL 연결을 생성하고 자동으로 닫아주는 컨텍스트 매니저"""
    try:
        conn = pymysql.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
            autocommit=True
        )
        yield conn
    except pymysql.MySQLError as e:
        logger.error(f"MySQL 연결 실패: {e}")
        yield None
    finally:
        if 'conn' in locals() and conn:
            conn.close()

# ----------------------------
# ✅ 테이블 데이터 로드 함수
# ----------------------------
table_data = {}

def load_table_data():
    global table_data  # 전역 변수에 저장
    table_names = ['krx_codes', 'usx_codes', 'coin_codes']
    new_table_data = {}

    with get_mysql_connection() as conn:
        if not conn:
            return {}

        try:
            cursor = conn.cursor()
            for table in table_names:
                cursor.execute(f"SELECT code, name, market, sector FROM {table}")
                new_table_data[table] = cursor.fetchall()
            logger.info("✅ 테이블 데이터 로드 완료.")
        except pymysql.MySQLError as e:
            logger.error(f"테이블 데이터 로드 실패: {e}")
        finally:
            cursor.close()

    table_data = new_table_data  # 전역 변수 업데이트

# ----------------------------
# ✅ 티커를 포함하는 테이블 찾기
# ----------------------------
def find_table_with_ticker(ticker):
    """주어진 티커가 포함된 테이블을 찾음"""
    for table_name, rows in table_data.items():
        if any(row.get('code') == ticker for row in rows):
            logger.info(f"✅ {ticker}가 포함된 테이블: {table_name}")
            return table_name
    return None

# ----------------------------
# ✅ 테이블 데이터 업데이트 API
# ----------------------------
@app.route('/update-tables', methods=['POST'])
def update_table_data():
    try:
        load_table_data()
        logger.info("새로운 테이블 데이터가 로드되었습니다.")
        return jsonify({"message": "Table data successfully updated"}), 200
    except Exception as e:
        logger.error(f"테이블 업데이트 실패: {e}")
        return jsonify({"error": str(e)}), 500

# ----------------------------
# ✅ 가격 정보 조회 API
# ----------------------------
@app.route('/prices', methods=['GET'])
def get_data():
    ticker = request.args.get('ticker')
    start_date = request.args.get('t')
    end_date = request.args.get('end_date')

    if not ticker:
        return jsonify({"error": "Missing required parameter: ticker"}), 400

    table_name = find_table_with_ticker(ticker)
    if not table_name:
        return jsonify({"error": f"Ticker {ticker} not found in any table"}), 404

    cache_key = f"prices:{ticker}"
    cached_data = redis_client.get(cache_key)

    if cached_data:
        cached_data = json.loads(cached_data)
        return jsonify(cached_data)

    # ----------------------------
    # ✅ MySQL에서 데이터 조회
    # ----------------------------
    query = f"""
        SELECT * 
        FROM {table_name}
        WHERE code = %s 
        ORDER BY date ASC
    """

    with get_mysql_connection() as conn:
        if not conn:
            return jsonify({"error": "Failed to connect to MySQL"}), 500

        cursor = conn.cursor()
        cursor.execute(query, (ticker,))
        records = cursor.fetchall()
        cursor.close()

    if not records:
        return jsonify({"error": f"No data found for ticker {ticker}"}), 404

    # ----------------------------
    # ✅ Redis에 데이터 캐싱
    # ----------------------------
    redis_client.setex(cache_key, 300, json.dumps(records))
    
    return jsonify({"code": ticker, "data": records})

# ----------------------------
# ✅ Flask 실행 시 테이블 데이터 로드 (순서 중요!)
# ----------------------------
with app.app_context():
    logger.info("🚀 uWSGI 환경 - 서버 시작 시 테이블 데이터 불러오기")
    load_table_data()

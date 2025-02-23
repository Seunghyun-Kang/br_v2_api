import sys
import os
import json
import logging
import pymysql
import redis
import threading
import time
from flask import Flask, jsonify, request
from datetime import datetime, date, timedelta
from contextlib import contextmanager
import decimal
from flask_cors import CORS
import exchange_calendars as ecals
import pandas as pd
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
CORS(app)
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
    redis_client = redis.StrictRedis(
        host='localhost', port=6379, decode_responses=True)
except redis.ConnectionError:
    logger.error("Redis 연결 실패.")
    exit(1)

# ----------------------------
# ✅ MySQL 연결을 컨텍스트 매니저로 개선 (이걸 load_table_data()보다 먼저 정의해야 함!)
# ----------------------------


@contextmanager
def get_mysql_connection():
    """MySQL 연결을 생성하고 자동으로 닫아주는 컨텍스트 매니저"""
    conn = None  # 연결 초기화
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
        yield conn  # 정상적으로 연결되었을 때만 반환
    except pymysql.MySQLError as e:
        logger.error(f"❌ MySQL 연결 실패: {e}")
        return  # `yield None` 대신 `return` 사용하여 예외 발생 시 함수 종료
    finally:
        if conn:  # `conn`이 생성되었을 때만 닫기
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
                cursor.execute(
                    f"SELECT code, name, market, sector FROM {table}")
                new_table_data[table] = cursor.fetchall()
            logger.info("✅ 테이블 데이터 로드 완료.")
        except pymysql.MySQLError as e:
            logger.error(f"❌ 테이블 데이터 로드 실패: {e}")
        finally:
            cursor.close()

    table_data = new_table_data  # 전역 변수 업데이트

# ✅ 티커를 포함하는 테이블 찾기
# ----------------------------


def find_prices_table_with_ticker(ticker):
    for table_name, rows in table_data.items():
        if any(row.get('code') == ticker for row in rows):
            logger.info(f"✅ {ticker}가 포함된 테이블: {table_name}")
            return table_name.replace("_codes", "_prices")
    return None


def find_signals_table_with_ticker(ticker):
    for table_name, rows in table_data.items():
        if any(row.get('code') == ticker for row in rows):
            logger.info(f"✅ {ticker}가 포함된 테이블: {table_name}")
            if 'krx' in table_name:
                return 'krx_signals'
            elif 'usx' in table_name:
                return 'usx_signals'
            else:
                return 'coin_signals'
    return None

# ----------------------------
# ✅ 3시간마다 테이블 데이터 자동 업데이트
# ----------------------------


def periodic_table_update():
    while True:
        logger.info("⏳ 3시간마다 테이블 데이터 업데이트 실행 중...")
        load_table_data()
        time.sleep(3 * 60 * 60)  # 3시간 = 10800초

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
# ✅ 데이터 내 Decimal과 datetime 값을 JSON 직렬화 가능하도록 변환
# ----------------------------


def convert_to_serializable(data):
    if isinstance(data, list):  # 리스트 처리
        return [convert_to_serializable(item) for item in data]
    elif isinstance(data, dict):  # 딕셔너리 처리
        return {key: convert_to_serializable(value) for key, value in data.items()}
    elif isinstance(data, decimal.Decimal):  # Decimal → float 변환
        return float(data)
    elif isinstance(data, datetime):  # ✅ datetime.datetime 대신 datetime 직접 사용
        return data.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(data, date):  # ✅ datetime.date도 변환
        return data.strftime('%Y-%m-%d')
    return data  # 다른 타입은 그대로 반환


# ----------------------------
# ✅ 테이블 정보 조회 API (/tables)
# ----------------------------
@app.route('/tables', methods=['GET'])
def get_tables():
    """현재 로드된 테이블 데이터를 반환"""
    if not table_data:
        return jsonify({"error": "Table data not loaded"}), 500

    return jsonify(table_data), 200

# ----------------------------
# ✅ 가격 정보 조회 API
# ----------------------------
@app.route('/prices', methods=['GET'])
def get_data():
    ticker = request.args.get('ticker')

    if not ticker:
        return jsonify({"error": "Missing required parameter: ticker"}), 400

    table_name = find_prices_table_with_ticker(ticker)
    if not table_name:
        return jsonify({"error": f"Ticker {ticker} not found in any table"}), 404

    cache_key = f"prices:{ticker}"
    cached_data = redis_client.get(cache_key)

    try:
        if cached_data:
            cached_data = json.loads(cached_data)
            logger.info("🚀 redis에서 불러오기 성공")
            return jsonify(cached_data)
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")

    query = f"""
        SELECT * 
        FROM {table_name}
        WHERE code = %s 
        ORDER BY date ASC
    """

    try:
        with get_mysql_connection() as conn:
            if not conn:  # MySQL 연결 실패 처리
                return jsonify({"error": "Failed to connect to MySQL"}), 500

            cursor = conn.cursor()
            cursor.execute(query, (ticker,))
            records = cursor.fetchall()
            cursor.close()

        if not records:
            return jsonify({"error": f"No data found for ticker {ticker}"}), 404

        # ✅ Decimal 값을 float으로 변환
        records = convert_to_serializable(records)
        # Redis에 데이터 캐싱
        redis_client.setex(cache_key, 300, json.dumps(records))
        logger.info("🚀 DB에서 불러오기 성공")
        return jsonify({"code": ticker, "data": records})

    except pymysql.MySQLError as e:
        logger.error(f"❌ MySQL 쿼리 실행 중 오류 발생: {e}")
        return jsonify({"error": f"MySQL Error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")
        return jsonify({"error": str(e)}), 500

# ----------------------------
# ✅ 가격 정보 조회 API
# ----------------------------
@app.route('/latest_prices_ticker', methods=['GET'])
def get_latest_price_data1():
    ticker = request.args.get('ticker')

    if not ticker:
        return jsonify({"error": "Missing required parameter: ticker"}), 400

    table_name = find_prices_table_with_ticker(ticker)
    if not table_name:
        return jsonify({"error": f"Ticker {ticker} not found in any table"}), 404

    cache_key = f"latest_prices:{ticker}"
    cached_data = redis_client.get(cache_key)

    try:
        if cached_data:
            cached_data = json.loads(cached_data)
            logger.info("🚀 redis에서 불러오기 성공")
            return jsonify(cached_data)
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")

    query = f"""
        SELECT code, date, close
        FROM {table_name}
        WHERE code = %s 
        ORDER BY date ASC
        LIMIT 1;
    """

    try:
        with get_mysql_connection() as conn:
            if not conn:  # MySQL 연결 실패 처리
                return jsonify({"error": "Failed to connect to MySQL"}), 500

            cursor = conn.cursor()
            cursor.execute(query, (ticker,))
            records = cursor.fetchall()
            cursor.close()

        if not records:
            return jsonify({"error": f"No data found for ticker {ticker}"}), 404

        # ✅ Decimal 값을 float으로 변환
        records = convert_to_serializable(records)
        # Redis에 데이터 캐싱
        redis_client.setex(cache_key, 300, json.dumps(records))
        logger.info("🚀 DB에서 불러오기 성공")
        return jsonify({"code": ticker, "data": records})

    except pymysql.MySQLError as e:
        logger.error(f"❌ MySQL 쿼리 실행 중 오류 발생: {e}")
        return jsonify({"error": f"MySQL Error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")
        return jsonify({"error": str(e)}), 500

# ----------------------------
# ✅ 가격 정보 조회 API
# ----------------------------
@app.route('/latest_prices', methods=['GET'])
def get_latest_price_data2():
    market_type = request.args.get('market_type')
    date = request.args.get('date')

    if 'krx' in market_type:
        table_name = 'krx_prices'
    elif 'usx' in market_type:
        table_name = 'usx_prices'
    else:
        table_name = 'coin_prices'

    cache_key = f"latest_prices:{market_type}"
    cached_data = redis_client.get(cache_key)

    try:
        if cached_data:
            cached_data = json.loads(cached_data)
            logger.info("🚀 redis에서 불러오기 성공")
            return jsonify(cached_data)
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")

    query = f"""
        SELECT code, date, close
        FROM {table_name}
        WHERE date = %s
    """

    try:
        with get_mysql_connection() as conn:
            if not conn:  # MySQL 연결 실패 처리
                return jsonify({"error": "Failed to connect to MySQL"}), 500

            cursor = conn.cursor()
            cursor.execute(query, (date,))
            records = cursor.fetchall()
            cursor.close()

        if not records:
            return jsonify({"error": f"No data found"}), 404

        # ✅ Decimal 값을 float으로 변환
        records = convert_to_serializable(records)
        # Redis에 데이터 캐싱
        redis_client.setex(cache_key, 300, json.dumps(records))
        logger.info("🚀 DB에서 불러오기 성공")
        return jsonify(records)

    except pymysql.MySQLError as e:
        logger.error(f"❌ MySQL 쿼리 실행 중 오류 발생: {e}")
        return jsonify({"error": f"MySQL Error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")
        return jsonify({"error": str(e)}), 500


def get_next_market_date(market_type):

    if 'krx' in market_type:
        calendar_type = 'XKRX'
    elif 'usx' in market_type:
        calendar_type = 'XNYS'
    else:
        calendar_type = 'COINS'
        
    market_calendar = ecals.get_calendar(calendar_type)
    return market_calendar.next_open(datetime.now()).to_pydatetime().date()
    
@app.route('/latest_signals', methods=['GET'])
def get_latest_data():
    market_type = request.args.get('type')
    signal_type = request.args.get('signal_type')

    if not market_type:
        return jsonify({"error": "Missing required parameter: market_type"}), 400

    # 테이블명 설정
    if 'krx' in market_type:
        table_name = 'krx_signals'
    elif 'usx' in market_type:
        table_name = 'usx_signals'
    else:
        table_name = 'coin_signals'
    database = 'be_rich'

    # Redis 캐시 확인
    cache_key = f"signals:{market_type}:{signal_type}"
    cached_data = redis_client.get(cache_key)
    if cached_data:
        try:
            logging.info("🚀 Redis 캐시에서 최신 데이터 불러오기 성공")
            return jsonify(json.loads(cached_data))
        except Exception as e:
            logging.error(f"❌ Redis 캐시 변환 오류: {e}")

    today = datetime.now().strftime('%Y-%m-%d')
    next_market_date = get_next_market_date(market_type).strftime("%Y-%m-%d")

    # MySQL 연결
    conn = get_mysql_connection()
    if not conn:
        return jsonify({"error": "Failed to connect to MySQL"}), 500

    try:
        if signal_type == 'ideal':
            with get_mysql_connection() as conn:
                if not conn:
                    return jsonify({"error": "Failed to connect to MySQL"}), 500

                query = f"""
                    SELECT *
                    FROM {table_name}
                    WHERE target_date = '{next_market_date}' AND ideal_signal = 1
                """
                cursor = conn.cursor()
                cursor.execute(query)
                buy_records = cursor.fetchall()

                query = f"""
                    SELECT *
                    FROM {table_name}
                    WHERE target_date = '{next_market_date}' AND ideal_signal = -1
                """
                cursor.execute(query)
                sell_records = cursor.fetchall()

                cursor.close()

                final = {
                    "today": today,
                    "next": next_market_date,
                    "buy": convert_to_serializable(buy_records),
                    "sell": convert_to_serializable(sell_records)
                }
                redis_client.setex(cache_key, 300, json.dumps(final))

                return jsonify(final)
        else:
            with get_mysql_connection() as conn:
                if not conn:
                    return jsonify({"error": "Failed to connect to MySQL"}), 500
                # 'signal'이 포함된 컬럼명 조회
                column_query = f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{database}' 
                    AND TABLE_NAME = '{table_name}'
                    AND COLUMN_NAME LIKE '%signal%'
                """
                cursor = conn.cursor()
                cursor.execute(column_query)
                columns = cursor.fetchall()
                signal_columns = [row['COLUMN_NAME'] for row in columns]
                print(f"❌칼럼명 결과: {signal_columns}")
                if not signal_columns:
                    return jsonify({"error": "No 'signal' columns found"}), 404

                # 조건문 생성
                signal_buy = " + ".join([f"(CASE WHEN {col} = 1 THEN 1 ELSE 0 END)" for col in signal_columns])
                signal_sell = " + ".join([f"(CASE WHEN {col} = -1 THEN 1 ELSE 0 END)" for col in signal_columns])

                query = f"""
                    SELECT *,
                        ({signal_buy}) AS buy_sum,
                        ({signal_sell}) AS sell_sum
                    FROM {table_name}
                    WHERE target_date = '{next_market_date}'
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                cursor.close()

                # Python에서 필터링
                buy_records = [row for row in rows if row['buy_sum'] >= 4]
                sell_records = [row for row in rows if row['sell_sum'] >= 4]

                final = {
                    "today": today,
                    "next": next_market_date,
                    "buy": convert_to_serializable(buy_records),
                    "sell": convert_to_serializable(sell_records)
                }
                redis_client.setex(cache_key, 300, json.dumps(final))

                return jsonify(final)


    except pymysql.MySQLError as e:
        logging.error(f"❌ MySQL 쿼리 실행 중 오류 발생: {e}")
        return jsonify({"error": f"MySQL Error: {str(e)}"}), 500

    except Exception as e:
        logging.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/signals', methods=['GET'])
def get_signals_by_ticker():
    ticker = request.args.get('ticker')
    if not ticker:
        return jsonify({"error": "Missing required parameter: ticker"}), 400

    # 티커에 따른 테이블 선택 (예: 티커 접두사에 따라)
    table_name = find_signals_table_with_ticker(ticker)
    if not table_name:
        return jsonify({"error": f"Ticker {ticker} not found in any table"}), 404

    cache_key = f"signals:ticker:{ticker}"
    cached_data = redis_client.get(cache_key)
    try:
        if cached_data:
            cached_data = json.loads(cached_data)
            logger.info("🚀 Redis 캐시에서 특정 티커 signal 데이터 불러오기 성공")
            return jsonify(cached_data)
    except Exception as e:
        logger.error(f"❌ 캐시 조회 중 오류 발생: {e}")

    query = f"""
        SELECT * 
        FROM {table_name}
        WHERE code = %s
        ORDER BY date ASC;
    """

    try:
        with get_mysql_connection() as conn:
            if not conn:
                return jsonify({"error": "Failed to connect to MySQL"}), 500

            cursor = conn.cursor()
            cursor.execute(query, (ticker,))
            records = cursor.fetchall()
            cursor.close()

        if not records:
            return jsonify({"error": f"No data found for ticker {ticker}"}), 404

        records = convert_to_serializable(records)
        redis_client.setex(cache_key, 300, json.dumps(records))
        logger.info("🚀 DB에서 특정 티커 signal 데이터 불러오기 성공")
        return jsonify({"code": ticker, "data": records})

    except pymysql.MySQLError as e:
        logger.error(f"❌ MySQL 쿼리 실행 중 오류 발생: {e}")
        return jsonify({"error": f"MySQL Error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")
        return jsonify({"error": str(e)}), 500

# ----------------------------
# ✅ 매매 이력 API
# ----------------------------
@app.route('/trade_history', methods=['GET'])
def get_trade_history():
    market_type = request.args.get('type')
    signal_type = request.args.get('signal_type')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if not market_type:
        return jsonify({"error": "Missing required parameter: market_type"}), 400

    if not start_date:
        start_date = datetime.now().strftime('%Y-%m-%d')

    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')

    if 'krx' in market_type:
        table_name = f'krx_trades_{signal_type}'
    elif 'usx' in market_type:
        table_name = f'usx_trades_{signal_type}'
    else:
        table_name = f'coin_trades_{signal_type}'

    cache_key = f"trade_history:{market_type}:{signal_type}:{start_date}:{end_date}"
    cached_data = redis_client.get(cache_key)

    try:
        if cached_data:
            cached_data = json.loads(cached_data)
            logger.info("🚀 redis에서 불러오기 성공")
            return jsonify(cached_data)
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")

    query = f"""
        SELECT * 
        FROM {table_name}
        WHERE date <= {end_date} AND date >= {start_date}
    """

    try:
        with get_mysql_connection() as conn:
            if not conn:  # MySQL 연결 실패 처리
                return jsonify({"error": "Failed to connect to MySQL"}), 500

            cursor = conn.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            cursor.close()

        if not records:
            return jsonify({"error": f"No data found for trade {market_type}"}), 404

        # ✅ Decimal 값을 float으로 변환
        records = convert_to_serializable(records)
        # Redis에 데이터 캐싱
        redis_client.setex(cache_key, 300, json.dumps(records))
        logger.info("🚀 DB에서 불러오기 성공")
        return jsonify(records)

    except pymysql.MySQLError as e:
        logger.error(f"❌ MySQL 쿼리 실행 중 오류 발생: {e}")
        return jsonify({"error": f"MySQL Error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")
        return jsonify({"error": str(e)}), 500

# ----------------------------
# ✅ 매매 이력 API
# ----------------------------
@app.route('/profits', methods=['GET'])
def get_profits():
    market_type = request.args.get('type')
    signal_type = request.args.get('signal_type')
    start_date = request.args.get('start_date')
    uid = request.args.get('uid')

    if not market_type:
        return jsonify({"error": "Missing required parameter: market_type"}), 400

    if 'krx' in market_type:
        table_name = f'krx_trades_{signal_type}'
    elif 'usx' in market_type:
        table_name = f'usx_trades_{signal_type}'
    else:
        table_name = f'coin_trades_{signal_type}'

    cache_key = f"profits:{market_type}:{signal_type}:{start_date}"
    cached_data = redis_client.get(cache_key)

    try:
        if cached_data:
            cached_data = json.loads(cached_data)
            logger.info("🚀 redis에서 불러오기 성공")
            return jsonify(cached_data)
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")

    query = f"""
        SELECT date, profit
        FROM {table_name}
        WHERE date >= %s AND profit IS NOT NULL
    """

    try:
        with get_mysql_connection() as conn:
            if not conn:  # MySQL 연결 실패 처리
                return jsonify({"error": "Failed to connect to MySQL"}), 500

            cursor = conn.cursor()
            cursor.execute(query, (start_date,))
            records = cursor.fetchall()
            cursor.close()

        if not records:
            return jsonify({"error": f"No data found for trade {market_type}"}), 404

        # ✅ Decimal 값을 float으로 변환
        records = convert_to_serializable(records)
        # Redis에 데이터 캐싱
        redis_client.setex(cache_key, 300, json.dumps(records))
        logger.info("🚀 DB에서 불러오기 성공")
        return jsonify(records)

    except pymysql.MySQLError as e:
        logger.error(f"❌ MySQL 쿼리 실행 중 오류 발생: {e}")
        return jsonify({"error": f"MySQL Error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")
        return jsonify({"error": str(e)}), 500


# ----------------------------
# ✅ 보유 종목 API
# ----------------------------
@app.route('/owned', methods=['GET'])
def get_owned():
    market_type = request.args.get('type')
    signal_type = request.args.get('signal_type')

    if not market_type:
        return jsonify({"error": "Missing required parameter: market_type"}), 400

    if 'krx' in market_type:
        table_name = f'krx_owned_{signal_type}'
    elif 'usx' in market_type:
        table_name = f'usx_owned_{signal_type}'
    else:
        table_name = f'coin_owned_{signal_type}'

    cache_key = f"owned:{market_type}:{signal_type}"
    cached_data = redis_client.get(cache_key)

    try:
        if cached_data:
            cached_data = json.loads(cached_data)
            logger.info("🚀 redis에서 불러오기 성공")
            return jsonify(cached_data)
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")

    query = f"""
        SELECT * 
        FROM {table_name}
    """

    try:
        with get_mysql_connection() as conn:
            if not conn:  # MySQL 연결 실패 처리
                return jsonify({"error": "Failed to connect to MySQL"}), 500

            cursor = conn.cursor()
            cursor.execute(query)
            records = cursor.fetchall()
            cursor.close()

        if not records:
            return jsonify({"error": f"No data found for owned {market_type}"}), 404

        # ✅ Decimal 값을 float으로 변환
        records = convert_to_serializable(records)
        # Redis에 데이터 캐싱
        redis_client.setex(cache_key, 300, json.dumps(records))
        logger.info("🚀 DB에서 불러오기 성공")
        return jsonify(records)

    except pymysql.MySQLError as e:
        logger.error(f"❌ MySQL 쿼리 실행 중 오류 발생: {e}")
        return jsonify({"error": f"MySQL Error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")
        return jsonify({"error": str(e)}), 500

# ----------------------------
# ✅ 최신 업데이트 날짜 API
# ----------------------------
@app.route('/latest_update_date', methods=['GET'])
def get_latest_update_date():
    market_type = request.args.get('market_type')

    if not market_type:
        return jsonify({"error": "Missing required parameter: market_type"}), 400

    if 'krx' in market_type:
        table_name = f'krx_date'
        code = "005930"
    elif 'usx' in market_type:
        table_name = f'usx_date'
        code = "AAPL"
    else:
        table_name = f'coin_date'
        code = "BTC"

    cache_key = f"latest_update_date:{type}"
    cached_data = redis_client.get(cache_key)

    try:
        if cached_data:
            cached_data = json.loads(cached_data)
            logger.info("🚀 redis에서 불러오기 성공")
            return jsonify(cached_data)
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")

    query = f"""
        SELECT latest_price_date
        FROM {table_name}
        WHERE code = %s
    """

    try:
        with get_mysql_connection() as conn:
            if not conn:  # MySQL 연결 실패 처리
                return jsonify({"error": "Failed to connect to MySQL"}), 500

            cursor = conn.cursor()
            cursor.execute(query, (code,))
            records = cursor.fetchall()
            cursor.close()

        if not records:
            return jsonify({"error": f"No data found"}), 404

        # ✅ Decimal 값을 float으로 변환
        records = convert_to_serializable(records)
        # Redis에 데이터 캐싱
        redis_client.setex(cache_key, 300, json.dumps(records))
        logger.info("🚀 DB에서 불러오기 성공")
        return jsonify(records)

    except pymysql.MySQLError as e:
        logger.error(f"❌ MySQL 쿼리 실행 중 오류 발생: {e}")
        return jsonify({"error": f"MySQL Error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"❌ 데이터 조회 중 예상치 못한 오류 발생: {e}")
        return jsonify({"error": str(e)}), 500

# ✅ 백그라운드에서 자동 실행
update_thread = threading.Thread(target=periodic_table_update, daemon=True)
update_thread.start()

# ----------------------------
# ✅ Flask 실행 시 테이블 데이터 로드 (순서 중요!)
# ----------------------------
with app.app_context():
    logger.info("🚀 uWSGI 환경 - 서버 시작 시 테이블 데이터 불러오기")
    load_table_data()

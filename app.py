from flask import Flask, jsonify, request
import mysql.connector
import redis
import json

app = Flask(__name__)

try:
    with open('config.json', 'r') as config_file:
        db_config = json.load(config_file)
except FileNotFoundError:
    print("********************Error: config.json file not found.")
    exit(1)
except json.JSONDecodeError:
    print("********************Error: config.json is not a valid JSON file.")
    exit(1)

try:
    redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
except FileNotFoundError:
    print("********************Error: Redis connection failed.")
    exit(1)

table_data = {}

def get_mysql_connection():
    print("********************get_mysql_connection")
    try: 
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        return connection
    except FileNotFoundError:
        print("********************mysql connection failed.")
        exit(1)

def load_table_data():
    global table_data
    table_names = ['krx_codes', 'usx_codes', 'coin_codes']
    table_data = {}

    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(dictionary=True)

        for table in table_names:
            cursor.execute(f"SELECT code, name, market, sector FROM {table}")
            rows = cursor.fetchall()
            table_data[table] = rows

        print("********************Table data successfully loaded.")
    except Exception as e:
        print(f"Error loading table data: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def find_table_with_ticker(ticker):
    global table_data
    for table_name, rows in table_data.items():
        if any(row.get('code') == ticker for row in rows):
            print(f"********************table_name: {table_name}")
            return table_name
    return None

@app.route('/update-tables', methods=['POST'])
def update_table_data():
    try:
        load_table_data()
        print(table_data)
        return jsonify({"message": "Table data successfully updated"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/prices', methods=['GET'])
def get_data():
    ticker = request.args.get('ticker')
    start_date = request.args.get('t')
    end_date = request.args.get('end_date')

    if not ticker:
        return jsonify({"error": "Missing required parameter: ticker"}), 400

    try:
        conn = get_mysql_connection()
        cursor = conn.cursor(dictionary=True)
        table_name = find_table_with_ticker(ticker)

        if not table_name:
            return jsonify({"error": f"Ticker {ticker} not found in any table"}), 404

        # Redis 캐시에서 데이터 확인
        cache_key = f"prices:{ticker}"
        cached_data = redis_client.get(cache_key)

        # 캐시 데이터가 있는 경우 로드
        if cached_data:
            cached_data = json.loads(cached_data)
            cached_start = cached_data["start_date"]
            cached_end = cached_data["end_date"]
            cached_records = cached_data["data"]
        else:
            cached_start, cached_end, cached_records = None, None, []

        # 필요한 범위 확인
        missing_start = None
        missing_end = None

        if start_date:
            if not cached_start or start_date < cached_start:
                missing_start = start_date
        else:
            cursor.execute(f"SELECT MIN(date) AS oldest_date FROM {table_name} WHERE code = %s", (ticker,))
            result = cursor.fetchone()
            if result and result['oldest_date']:
                missing_start = result['oldest_date']

        if end_date:
            if not cached_end or end_date > cached_end:
                missing_end = end_date
        else:
            cursor.execute(f"SELECT MAX(date) AS latest_date FROM {table_name} WHERE code = %s", (ticker,))
            result = cursor.fetchone()
            if result and result['latest_date']:
                missing_end = result['latest_date']

        # MySQL에서 필요한 부분만 추가 조회
        if missing_start or missing_end:
            query = f"""
                SELECT * 
                FROM {table_name}
                WHERE code = %s AND date BETWEEN %s AND %s
                ORDER BY date ASC
            """
            cursor.execute(query, (ticker, missing_start or cached_start, missing_end or cached_end))
            new_records = cursor.fetchall()
        else:
            new_records = []

        # 모든 데이터를 병합
        full_data = {record["date"]: record for record in cached_records}
        for record in new_records:
            full_data[record["date"]] = record
        merged_records = list(full_data.values())
        merged_records.sort(key=lambda x: x["date"])

        # Redis에 병합 데이터 저장
        merged_start = min(r["date"] for r in merged_records)
        merged_end = max(r["date"] for r in merged_records)
        redis_client.setex(
            cache_key,
            300,
            json.dumps({"start_date": merged_start, "end_date": merged_end, "data": merged_records}),
        )

        # 필요한 범위만 반환
        final_data = [
            record for record in merged_records if start_date <= record["date"] <= end_date
        ]

        return jsonify({
            "code": ticker,
            "data": final_data,
            "start_date": start_date,
            "end_date": end_date
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    load_table_data()
    app.run(debug=True)

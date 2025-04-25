import asyncio
import logging
import queue
import paho.mqtt.client as mqtt
import psycopg
from psycopg_pool import AsyncConnectionPool
from psycopg import sql
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import ASYNCHRONOUS,SYNCHRONOUS
import os
from dotenv import load_dotenv

# 載入 .env 文件
load_dotenv()

# 設置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MQTT 配置（大寫參數）
# MQTT 配置（從環境變數讀取）
BROKER = os.getenv("BROKER")
PORT = int(os.getenv("PORT"))
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
SUB_TOPIC = os.getenv("SUB_TOPIC")

# PostgreSQL 配置（從環境變數讀取）
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# InfluxDB 配置（從環境變數讀取）
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# 初始化非同步 PostgreSQL 連線池
PG_POOL = AsyncConnectionPool(
    conninfo=f"host={PG_HOST} port={PG_PORT} dbname={PG_DATABASE} user={PG_USER} password={PG_PASSWORD}",
    min_size=1,
    max_size=10,
    open=False
)

# 配置非同步寫入選項
write_options = WriteOptions(
    batch_size=5,  # 每 500 筆資料寫入一次
    flush_interval=1000,  # 每 1000 毫秒（1 秒）強制寫入一次
    jitter_interval=200,  # 隨機延遲，減少同時寫入衝突
    retry_interval=5000,  # 重試間隔 5 秒
    max_retries=3,  # 最大重試次數
    max_retry_delay=30000,  # 最大重試延遲 30 秒
    exponential_base=2  # 指數退避基數
)

# 初始化 InfluxDB 客戶端（使用非同步模式）
INFLUXDB_CLIENT = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
INFLUXDB_WRITE_API = INFLUXDB_CLIENT.write_api(
    write_options=write_options,
)


# 創建佇列用於儲存訊息
MESSAGE_QUEUE = queue.Queue()

# 用於儲存 customer_id: uuid 的映射
CUSTOMER_UUID_MAP = {}
SAMPLE_DATA = {}

def find_customer_id_by_uuid(uuid):
    for customer_id, uuid_list in CUSTOMER_UUID_MAP.items():
        if uuid in uuid_list:
            return customer_id
    return None

async def check_and_create_schema():
    try:
        async with PG_POOL.connection() as conn:
            for customer_id in CUSTOMER_UUID_MAP.keys():
                schema_name = str(customer_id)
                # 檢查 Schema 是否存在
                schema_exists_query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.schemata 
                    WHERE schema_name = %s
                );
                """
                result = await conn.execute(schema_exists_query, (schema_name,))
                schema_exists = (await result.fetchone())[0]

                if not schema_exists:
                    # 創建 Schema，使用 sql.Identifier 安全處理名稱
                    create_schema_query = sql.SQL("CREATE SCHEMA {}").format(
                        sql.Identifier(schema_name)
                    )
                    await conn.execute(create_schema_query)
                    logging.info(f"✅ 已創建 Schema {schema_name}")

    except Exception as e:
        logging.error(f"檢查或創建 Schemas 失敗: {str(e)}")
        raise

# 檢查表是否存在，並創建或更新
async def check_and_create_table(data, schema_name, table_name):
    try:
        async with PG_POOL.connection() as conn:
            # 檢查表是否存在
            table_exists_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            );
            """
            result = await conn.execute(table_exists_query, (schema_name, table_name))
            table_exists = (await result.fetchone())[0]

            if table_exists:
                # 獲取現有欄位
                columns_query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s;
                """
                result = await conn.execute(columns_query, (schema_name, table_name))
                existing_columns = {row[0] for row in await result.fetchall()}

                # 獲取 data 中的欄位
                data_columns = set(data.keys())

                # 排除 id 欄位進行比較
                existing_columns.discard("id")  # 移除 id，因為它是表結構中的主鍵

                # 找出需要新增的欄位
                missing_columns = data_columns - existing_columns
                if missing_columns:
                    logging.info(f"表 {schema_name}.{table_name} 缺少欄位: {missing_columns}，正在添加...")
                    # 動態添加缺失的欄位
                    for column in missing_columns:
                        value = data[column]
                        col_type = infer_column_type(value, column)
                        alter_table_query = sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                            sql.Identifier(column),
                            sql.SQL(col_type)
                        )
                        await conn.execute(alter_table_query)
                        logging.info(f"已添加欄位 {column} 到表 {schema_name}.{table_name}，類型: {col_type}")
                else:
                    logging.debug(f"表 {schema_name}.{table_name} 的欄位已匹配，無需更新")

            if not table_exists:
                # 動態生成創建表的 SQL，使用 sql.Identifier 安全處理名稱
                columns_definitions = []
                for key, value in data.items():
                    col_type = infer_column_type(value, key)
                    columns_definitions.append(
                        sql.SQL("{} {}").format(sql.Identifier(key), sql.SQL(col_type))
                    )
                columns_sql = sql.SQL(", ").join(columns_definitions)
                create_table_query = sql.SQL("""
                CREATE TABLE {}.{} (
                    id SERIAL PRIMARY KEY,
                    {}
                )
                """).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    columns_sql
                )
                await conn.execute(create_table_query)
                logging.info(f"✅ 已創建表 {schema_name}.{table_name}，欄位: {[str(definition.as_string(conn)) for definition in columns_definitions]}")

    except Exception as e:
        logging.error(f"檢查或創建表失敗（schema_name: {schema_name}, table_name: {table_name}）: {str(e)}")
        raise

# 初始化函數：從 public Schema 中讀取 customer_id 和 uuid 的映射（支援一對多），並初始化 SAMPLE_DATA
async def initialize_customer_uuid_map():
    global CUSTOMER_UUID_MAP, SAMPLE_DATA
    try:
        async with PG_POOL.connection() as conn:
            # 執行 JOIN 查詢，從 public.auth 和 public.customer_uuid 表中獲取 customer_id 和 uuid
            query = """
            SELECT a.customer_id, cu.uuid
            FROM public.auth a
            JOIN public.customer_uuid cu ON a.customer_id = cu.customer_id;
            """
            result = await conn.execute(query)
            rows = await result.fetchall()

            # 將查詢結果整理為 customer_id: [uuid] 的鍵值對（支援一對多）
            CUSTOMER_UUID_MAP = {}
            for row in rows:
                customer_id, uuid = row[0], row[1]
                if customer_id in CUSTOMER_UUID_MAP:
                    CUSTOMER_UUID_MAP[customer_id].append(uuid)
                else:
                    CUSTOMER_UUID_MAP[customer_id] = [uuid]

            logging.info(f"✅ 成功初始化 customer_uuid 映射，總計 {len(CUSTOMER_UUID_MAP)} 個 customer_id")

        # 初始化 SAMPLE_DATA，從每個 schema_name.power_metrics 表中獲取最新一筆資料
        async with PG_POOL.connection() as conn:
            for customer_id in CUSTOMER_UUID_MAP.keys():
                schema_name = str(customer_id)
                table_name = "power_metrics"
                try:
                    # 檢查表是否存在
                    table_exists_query = """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s AND table_name = %s
                    );
                    """
                    result = await conn.execute(table_exists_query, (schema_name, table_name))
                    table_exists = (await result.fetchone())[0]

                    if table_exists:
                        # 獲取最新一筆資料（按 id 降序排序）
                        select_query = sql.SQL("""
                        SELECT *
                        FROM {}.{}
                        ORDER BY id DESC
                        LIMIT 1
                        """).format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name)
                        )
                        result = await conn.execute(select_query)
                        row = await result.fetchone()

                        if row:
                            # 獲取欄位名稱
                            columns = [desc[0] for desc in result.description]
                            # 將資料轉為字典形式，排除 id 欄位
                            row_dict = dict(zip(columns, row))
                            row_dict.pop("id", None)  # 移除 id 欄位
                            SAMPLE_DATA[customer_id] = row_dict
                            logging.debug(f"從 {schema_name}.{table_name} 獲取最新資料: {row_dict}")
                        else:
                            logging.warning(f"表 {schema_name}.{table_name} 存在但沒有資料")
                            
                    else:
                        logging.warning(f"表 {schema_name}.{table_name} 不存在")
                        
                except Exception as e:
                    logging.error(f"從 {schema_name}.{table_name} 獲取資料失敗: {str(e)}")          
        logging.info(f"✅ 成功初始化 SAMPLE_DATA，總計 {len(SAMPLE_DATA)} 個 customer_id")

    except Exception as e:
        logging.error(f"❌ 初始化 customer_uuid 映射或 SAMPLE_DATA 失敗: {str(e)}")

# 遍歷 CUSTOMER_UUID_MAP 的鍵，檢查並創建 Tables，使用 SAMPLE_DATA 推斷表結構
async def check_and_create_tables():
    global CUSTOMER_UUID_MAP,SAMPLE_DATA
    try:
        for customer_id in CUSTOMER_UUID_MAP.keys():
            schema_name = str(customer_id)
            table_name = "power_metrics"
            # 從 SAMPLE_DATA 中獲取該 customer_id 的資料
            sample_data = SAMPLE_DATA.get(customer_id, {})
            if sample_data:
                await check_and_create_table(sample_data, schema_name, table_name)

    except Exception as e:
        logging.error(f"創建 Tables 失敗: {str(e)}")
        raise

async def refresh_customer_uuid_map():
    while True:
        try:
            await initialize_customer_uuid_map()
            await check_and_create_schema()
            await check_and_create_tables()
        except Exception as e:
            logging.error(f"定時刷新 customer_uuid 映射失敗: {str(e)}")
        await asyncio.sleep(600)  # 等待 10 分鐘 (600 秒)

# 解析函數（保持不變）
def timestamp_to_yymmddhhmm(data: list) -> str:
    yy = int(data[0], 16)
    mm = int(data[1], 16)
    dd = int(data[2], 16)
    hh = int(data[3], 16)
    min = int(data[4], 16)
    year = 2000 + yy
    if mm < 1 or mm > 12 or dd < 1 or dd > 31 or hh > 23 or min > 59:
        return "無效日期時間"
    return f"{year}-{mm:02d}-{dd:02d} {hh:02d}:{min:02d}"

def hex_to_decimal_little_endian(hex_bytes: list) -> int:
    hex_bytes_reversed = hex_bytes[::-1]
    hex_str = ''.join(hex_bytes_reversed)
    if hex_str :
        return int(hex_str, 16)
    else:
        return 0

def decode_payload(payload: bytes):
    data_hex = payload.hex(' ').upper()
    data_bytes = data_hex.split()

    uuid = ''.join(data_bytes[0:16])
    # if uuid != '25325545C2FD4C2F94984F1D5F2D39F9':
    #     return None

    customer_id = find_customer_id_by_uuid(uuid)
    if not customer_id:
        return None
    
    timestamp_str = timestamp_to_yymmddhhmm(data_bytes[24:29])
    building_str = None
    result = {
        "customer_id":customer_id,
        "uuid": uuid,
        "id_esp32": ''.join(data_bytes[16:20]),
        "model": int(''.join(data_bytes[20]), 16),
        "kraken_reserved": int(''.join(data_bytes[21:24]), 16),
        "timestamp": timestamp_str,
        "device_type": int(data_bytes[29], 16),
        "addr": hex_to_decimal_little_endian(data_bytes[30:32]),
        "buildingtime": building_str,
        "status": int(data_bytes[36], 16),
        "datatype": int(data_bytes[37], 16),
        "av": round(hex_to_decimal_little_endian(data_bytes[38:40]) * 0.01,5),
        "bv": round(hex_to_decimal_little_endian(data_bytes[40:42]) * 0.01,5),
        "cv": round(hex_to_decimal_little_endian(data_bytes[42:44]) * 0.01,5),
        "abv": round(hex_to_decimal_little_endian(data_bytes[44:46]) * 0.01,5),
        "bcv": round(hex_to_decimal_little_endian(data_bytes[46:48]) * 0.01,5),
        "acv": round(hex_to_decimal_little_endian(data_bytes[48:50]) * 0.01,5),
        "aa": round(hex_to_decimal_little_endian(data_bytes[50:52]) * 0.01,5),
        "ba": round(hex_to_decimal_little_endian(data_bytes[52:54]) * 0.01,5),
        "ca": round(hex_to_decimal_little_endian(data_bytes[54:56]) * 0.01,5),
        "hz": round(hex_to_decimal_little_endian(data_bytes[56:58]) * 0.001,5),
        "a_kwh": round(hex_to_decimal_little_endian(data_bytes[58:62]) * 0.01,5),
        "b_kwh": round(hex_to_decimal_little_endian(data_bytes[62:66]) * 0.01,5),
        "c_kwh": round(hex_to_decimal_little_endian(data_bytes[66:70]) * 0.01,5),
        "t_kwh": round(hex_to_decimal_little_endian(data_bytes[70:74]) * 0.01,5),
        "amax_kw": round(hex_to_decimal_little_endian(data_bytes[74:76]) * 0.01,5),
        "bmax_kw": round(hex_to_decimal_little_endian(data_bytes[76:78]) * 0.01,5),
        "cmax_kw": round(hex_to_decimal_little_endian(data_bytes[78:80]) * 0.01,5),
        "tmax_kw": round(hex_to_decimal_little_endian(data_bytes[80:82]) * 0.01,5),
        "aavg_kw": round(hex_to_decimal_little_endian(data_bytes[82:84]) * 0.01,5),
        "bavg_kw": round(hex_to_decimal_little_endian(data_bytes[84:86]) * 0.01,5),
        "cavg_kw": round(hex_to_decimal_little_endian(data_bytes[86:88]) * 0.01,5),
        "tavg_kw": round(hex_to_decimal_little_endian(data_bytes[88:90]) * 0.01,5),
        "a_kw": round(hex_to_decimal_little_endian(data_bytes[90:92]) * 0.01,5),
        "b_kw": round(hex_to_decimal_little_endian(data_bytes[92:94]) * 0.01,5),
        "c_kw": round(hex_to_decimal_little_endian(data_bytes[94:96]) * 0.01,5),
        "t_kw": round(hex_to_decimal_little_endian(data_bytes[96:98]) * 0.01,5),
        "a_kvar": round(hex_to_decimal_little_endian(data_bytes[98:100]) * 0.01,5),
        "b_kvar": round(hex_to_decimal_little_endian(data_bytes[100:102]) * 0.01,5),
        "c_kvar": round(hex_to_decimal_little_endian(data_bytes[102:104]) * 0.01,5),
        "t_kvar": round(hex_to_decimal_little_endian(data_bytes[104:106]) * 0.01,5),
        "a_kva": round(hex_to_decimal_little_endian(data_bytes[106:108]) * 0.01,5),
        "b_kva": round(hex_to_decimal_little_endian(data_bytes[108:110]) * 0.01,5),
        "c_kva": round(hex_to_decimal_little_endian(data_bytes[110:112]) * 0.01,5),
        "t_kva": round(hex_to_decimal_little_endian(data_bytes[112:114]) * 0.01,5),
        "a_pf": round(hex_to_decimal_little_endian(data_bytes[114:116]) * 0.0001,5),
        "b_pf": round(hex_to_decimal_little_endian(data_bytes[116:118]) * 0.0001,5),
        "c_pf": round(hex_to_decimal_little_endian(data_bytes[118:120]) * 0.0001,5),
        "t_pf": round(hex_to_decimal_little_endian(data_bytes[120:122]) * 0.0001,5),
        "channel_reserved": hex_to_decimal_little_endian(data_bytes[122:])
    }
    
    return result

# MQTT 回調函數
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        logging.info("✅ 成功連線")
        client.subscribe(SUB_TOPIC)
    else:
        logging.error(f"❌ MQTT連線失敗，錯誤碼: {reason_code}")

def on_message(client, userdata, msg):

    MESSAGE_QUEUE.put(msg.payload)
    logging.info(f"✅ 成功儲存資料到Message Queue")
# 非同步從佇列中取出訊息並寫入資料庫
async def process_queue():
    while True:
        try:
            # 非阻塞地從佇列中取訊息
            data = MESSAGE_QUEUE.get_nowait()
            result = decode_payload(data)

            if result :
                await write_to_influxdb(result)
                await write_to_postgresql(result)

        except queue.Empty:
            await asyncio.sleep(0.1)
        except Exception as e:
            logging.error(f"處理佇列訊息失敗: {e}")

# 根據值的類型推斷 PostgreSQL 欄位類型
def infer_column_type(value, key: str) -> str:
    if isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, str):
        # 檢查是否為日期時間格式
        if "timestamp" in key.lower() or "time" in key.lower():
            return "TIMESTAMP" if "timestamp" in key.lower() else "VARCHAR(20)"
        return "VARCHAR(255)"  # 默認為 VARCHAR，根據需要調整長度
    else:
        return "TEXT"  # 其他類型使用 TEXT

# 非同步寫入 InfluxDB（修改後：將 customer_id 作為 tag）
async def write_to_influxdb(data):
    try:
        
        # 檢查必要欄位
        uuid = data.get("uuid")
        if not uuid:
            raise ValueError("data 中缺少 uuid 欄位")
        
        customer_id = data.get("customer_id")
        addr = data.get("addr")
        id_esp32 = data.get("id_esp32")
        # 創建 InfluxDB Point
        point = Point("power_metrics") \
            .tag("customer_id", customer_id) \
            .tag("uuid", uuid) \
            .tag("id_esp32", id_esp32) \
            .tag("addr", addr)# 添加 customer_id 作為 tag

        # 將所有欄位（包括 timestamp）作為 fields，排除特定欄位
        exclude_fields = {"uuid", "buildingtime","customer_id","addr","id_esp32"}
        for key, value in data.items():
            if key in exclude_fields or value is None:
                continue
            if isinstance(value, (int, float, bool, str)):
                point.field(key, value)
            else:
                logging.warning(f"跳過無效字段 {key}: 值 {value} 的類型 {type(value)} 不受支持")

        # 不設置時間戳，讓 InfluxDB 自動生成
        INFLUXDB_WRITE_API.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        logging.info(f"✅ 資料已提交到 InfluxDB 寫入隊列")

    except Exception as e:
        logging.error(f"❌ 準備寫入 InfluxDB 失敗（uuid: {uuid or '未知'}, customer_id: {customer_id or '未知'}）：{str(e)}")
        raise

# 非同步資料庫寫入
async def write_to_postgresql(data):
    global SAMPLE_DATA,CUSTOMER_UUID_MAP
    try:
        # 檢查表是否存在並確保欄位匹配
        schema_name = data.pop("customer_id", None)
        table_name = "power_metrics"
        SAMPLE_DATA[schema_name] = data
        async with PG_POOL.connection() as conn:
            # 使用 await conn.set_autocommit(True) 設置 autocommit
            await conn.set_autocommit(True)
            columns = list(data.keys())
            values = [
                None if key in ("timestamp", "buildingtime") and value == "無效日期時間" else value
                for key, value in data.items()
            ]
            # 使用 sql.Identifier 安全處理名稱，確保格式正確
            query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(columns))
            )
            # 記錄生成的 SQL 語句以便調試
            logging.debug(f"生成的 SQL 語句: {query.as_string(conn)}")
            await conn.execute(query, values)

        logging.info("✅ 成功寫入 PostgreSQL")
    except Exception as e:
        logging.error(f"❌ 寫入 PostgreSQL 失敗（schema_name: {schema_name}, table_name: {table_name}）: {str(e)}")

# 主程式
async def main():
    # 開啟 PostgreSQL 連線池（如果需要寫入資料庫）
    await PG_POOL.open()
    try:
        # 啟動 MQTT 客戶端（非阻塞）
        await initialize_customer_uuid_map()
        await check_and_create_schema()
        await check_and_create_tables()
        asyncio.create_task(refresh_customer_uuid_map())
        client.connect(BROKER, PORT, keepalive=60)
        client.loop_start()  # 在背景執行 MQTT 迴圈
        # 執行佇列處理任務
        await process_queue()
    except Exception as e:
        logging.error(f"程式執行失敗: {e}")
    finally:
        client.loop_stop()
        client.disconnect()
        await PG_POOL.close()
        INFLUXDB_WRITE_API.flush()
        INFLUXDB_WRITE_API.close()  # 關閉 InfluxDB 寫入 API
        INFLUXDB_CLIENT.close()  # 關閉 InfluxDB 客戶端
        logging.info("正在關閉...")

if __name__ == "__main__":
    # MQTT 客戶端設置
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    asyncio.run(main())
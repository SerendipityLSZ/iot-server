# -*- coding: gbk -*-

import os
import sys
import json
import pymysql
import mysql.connector
import time
import threading
import signal
import hashlib
import hmac
import base64
import stomp
import ssl
import schedule

from typing import List

from alibabacloud_dysmsapi20170525.client import Client as Dysmsapi20170525Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_dysmsapi20170525 import models as dysmsapi_20170525_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient
temperature = 0 # 温度
humidity = 0 # 湿度
soil_humidity = 0 # 土壤湿度
cogas = 0 # 一氧化碳浓度
smoggas = 0 # 烟雾浓度
fire = 0 # 火焰状态

humidity_downvalue = 00 # 温度上限
soil_humidity_upvalue = 0 # 温度下限
soil_humidity_downv0 # 湿度上限
cogas_value = 0 # 一氧化0 # 湿度下限
smoggas_value = 0 # 烟雾浓度0 # 土壤湿度上限0 # 土壤湿度下限

bird = 0 # 鸟

# 状态'1'表示打开，'0'表示关闭
fan = 0 # 风扇状态
heat = 0 # 加热器状态
humidification = 0 # 加湿器状态
pumpout = 0#  # 水泵状态
pumpin = 0 # 抽水机状态
buzzer = 0 # 蜂鸣器状态

somewhere_temperature = 0.0 # 任意地方温度
somewhere_humidity = 0.0 # 任意地方湿度

received_messages = [] # 接收到的消息

def connect_and_subscribe(conn):
    # 工程代码泄露可能会导致 AccessKey 泄露，并威胁账号下所有资源的安全性。以下代码示例使用环境变量获取 AccessKey 的方式进行调用，仅供参考
    accessKey = os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID']
    accessSecret = os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET']
    consumerGroupId = "DEFAULT_GROUP"
    # iotInstanceId：实例ID。
    iotInstanceId = "iot-06z00e4lgvr7bq8"
    clientId = "lzy.eacsu.cn"
    # 签名方法：支持hmacmd5，hmacsha1和hmacsha256。
    signMethod = "hmacsha1"
    timestamp = current_time_millis()
    # userName组装方法，请参见AMQP客户端接入说明文档。
    # 若使用二进制传输，则userName需要添加encode=base64参数，服务端会将消息体base64编码后再推送。具体添加方法请参见下一章节“二进制消息体说明”。
    username = clientId + "|authMode=aksign" + ",signMethod=" + signMethod \
                    + ",timestamp=" + timestamp + ",authId=" + accessKey \
                    + ",iotInstanceId=" + iotInstanceId \
                    + ",consumerGroupId=" + consumerGroupId + "|"
    signContent = "authId=" + accessKey + "&timestamp=" + timestamp
    # 计算签名，password组装方法，请参见AMQP客户端接入说明文档。
    password = do_sign(accessSecret.encode("utf-8"), signContent.encode("utf-8"))
    
    conn.set_listener('', MyListener(conn))
    conn.connect(username, password, wait=True)
    # 清除历史连接检查任务，新建连接检查任务
    schedule.clear('conn-check')
    schedule.every(1).seconds.do(do_check,conn).tag('conn-check')

class MyListener(stomp.ConnectionListener):
    def __init__(self, conn):
        self.conn = conn

    def on_error(self, frame):
        print('received an error "%s"' % frame.body)

    def on_message(self, frame):

        global received_messages
        print('received a message "%s"' % frame.body)
        received_messages = json.loads(frame.body)
        parse_and_insert_measure_data(frame.body)

    def on_heartbeat_timeout(self):
        print('on_heartbeat_timeout')

    def on_connected(self, headers):
        print("successfully connected")
        conn.subscribe(destination='/topic/#', id=1, ack='auto')
        print("successfully subscribe")

    def on_disconnected(self):
        print('disconnected')
        connect_and_subscribe(self.conn)

def process_messages():
    # 确保在调用此函数之前，已有消息被接收和存储
    global received_messages
    for message in received_messages:
        # 处理每个消息
        print(message)

def read_message(conn):
    conn.subscribe(destination='/topic/#', id=1, ack='auto')
    
def current_time_millis():
    return str(int(round(time.time() * 1000)))

def do_sign(secret, sign_content):
    m = hmac.new(secret, sign_content, digestmod=hashlib.sha1)
    return base64.b64encode(m.digest()).decode("utf-8")

# 检查连接，如果未连接则重新建连
def do_check(conn):
    print('check connection, is_connected: %s', conn.is_connected())
    if (not conn.is_connected()):
        try:
            connect_and_subscribe(conn)
        except Exception as e:
            print('disconnected, ', e)

# 定时任务方法，检查连接状态
def connection_check_timer(stop_event):
    while not stop_event.is_set():
        schedule.run_pending()
        time.sleep(10)

# 执行SQL语句
def execute_sql(sql, val, fetch=False):
    try:
        # 在这里建立连接
        connection = pymysql.connect(
            host='localhost',
            user='root',
            passwd='123456',
            db='alilot',
            charset='utf8'
        )
        with connection.cursor() as cursor:
            cursor.execute(sql, val)
            if fetch:
                result = cursor.fetchone()
                connection.commit()
                return result
            connection.commit()
            print("数据插入成功")
    except pymysql.Error as e:
        print(f"数据库操作失败: {e}")
        return None
    finally:
        connection.close()  # 确保在完成操作后关闭连接

# 获取最新的测量数据
def get_latest_measure_data_number():
    sql = "SELECT MAX(number) FROM measure_data"
    latest_number = execute_sql(sql, None, fetch=True)
    return latest_number[0] if latest_number else 0

# 解析并插入测量数据
def parse_and_insert_measure_data(json_str):
    # 解析JSON字符串
    data = json.loads(json_str)
    
    # 获取最新的number值
    latest_number = get_latest_measure_data_number()
    
    # 提取所需的数据
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    temperature = data["items"]["temperature"]["value"]
    humidity = data["items"]["humidity"]["value"]
    soil_humidity = data["items"]["soil_humidity"]["value"]
    cogas = data["items"]["cogas"]["value"]
    smoggas = data["items"]["smoggas"]["value"]
    fire = data["items"]["fire"]["value"]
    bird = data["items"]["bird"]["value"]
    
    # 构建SQL语句，插入数据到数据库
    sql = ("INSERT INTO measure_data (time, number, temperature, humidity, soil_humidity, cogas, smoggas,"
           "fire, bird) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
    val = (current_time, latest_number + 1, temperature, humidity, soil_humidity, cogas, smoggas, fire, bird)
    execute_sql(sql, val)
    
    print("数据插入成功")

def get_latest_device_data_number():
    sql = "SELECT MAX(number) FROM device_data"
    latest_number = execute_sql(sql, None, fetch=True)
    return latest_number[0] if latest_number else 0

# 解析并插入设备数据
def parse_and_insert_device_data(json_str):
    # 解析JSON字符串
    data = json.loads(json_str)

    # 获取最新的number值
    latest_number = get_latest_device_data_number()

    # 提取所需的数据
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    fan = data["items"]["fan"]["value"]
    heat = data["items"]["heat"]["value"]
    humidification = data["items"]["humidification"]["value"]
    pumpout = data["items"]["pumpout"]["value"]
    pumpin = data["items"]["pumpin"]["value"]
    buzzer = data["items"]["buzzer"]["value"]
    
    # 构建SQL语句，插入数据到数据库
    sql = ("INSERT INTO device_data (time, number, fan, heat, humidification, "
           "pumpout, pumpin, buzzer) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    val = (current_time, latest_number + 1, fan, heat, humidification, pumpout, pumpin, buzzer)
    execute_sql(sql, val)
    
    print("数据插入成功")

# 获取最新的设置数据
def get_latest_setting_data_number():
    sql = "SELECT MAX(number) FROM setting_data"
    latest_number = execute_sql(sql, None, fetch=True)
    return latest_number[0] if latest_number else 0

# 解析并插入设置数据
def parse_and_insert_setting_data(json_str):
    # 解析JSON字符串
    data = json.loads(json_str)

    # 获取最新的number值
    latest_number = get_latest_setting_data_number()

    # 提取所需的数据
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    temperature_upvalue = data["items"]["temperature_upvalue"]["value"]
    temperature_downvalue = data["items"]["temperature_downvalue"]["value"]
    humidity_upvalue = data["items"]["humidity_upvalue"]["value"]
    humidity_downvalue = data["items"]["humidity_downvalue"]["value"]
    soil_humidity_upvalue = data["items"]["soil_humidity_upvalue"]["value"]
    soil_humidity_downvalue = data["items"]["soil_humidity_downvalue"]["value"]
    cogas_value = data["items"]["cogas_value"]["value"]
    smoggas_value = data["items"]["smoggas_value"]["value"]

    # 构建SQL语句，插入数据到数据库
    sql = ("INSERT INTO setting_data (time, number, temperature_upvalue, temperature_downvalue, humidity_upvalue, "
           "humidity_downvalue, soil_humidity_upvalue, soil_humidity_downvalue ,cogas_value ,smoggas_value) "
           " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    val = (current_time, latest_number + 1, temperature_upvalue, temperature_downvalue,
            humidity_upvalue, humidity_downvalue, soil_humidity_upvalue, soil_humidity_downvalue, 
            cogas_value, smoggas_value)
    execute_sql(sql, val)
    
    print("数据插入成功")

class Sample:
    def __init__(self):
        pass

    @staticmethod
    def create_client(
        access_key_id: str,
        access_key_secret: str,
    ) -> Dysmsapi20170525Client:
        """
        使用AK&SK初始化账号Client
        @param access_key_id:
        @param access_key_secret:
        @return: Client
        @throws Exception
        """
        config = open_api_models.Config(
            # 必填，您的 AccessKey ID,
            access_key_id=os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'],
            # 必填，您的 AccessKey Secret,
            access_key_secret=os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET']
        )
        # Endpoint 请参考 https://api.aliyun.com/product/Dysmsapi
        config.endpoint = f'dysmsapi.aliyuncs.com'
        return Dysmsapi20170525Client(config)

    @staticmethod
    def main(
        args: List[str],
    ) -> None:
        # 请确保代码运行环境设置了环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID 和 ALIBABA_CLOUD_ACCESS_KEY_SECRET。
        # 工程代码泄露可能会导致 AccessKey 泄露，并威胁账号下所有资源的安全性。以下代码示例使用环境变量获取 AccessKey 的方式进行调用，仅供参考，建议使用更安全的 STS 方式，更多鉴权访问方式请参见：https://help.aliyun.com/document_detail/378659.html
        client = Sample.create_client(os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'], os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET'])
        send_sms_request = dysmsapi_20170525_models.SendSmsRequest(
            phone_numbers='18890092933',
            sign_name='智能温室大棚管理系统',
            template_code='SMS_465071110',
            template_param='{"attribute1":"1234","value":"1234","attribute2":"1234","status":"1234","device":"1234","time":"1234"}'
        )
        runtime = util_models.RuntimeOptions()
        try:
            # 复制代码运行请自行打印 API 的返回值
            client.send_sms_with_options(send_sms_request, runtime)
        except Exception as error:
            # 此处仅做打印展示，请谨慎对待异常处理，在工程项目中切勿直接忽略异常。
            # 错误 message
            print(error.message)
            # 诊断地址
            print(error.data.get("Recommend"))
            UtilClient.assert_as_string(error.message)

    @staticmethod
    async def main_async(
        args: List[str],
    ) -> None:
        # 请确保代码运行环境设置了环境变量 ALIBABA_CLOUD_ACCESS_KEY_ID 和 ALIBABA_CLOUD_ACCESS_KEY_SECRET。
        # 工程代码泄露可能会导致 AccessKey 泄露，并威胁账号下所有资源的安全性。以下代码示例使用环境变量获取 AccessKey 的方式进行调用，仅供参考，建议使用更安全的 STS 方式，更多鉴权访问方式请参见：https://help.aliyun.com/document_detail/378659.html
        client = Sample.create_client(os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'], os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET'])
        send_sms_request = dysmsapi_20170525_models.SendSmsRequest(
            phone_numbers='18890092933',
            sign_name='智能温室大棚管理系统',
            template_code='SMS_465071110',
            template_param='{"attribute1":"1234","value":"1234","attribute2":"1234","status":"1234","device":"1234","time":"1234"}'
        )
        runtime = util_models.RuntimeOptions()
        try:
            # 复制代码运行请自行打印 API 的返回值
            await client.send_sms_with_options_async(send_sms_request, runtime)
        except Exception as error:
            # 此处仅做打印展示，请谨慎对待异常处理，在工程项目中切勿直接忽略异常。
            # 错误 message
            print(error.message)
            # 诊断地址
            print(error.data.get("Recommend"))
            UtilClient.assert_as_string(error.message)


#  接入域名，请参见AMQP客户端接入说明文档。这里直接填入域名，不需要带amqps://前缀
if __name__ == "__main__":
    
    stop_event = threading.Event()

    conn = stomp.Connection([('iot-06z00e4lgvr7bq8.amqp.iothub.aliyuncs.com', 61614)], heartbeats=(0,300))
    conn.set_ssl(for_hosts=[('iot-06z00e4lgvr7bq8.amqp.iothub.aliyuncs.com', 61614)], ssl_version=ssl.PROTOCOL_TLS)

    try:
        connect_and_subscribe(conn)
    except Exception as e:
        print('connecting failed')
        raise e
    
    parse_and_insert_measure_data(json_str)

    if () : {
        Sample.main(sys.argv[1:])
    }

    thread = threading.Thread(target=connection_check_timer, args=(stop_event,))
    thread.daemon = True  # 将线程标记为守护线程
    thread.start()

    def signal_handler(signum, frame):
        print("Exiting gracefully...")
        # 设置停止事件，使线程可以退出
        stop_event.set()
        # 等待线程结束
        thread.join()
        # 关闭STOMP连接
        if conn.is_connected():
            conn.disconnect()

    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 主线程继续执行其他任务，如果有的话
    # 使用stop_event.wait()在这里阻塞，直到收到退出信号
    stop_event.wait()

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
temperature = 0 # �¶�
humidity = 0 # ʪ��
soil_humidity = 0 # ����ʪ��
cogas = 0 # һ����̼Ũ��
smoggas = 0 # ����Ũ��
fire = 0 # ����״̬

humidity_downvalue = 00 # �¶�����
soil_humidity_upvalue = 0 # �¶�����
soil_humidity_downv0 # ʪ������
cogas_value = 0 # һ����0 # ʪ������
smoggas_value = 0 # ����Ũ��0 # ����ʪ������0 # ����ʪ������

bird = 0 # ��

# ״̬'1'��ʾ�򿪣�'0'��ʾ�ر�
fan = 0 # ����״̬
heat = 0 # ������״̬
humidification = 0 # ��ʪ��״̬
pumpout = 0#  # ˮ��״̬
pumpin = 0 # ��ˮ��״̬
buzzer = 0 # ������״̬

somewhere_temperature = 0.0 # ����ط��¶�
somewhere_humidity = 0.0 # ����ط�ʪ��

received_messages = [] # ���յ�����Ϣ

def connect_and_subscribe(conn):
    # ���̴���й¶���ܻᵼ�� AccessKey й¶������в�˺���������Դ�İ�ȫ�ԡ����´���ʾ��ʹ�û���������ȡ AccessKey �ķ�ʽ���е��ã������ο�
    accessKey = os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID']
    accessSecret = os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET']
    consumerGroupId = "DEFAULT_GROUP"
    # iotInstanceId��ʵ��ID��
    iotInstanceId = "iot-06z00e4lgvr7bq8"
    clientId = "lzy.eacsu.cn"
    # ǩ��������֧��hmacmd5��hmacsha1��hmacsha256��
    signMethod = "hmacsha1"
    timestamp = current_time_millis()
    # userName��װ��������μ�AMQP�ͻ��˽���˵���ĵ���
    # ��ʹ�ö����ƴ��䣬��userName��Ҫ���encode=base64����������˻Ὣ��Ϣ��base64����������͡�������ӷ�����μ���һ�½ڡ���������Ϣ��˵������
    username = clientId + "|authMode=aksign" + ",signMethod=" + signMethod \
                    + ",timestamp=" + timestamp + ",authId=" + accessKey \
                    + ",iotInstanceId=" + iotInstanceId \
                    + ",consumerGroupId=" + consumerGroupId + "|"
    signContent = "authId=" + accessKey + "&timestamp=" + timestamp
    # ����ǩ����password��װ��������μ�AMQP�ͻ��˽���˵���ĵ���
    password = do_sign(accessSecret.encode("utf-8"), signContent.encode("utf-8"))
    
    conn.set_listener('', MyListener(conn))
    conn.connect(username, password, wait=True)
    # �����ʷ���Ӽ�������½����Ӽ������
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
    # ȷ���ڵ��ô˺���֮ǰ��������Ϣ�����պʹ洢
    global received_messages
    for message in received_messages:
        # ����ÿ����Ϣ
        print(message)

def read_message(conn):
    conn.subscribe(destination='/topic/#', id=1, ack='auto')
    
def current_time_millis():
    return str(int(round(time.time() * 1000)))

def do_sign(secret, sign_content):
    m = hmac.new(secret, sign_content, digestmod=hashlib.sha1)
    return base64.b64encode(m.digest()).decode("utf-8")

# ������ӣ����δ���������½���
def do_check(conn):
    print('check connection, is_connected: %s', conn.is_connected())
    if (not conn.is_connected()):
        try:
            connect_and_subscribe(conn)
        except Exception as e:
            print('disconnected, ', e)

# ��ʱ���񷽷����������״̬
def connection_check_timer(stop_event):
    while not stop_event.is_set():
        schedule.run_pending()
        time.sleep(10)

# ִ��SQL���
def execute_sql(sql, val, fetch=False):
    try:
        # �����ｨ������
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
            print("���ݲ���ɹ�")
    except pymysql.Error as e:
        print(f"���ݿ����ʧ��: {e}")
        return None
    finally:
        connection.close()  # ȷ������ɲ�����ر�����

# ��ȡ���µĲ�������
def get_latest_measure_data_number():
    sql = "SELECT MAX(number) FROM measure_data"
    latest_number = execute_sql(sql, None, fetch=True)
    return latest_number[0] if latest_number else 0

# �����������������
def parse_and_insert_measure_data(json_str):
    # ����JSON�ַ���
    data = json.loads(json_str)
    
    # ��ȡ���µ�numberֵ
    latest_number = get_latest_measure_data_number()
    
    # ��ȡ���������
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    temperature = data["items"]["temperature"]["value"]
    humidity = data["items"]["humidity"]["value"]
    soil_humidity = data["items"]["soil_humidity"]["value"]
    cogas = data["items"]["cogas"]["value"]
    smoggas = data["items"]["smoggas"]["value"]
    fire = data["items"]["fire"]["value"]
    bird = data["items"]["bird"]["value"]
    
    # ����SQL��䣬�������ݵ����ݿ�
    sql = ("INSERT INTO measure_data (time, number, temperature, humidity, soil_humidity, cogas, smoggas,"
           "fire, bird) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
    val = (current_time, latest_number + 1, temperature, humidity, soil_humidity, cogas, smoggas, fire, bird)
    execute_sql(sql, val)
    
    print("���ݲ���ɹ�")

def get_latest_device_data_number():
    sql = "SELECT MAX(number) FROM device_data"
    latest_number = execute_sql(sql, None, fetch=True)
    return latest_number[0] if latest_number else 0

# �����������豸����
def parse_and_insert_device_data(json_str):
    # ����JSON�ַ���
    data = json.loads(json_str)

    # ��ȡ���µ�numberֵ
    latest_number = get_latest_device_data_number()

    # ��ȡ���������
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    fan = data["items"]["fan"]["value"]
    heat = data["items"]["heat"]["value"]
    humidification = data["items"]["humidification"]["value"]
    pumpout = data["items"]["pumpout"]["value"]
    pumpin = data["items"]["pumpin"]["value"]
    buzzer = data["items"]["buzzer"]["value"]
    
    # ����SQL��䣬�������ݵ����ݿ�
    sql = ("INSERT INTO device_data (time, number, fan, heat, humidification, "
           "pumpout, pumpin, buzzer) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    val = (current_time, latest_number + 1, fan, heat, humidification, pumpout, pumpin, buzzer)
    execute_sql(sql, val)
    
    print("���ݲ���ɹ�")

# ��ȡ���µ���������
def get_latest_setting_data_number():
    sql = "SELECT MAX(number) FROM setting_data"
    latest_number = execute_sql(sql, None, fetch=True)
    return latest_number[0] if latest_number else 0

# ������������������
def parse_and_insert_setting_data(json_str):
    # ����JSON�ַ���
    data = json.loads(json_str)

    # ��ȡ���µ�numberֵ
    latest_number = get_latest_setting_data_number()

    # ��ȡ���������
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    temperature_upvalue = data["items"]["temperature_upvalue"]["value"]
    temperature_downvalue = data["items"]["temperature_downvalue"]["value"]
    humidity_upvalue = data["items"]["humidity_upvalue"]["value"]
    humidity_downvalue = data["items"]["humidity_downvalue"]["value"]
    soil_humidity_upvalue = data["items"]["soil_humidity_upvalue"]["value"]
    soil_humidity_downvalue = data["items"]["soil_humidity_downvalue"]["value"]
    cogas_value = data["items"]["cogas_value"]["value"]
    smoggas_value = data["items"]["smoggas_value"]["value"]

    # ����SQL��䣬�������ݵ����ݿ�
    sql = ("INSERT INTO setting_data (time, number, temperature_upvalue, temperature_downvalue, humidity_upvalue, "
           "humidity_downvalue, soil_humidity_upvalue, soil_humidity_downvalue ,cogas_value ,smoggas_value) "
           " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    val = (current_time, latest_number + 1, temperature_upvalue, temperature_downvalue,
            humidity_upvalue, humidity_downvalue, soil_humidity_upvalue, soil_humidity_downvalue, 
            cogas_value, smoggas_value)
    execute_sql(sql, val)
    
    print("���ݲ���ɹ�")

class Sample:
    def __init__(self):
        pass

    @staticmethod
    def create_client(
        access_key_id: str,
        access_key_secret: str,
    ) -> Dysmsapi20170525Client:
        """
        ʹ��AK&SK��ʼ���˺�Client
        @param access_key_id:
        @param access_key_secret:
        @return: Client
        @throws Exception
        """
        config = open_api_models.Config(
            # ������� AccessKey ID,
            access_key_id=os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'],
            # ������� AccessKey Secret,
            access_key_secret=os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET']
        )
        # Endpoint ��ο� https://api.aliyun.com/product/Dysmsapi
        config.endpoint = f'dysmsapi.aliyuncs.com'
        return Dysmsapi20170525Client(config)

    @staticmethod
    def main(
        args: List[str],
    ) -> None:
        # ��ȷ���������л��������˻������� ALIBABA_CLOUD_ACCESS_KEY_ID �� ALIBABA_CLOUD_ACCESS_KEY_SECRET��
        # ���̴���й¶���ܻᵼ�� AccessKey й¶������в�˺���������Դ�İ�ȫ�ԡ����´���ʾ��ʹ�û���������ȡ AccessKey �ķ�ʽ���е��ã������ο�������ʹ�ø���ȫ�� STS ��ʽ�������Ȩ���ʷ�ʽ��μ���https://help.aliyun.com/document_detail/378659.html
        client = Sample.create_client(os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'], os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET'])
        send_sms_request = dysmsapi_20170525_models.SendSmsRequest(
            phone_numbers='18890092933',
            sign_name='�������Ҵ������ϵͳ',
            template_code='SMS_465071110',
            template_param='{"attribute1":"1234","value":"1234","attribute2":"1234","status":"1234","device":"1234","time":"1234"}'
        )
        runtime = util_models.RuntimeOptions()
        try:
            # ���ƴ������������д�ӡ API �ķ���ֵ
            client.send_sms_with_options(send_sms_request, runtime)
        except Exception as error:
            # �˴�������ӡչʾ��������Դ��쳣�����ڹ�����Ŀ������ֱ�Ӻ����쳣��
            # ���� message
            print(error.message)
            # ��ϵ�ַ
            print(error.data.get("Recommend"))
            UtilClient.assert_as_string(error.message)

    @staticmethod
    async def main_async(
        args: List[str],
    ) -> None:
        # ��ȷ���������л��������˻������� ALIBABA_CLOUD_ACCESS_KEY_ID �� ALIBABA_CLOUD_ACCESS_KEY_SECRET��
        # ���̴���й¶���ܻᵼ�� AccessKey й¶������в�˺���������Դ�İ�ȫ�ԡ����´���ʾ��ʹ�û���������ȡ AccessKey �ķ�ʽ���е��ã������ο�������ʹ�ø���ȫ�� STS ��ʽ�������Ȩ���ʷ�ʽ��μ���https://help.aliyun.com/document_detail/378659.html
        client = Sample.create_client(os.environ['ALIBABA_CLOUD_ACCESS_KEY_ID'], os.environ['ALIBABA_CLOUD_ACCESS_KEY_SECRET'])
        send_sms_request = dysmsapi_20170525_models.SendSmsRequest(
            phone_numbers='18890092933',
            sign_name='�������Ҵ������ϵͳ',
            template_code='SMS_465071110',
            template_param='{"attribute1":"1234","value":"1234","attribute2":"1234","status":"1234","device":"1234","time":"1234"}'
        )
        runtime = util_models.RuntimeOptions()
        try:
            # ���ƴ������������д�ӡ API �ķ���ֵ
            await client.send_sms_with_options_async(send_sms_request, runtime)
        except Exception as error:
            # �˴�������ӡչʾ��������Դ��쳣�����ڹ�����Ŀ������ֱ�Ӻ����쳣��
            # ���� message
            print(error.message)
            # ��ϵ�ַ
            print(error.data.get("Recommend"))
            UtilClient.assert_as_string(error.message)


#  ������������μ�AMQP�ͻ��˽���˵���ĵ�������ֱ����������������Ҫ��amqps://ǰ׺
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
    thread.daemon = True  # ���̱߳��Ϊ�ػ��߳�
    thread.start()

    def signal_handler(signum, frame):
        print("Exiting gracefully...")
        # ����ֹͣ�¼���ʹ�߳̿����˳�
        stop_event.set()
        # �ȴ��߳̽���
        thread.join()
        # �ر�STOMP����
        if conn.is_connected():
            conn.disconnect()

    # ע���źŴ�����
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # ���̼߳���ִ��������������еĻ�
    # ʹ��stop_event.wait()������������ֱ���յ��˳��ź�
    stop_event.wait()

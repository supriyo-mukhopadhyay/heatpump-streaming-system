from paho.mqtt import client as mqtt_client
import random
import json
import logging
import sys
import time
import argparse
import boto3
import datetime
import hashlib
from dotenv import load_dotenv
import os

# AWS Credentials -->
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRETE_ACCESS_KEY = os.getenv("SECRETE_ACCESS_KEY")
REGION = os.getenv("REGION")
#################################### AWS ################################################

# Create AWS session with credentials. using boto3 lib
session = boto3.Session(
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRETE_ACCESS_KEY,
    region_name=REGION,
)

# Create kinesis client.
# Call the boto3 client with the `kinesis` resource.  Store the object in `client`.
kinesis = session.client("kinesis", region_name=REGION)

# Specify mqtt details.
broker = "ec2-51-20-252-192.eu-north-1.compute.amazonaws.com"
port = 1883
topic = "NTC/data/transmit-EP03"
client_id = f"python-mqtt-{random.randint(0, 1000)}"
# username = 'emqx'
# password = 'public'

##################################### variables ##########################################

kinesis_stream_name = ""
UNSIGNED_CHAR = 0
SIGNED_CHAR = 16
UNSIGNED_SHORT = 1
SIGNED_SHORT = 17
# _payloadHexArray_ = []
sec_counter = 0

################################# Logging ###############################################
# All application logs are saved in producer.log file in project directory
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("producer.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
#################################### parsing cli #########################################
# Parse command line input
parser = argparse.ArgumentParser(description="Parse JSON object from command line")
parser.add_argument("--stream", type=str, help="Kinesis data stream name")
args = parser.parse_args()
kinesis_stream_name = args.stream


def create_kinesis_data_stream(stream_name: str, shard_count: int = 2) -> None:
    try:
        # Check if the stream already exists
        if stream_name in kinesis.list_streams()["StreamNames"]:
            logging.warning(f"Kinesis data stream {stream_name} already exists")

            kinesis.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
            # Get the status of data stream
            status = kinesis.describe_stream(StreamName=stream_name, Limit=200)

            i = 0
            # wait untill delete operation is complete
            try:
                while status["StreamDescription"]["StreamStatus"] == "DELETING":
                    time.sleep(10)
                    logging.info(
                        'Stream "{}" is being deleted, {} seconds elapsed...'.format(
                            stream_name, 10 * (i + 1)
                        )
                    )

                    status = kinesis.describe_stream(StreamName=stream_name, Limit=200)
                    i = i + 1
            except Exception as e:
                status = "DELETED"
            logging.info('Stream "{}" has been succesfully deleted'.format(stream_name))
    except Exception as e:
        logging.error(
            {
                "message": "Failed to delete data stream in consumer pipeline",
                "error": str(e),
                "line": format(sys.exc_info()[-1].tb_lineno),
            }
        )
    # Use the `create_stream()` method from the client and pass the data stream name and the shard count.
    try:
        response_kinesis_create = kinesis.create_stream(
            StreamName=stream_name, ShardCount=shard_count
        )
        if response_kinesis_create["ResponseMetadata"]["HTTPStatusCode"] == 200:
            logging.info(
                f"Kinesis data stream created: { response_kinesis_create['ResponseMetadata']['RequestId']}"
            )
        # Get status of data stream
        status = kinesis.describe_stream(StreamName=stream_name, Limit=200)
        i = 0
        while status["StreamDescription"]["StreamStatus"] == "CREATING":
            time.sleep(10)
            logging.info(
                'Stream "{}" is being created, {} seconds elapsed...'.format(
                    stream_name, 10 * (i + 1)
                )
            )
            status = kinesis.describe_stream(StreamName=stream_name, Limit=200)
        if status == "ACTIVE":
            logging.info('Stream "{}" has been succesfully created'.format(stream_name))
            # Reducing the data retension time to 2 hrs, to reduce cost
            if status["StreamDescription"]["RetentionPeriodHours"] > 2:
                kinesis.decrease_stream_retention_period(
                    StreamName=stream_name,
                    RetentionPeriodHours=2,
                    # StreamARN='string'
                )
        elif status == "CREATING_FAILED":
            logging.warning('Stream "{}" creation has failed'.format(stream_name))
    except Exception as e:
        logging.error(
            {
                "message": "Error creating kinesis stream",
                "error": str(e),
                "line": format(sys.exc_info()[-1].tb_lineno),
            }
        )


def kinesis_stream(data_record) -> None:
    try:
        # execute single PutRecord request
        response = kinesis.put_record(
            StreamName=kinesis_stream_name,
            Data=json.dumps(data_record).encode("utf-8"),
            PartitionKey=data_record["Device_Id"],
        )
        logging.info(
            f"Produced record {response['SequenceNumber']} to Shard {response['ShardId']}"
        )
        return
    except Exception as e:
        logging.error(
            {
                "message": "Error producing record",
                "error": str(e),
                "record": data_record,
                "line": format(sys.exc_info()[-1].tb_lineno),
            }
        )
        return


@staticmethod
def hex_to_signed_char(s: str) -> int:
    __signedIntArray__ = []
    __signedNumber__ = 0
    for j in range(0, len(s), 2):
        __signedIntArray__.append(s[j : j + 2])
    __signedNumber__ = int(__signedIntArray__[0], 16)
    if __signedNumber__ > 32768:
        __signedNumber__ = __signedNumber__ - 65536
    return __signedNumber__


@staticmethod
def hex_to_signed_number(s: str) -> int:
    __signedIntArray__ = []
    __signedNumber__ = 0
    for j in range(0, len(s), 2):
        __signedIntArray__.append(s[j : j + 2])
    __lsb__ = int(__signedIntArray__[1], 16)
    __msb__ = int(__signedIntArray__[0], 16)
    __signedNumber__ = __lsb__ + (__msb__ << 8)
    if __signedNumber__ > 32768:
        __signedNumber__ = __signedNumber__ - 65536
    return __signedNumber__


@staticmethod
def hex_to_unsigned_char(s: str) -> int:
    __unsignedIntArray__ = []
    __unsignedNumber__ = 0
    for j in range(0, len(s), 2):
        __unsignedIntArray__.append(s[j : j + 2])
    __unsignedNumber__ = int(__unsignedIntArray__[0], 16)
    return __unsignedNumber__


@staticmethod
def hex_to_unsigned_number(s: str) -> int:
    __unsignedIntArray__ = []
    __unsignedNumber__ = 0
    for j in range(0, len(s), 2):
        __unsignedIntArray__.append(s[j : j + 2])
    __lsb__ = int(__unsignedIntArray__[1], 16)
    __msb__ = int(__unsignedIntArray__[0], 16)
    __unsignedNumber__ = __lsb__ + (__msb__ << 8)
    return __unsignedNumber__


def extract_deviceid(ascii_array, dataList):
    for i in range(0, len(ascii_array)):
        if ascii_array[i] != 0:
            dataList.append(ascii_array[i])
    return dataList


def msgExtraction(_payloadHexArray_):
    _usignedCharFlag__ = 0
    _usignedShortFlag__ = 0
    _signedCharFlag__ = 0
    _signedShortFlag__ = 0
    dataList = []
    ascii_array = []
    __signedString__ = ""
    __usignedString__ = ""
    __operationModeString__ = ""
    __hexCollectFlag__ = 0
    byteCounter = 0
    try:
        for x in range(0, len(_payloadHexArray_)):
            if byteCounter < 26:
                if byteCounter < 2:
                    dataList.append(int(_payloadHexArray_[x], 16))
                else:
                    ascii_array.append(int(_payloadHexArray_[x], 16))
            else:
                if byteCounter == 26:
                    dataList = extract_deviceid(ascii_array, dataList)
                if (
                    int(_payloadHexArray_[x], 16) == SIGNED_CHAR
                ) and __hexCollectFlag__ == 0:
                    _signedCharFlag__ = 1
                    _usignedCharFlag__ = 0
                    __hexCollectFlag__ = 1
                elif (
                    int(_payloadHexArray_[x], 16) == UNSIGNED_CHAR
                ) and __hexCollectFlag__ == 0:
                    _usignedCharFlag__ = 1
                    _signedCharFlag__ = 0
                    __hexCollectFlag__ = 1
                elif (
                    int(_payloadHexArray_[x], 16) == SIGNED_SHORT
                ) and __hexCollectFlag__ == 0:
                    _signedShortFlag__ = 1
                    _usignedShortFlag__ = 0
                    __hexCollectFlag__ = 1
                elif (
                    int(_payloadHexArray_[x], 16) == UNSIGNED_SHORT
                ) and __hexCollectFlag__ == 0:
                    _usignedShortFlag__ = 1
                    _signedShortFlag__ = 0
                    __hexCollectFlag__ = 1
                else:
                    if _usignedCharFlag__ == 1:
                        __usignedString__ = __usignedString__ + _payloadHexArray_[x]
                        if len(__usignedString__) == 2:
                            dataList.append(hex_to_unsigned_char(__usignedString__))
                            __usignedString__ = ""
                            _usignedCharFlag__ = 0
                            __hexCollectFlag__ = 0

                    elif _signedCharFlag__ == 1:
                        __signedString__ = __signedString__ + _payloadHexArray_[x]
                        if len(__signedString__) == 2:
                            dataList.append(hex_to_signed_char(__signedString__))
                            __signedString__ = ""
                            _signedCharFlag__ = 0
                            __hexCollectFlag__ = 0

                    elif _usignedShortFlag__ == 1:
                        __usignedString__ = __usignedString__ + _payloadHexArray_[x]
                        if len(__usignedString__) == 4:
                            dataList.append(hex_to_unsigned_number(__usignedString__))
                            __usignedString__ = ""
                            _usignedShortFlag__ = 0
                            __hexCollectFlag__ = 0

                    elif _signedShortFlag__ == 1:
                        __signedString__ = __signedString__ + _payloadHexArray_[x]
                        if len(__signedString__) == 4:
                            dataList.append(hex_to_signed_number(__signedString__))
                            __signedString__ = ""
                            _signedShortFlag__ = 0
                            __hexCollectFlag__ = 0

                    elif byteCounter == (dataList[2] - 2):
                        dataList.append(int(x, 16))

            byteCounter = byteCounter + 1
        return dataList

    except Exception as e:
        logging.error(
            {
                "Message": "Failed to transform hex to decimal: ",
                "error": str(e),
                "line": format(sys.exc_info()[-1].tb_lineno),
            }
        )

        logging.warning(
            {
                "Message": "Failed to transform hex to decimal: ",
                "error": str(e),
                "line": format(sys.exc_info()[-1].tb_lineno),
            }
        )

    # if len(dataList) > 4:
    #     try:
    #         print(dataList)
    #         _payloadHexArray_ = []
    #     except Exception as e:
    #         logging.error(
    #             {"Message": "Failed to send message to topic, error: ", "error": str(e)}
    #         )


def generate_event_id(deviceid, timestamp):
    raw = f"{deviceid}-{timestamp}"
    return hashlib.sha256(raw.encode()).hexdigest()


def datasource_transformation(dataList) -> str:
    deviceidAscii = [dataList[val] for val in range(2, len(dataList) - 19)]
    deviceid = "".join(map(chr, deviceidAscii))
    json_data = {}
    timestamp = datetime.datetime.now()
    try:
        json_data = {
            "Record_Id": f"{str(timestamp.microsecond) + deviceid}",
            "Device_Id": f"{deviceid}",
            "Data_Length": dataList[1],
            "AC_Input_0": dataList[14],
            "AC_Input_1": dataList[15],
            "AC_Input_2": dataList[16],
            "T1": dataList[17],
            "T2": dataList[18],
            "T3": dataList[19],
            "T4": dataList[20],
            "T5": dataList[21],
            "T6": dataList[22],
            "T7": dataList[23],
            "PCB_NTC": dataList[24],
            "Flow_1": dataList[25],
            "Flow_2": dataList[26],
            "Output_Flag_1": dataList[27],
            "Output_Flag_2": dataList[28],
            "Output_Flag_3": dataList[29],
            "Fan_Tach": dataList[30],
            "Stepper_Position": dataList[31],
            "Flow_Rate": dataList[32],
            "Time_stamp": f"{timestamp}",
            "Event_Id": f"{generate_event_id(deviceid, timestamp)}",
        }
        return json_data
    except Exception as e:
        logging.error(
            {
                "Message": "failed to receive complete dataframe: ",
                "error": str(e),
                "line": format(sys.exc_info()[-1].tb_lineno),
            }
        )

        logging.warning(
            {
                "Message": "failed to receive complete dataframe: ",
                "error": str(e),
                "line": format(sys.exc_info()[-1].tb_lineno),
            }
        )


def on_message(client, userdata, msg):
    global sec_counter
    sec_counter = sec_counter + 1
    payload = msg.payload
    _payloadHexArray_ = []
    payload = payload.hex()
    for j in range(0, len(payload), 2):
        _payloadHexArray_.append(payload[j : j + 2])
    dataList = msgExtraction(_payloadHexArray_)
    json_data = datasource_transformation(dataList=dataList)
    logging.info(json_data)
    if json_data != None and sec_counter == 2:
        kinesis_stream(json_data)
        sec_counter = 0
    # dataList = []


def connect_mqtt():
    # For paho-mqtt 2.0.0, you need to set callback_api_version. and also set the client ID
    _mqtt_client = mqtt_client.Client(
        callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2,
        client_id=client_id,
    )
    # if MQTT is setup with username and password then use the below line
    # client.username_pw_set(username, password)
    # client.on_connect = on_connect
    _mqtt_client.connect(broker, port)
    return _mqtt_client


def main():
    logging.info("Starting PutRecord Producer")
    create_kinesis_data_stream(stream_name=kinesis_stream_name, shard_count=1)
    _mqtt_client = connect_mqtt()
    _mqtt_client.subscribe(topic=topic)
    _mqtt_client.on_message = on_message
    logging.info("---- Producer starts sending data -----")
    # logging.disable(logging.INFO)
    # logging.basicConfig(level=logging.ERROR)
    _mqtt_client.loop_forever()


if __name__ == "__main__":
    main()

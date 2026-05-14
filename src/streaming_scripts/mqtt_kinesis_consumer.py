import argparse
import json
import logging
import sys
import time
from botocore.exceptions import ClientError
import boto3
import random
import mysql.connector
from mysql.connector import Error
import hashlib
from mysql.connector.errors import DatabaseError
from dotenv import load_dotenv
import os

load_dotenv()

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

s3Client = session.client("s3")
s3Resource = session.resource("s3")


# Call the boto3 client with the firehose resource. Assign it to the client variable.
firehose = session.client("firehose")

# get Account ID for firehose
client_accid = session.client("sts")
ACCOUNT_ID = client_accid.get_caller_identity()["Account"]
##################################### variables ##########################################

kinesis_source_stream_name = ""
kinesis_dest_stream_name = ""
rds_host = "grant-db.c3y486a8wivi.eu-north-1.rds.amazonaws.com"
rds_port = 3306
rds_username = "root"
rds_password = "Grant_AWS_1969"
rds_database = "ep011-db"

################################# Logging ###############################################
# All application logs are saved in producer.log file in project directory
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("consumer.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

#################################### parsing cli #########################################
# Parse command line input
parser = argparse.ArgumentParser(description="Parse JSON object from command line")
parser.add_argument("--source_stream", type=str, help="Kinesis source data stream name")
parser.add_argument(
    "--dest_stream", type=str, help="Kinesis destination data stream name"
)
args = parser.parse_args()
kinesis_source_stream_name = args.source_stream
kinesis_dest_stream_name = args.dest_stream


def is_stream_ready(stream_name: str) -> None:
    client = boto3.client("kinesis")
    response = client.describe_stream(StreamName=stream_name)
    return response["StreamDescription"]["StreamStatus"] == "ACTIVE"


def bucketNameValidation(BucketName: str = None) -> int:
    val = 0
    try:
        for bucket in s3Resource.buckets.all():
            # s3BucketList.append(bucket.name)
            # print(bucket)
            if BucketName == bucket.name:
                val = 1
                break
            else:
                continue
        return val
    except Exception as e:
        logging.error(
            {
                "message": "Error creating s3 bucket",
                "error": str(e),
            }
        )


def createBucket(bucketName: str = None, region: str = "eu-north-1") -> None:
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).
    """
    BucketName = bucketName
    # Create bucket
    try:
        if bucketNameValidation(BucketName) == 0:
            if region is None:
                s3Client.create_bucket(Bucket=BucketName)
            else:
                location = {"LocationConstraint": region}
                s3Client.create_bucket(
                    Bucket=BucketName, CreateBucketConfiguration=location
                )
        else:
            logging.info("Bucket exists !!!")
        return
    except ClientError as e:
        logging.error(
            {
                "message": "Error creating s3 bucket",
                "error": str(e),
                "line": format(sys.exc_info()[-1].tb_lineno),
            }
        )


def create_kinesis_firehose(
    firehose_name: str,
    stream_name: str,
    bucket_name: str,
    role_name: str,
    log_group: str,
    log_stream: str,
    account_id: int,
    region: str,
    secrete_name: str,
) -> None:

    # Check if firehose stream already exists
    if firehose_name in firehose.list_delivery_streams()["DeliveryStreamNames"]:
        logging.info(f"Kinesis firehose stream {firehose_name} already exists.")

        # Delete the current delivery stream with the same name
        response = firehose.delete_delivery_stream(
            DeliveryStreamName=firehose_name, AllowForceDelete=True
        )

        # Get status of the delivery stream
        status = firehose.describe_delivery_stream(DeliveryStreamName=firehose_name)[
            "DeliveryStreamDescription"
        ]["DeliveryStreamStatus"]
        logging.info('{} stream "{}" ...'.format(status, firehose_name))

        i = 0
        while status == "DELETING":
            time.sleep(10)
            logging.info(
                'Stream "{}" is being deleted, {} seconds elapsed...'.format(
                    firehose_name, 30 * (i + 1)
                )
            )
            try:
                status = firehose.describe_delivery_stream(
                    DeliveryStreamName=firehose_name
                )["DeliveryStreamDescription"]["DeliveryStreamStatus"]
                i += 1
            except:
                status = "DELETED"
        logging.info('Stream "{}" has been succesfully deleted'.format(firehose_name))

    # Use the create_delivery_stream() method of the client object.
    response_firehose_created = firehose.create_delivery_stream(
        # Pass the firehose name.
        DeliveryStreamName=firehose_name,
        # Specify that the delivery stream uses a Kinesis data stream as a source.
        DeliveryStreamType="KinesisStreamAsSource",
        # Configure the S3 as the destination.
        # S3DestinationConfiguration={
        #     "RoleARN": f"arn:aws:iam::{account_id}:role/{role_name}",
        #     "BucketARN": f"arn:aws:s3:::{bucket_name}",
        #     "Prefix": "firehose/",
        #     "ErrorOutputPrefix": "errors/",
        #     "BufferingHints": {"SizeInMBs": 2, "IntervalInSeconds": 60},
        #     "CompressionFormat": "UNCOMPRESSED",
        #     "CloudWatchLoggingOptions": {
        #         "Enabled": True,
        #         "LogGroupName": log_group,
        #         "LogStreamName": log_stream,
        #     },
        #     "EncryptionConfiguration": {"NoEncryptionConfig": "NoEncryption"},
        # },
        # Configure the mysql database as the destination.
        # Configure the Kinesis Stream as the Source.
        KinesisStreamSourceConfiguration={
            "KinesisStreamARN": f"arn:aws:kinesis:{region}:{account_id}:stream/{stream_name}",
            "RoleARN": f"arn:aws:iam::{account_id}:role/{role_name}",
        },
        DatabaseSourceConfiguration={
            "Type": "MySQL",
            "Endpoint": "ep011.c3y486a8wivi.eu-north-1.rds.amazonaws.com",
            "Port": 3306,
            "Databases": {"Include": ["ep011"]},
            "Tables": {"Include": ["heatpump_data"]},
            "Columns": {
                "Include": [
                    "Record-Id",
                    "Device-Id",
                    "DataLength",
                    "AC_Input_0",
                    "AC_Input_1",
                    "AC_Input_2",
                    "T1",
                    "T2",
                    "T3",
                    "T4",
                    "T5",
                    "T6",
                    "T7",
                    "PCB NTC",
                    "Flow-1",
                    "Flow-2",
                    "Output_Flag-1",
                    "Output_Flag-2",
                    "Output_Flag-3",
                    "Fan_Tach",
                    "Stepper_Position",
                    "Flow_Rate",
                    "Time-stamp",
                ]
            },
            "SurrogateKeys": ["ID"],
            "DatabaseSourceAuthenticationConfiguration": {
                "SecretsManagerConfiguration": {
                    "SecretARN": f"arn:aws:secretsmanager:{region}:{account_id}:secret:{secrete_name}-UzILlQ",
                    "RoleARN": f"arn:aws:iam::{account_id}:role/{role_name}",
                    "Enabled": True,
                }
            },
            "SnapshotWatermarkTable": "heatpump_data",
            "DatabaseSourceVPCConfiguration": {
                "VpcEndpointServiceName": f"com.amazonaws.{region}.rds"
            },
        },
    )

    # Get status of the delivery stream
    status = firehose.describe_delivery_stream(DeliveryStreamName=firehose_name)[
        "DeliveryStreamDescription"
    ]["DeliveryStreamStatus"]
    logging.info('{} stream "{}" ...'.format(status, firehose_name))

    # Wait until the delivery stream is active
    i = 0
    while status == "CREATING":
        time.sleep(10)
        logging.info(
            'Stream "{}" is being created, {} seconds elapsed...'.format(
                firehose_name, 30 * (i + 1)
            )
        )
        status = firehose.describe_delivery_stream(DeliveryStreamName=firehose_name)[
            "DeliveryStreamDescription"
        ]["DeliveryStreamStatus"]
        i += 1

    # Check that the delivery stream is active
    if status == "ACTIVE":
        logging.info('Stream "{}" has been succesfully created'.format(firehose_name))
        stream_arn = response_firehose_created["DeliveryStreamARN"]
        logging.info('Stream "{}" ARN: {}'.format(firehose_name, stream_arn))
    elif status == "CREATING_FAILED":
        logging.info('Stream "{}" creation has failed'.format(firehose_name))

    return


def create_kinesis_data_stream(stream_name: str, shard_count: int = 2) -> None:
    try:
        # Check if the data stream already exists
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
                'Kinesis destination Stream "{}" is being created, {} seconds elapsed...'.format(
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


class ShardIteratorPair:
    """This class represents a pair consisting of a shard ID and its
    corresponding shard iterator. It's used to store information about shards
    and their iterators.
    """

    def __init__(self, shard_id, iterator):
        self.shard_id = shard_id
        self.iterator = iterator


def fetch_shards_and_iterators(kinesis, stream_name: str) -> list:
    """This function retrieves a list of shard iterators for the specified
    Kinesis stream. It iterates over all shards in the stream, retrieves
    their iterators using "LATEST" as the iterator type (Start reading just after the most recent record in the
    shard, so that you always read the most recent data in the shard. ), and stores the shard
    ID and iterator in a list of ShardIteratorPairs. It handles pagination if
    the number of shards exceeds the limit returned by the API.

    Args:
        kinesis (boto3 client): Boto3 client for kinesis resources
        stream_name (str): Kinesis data stream name

    Returns:
        List: Pair of ShardId and corresponding Iterator
    """
    logging.info(f"Fetching shard Iterators ---")
    shard_iterators = []
    response_shards = kinesis.list_shards(StreamName=stream_name)
    while response_shards["Shards"]:
        for shard in response_shards["Shards"]:
            shard_id = shard["ShardId"]
            itr_response = kinesis.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                # ShardIteratorType="TRIM_HORIZON",  which starts reading from the oldest available data in the shard
                ShardIteratorType="LATEST",
            )
            shard_itr = ShardIteratorPair(shard_id, itr_response["ShardIterator"])
            shard_iterators.append(shard_itr)

        if "NextToken" in response_shards:
            response_shards = kinesis.list_shards(
                StreamName=stream_name, NextToken=response_shards["NextToken"]
            )
        else:
            break

    return shard_iterators


def register_consumer(kinesis, kinesis_source_stream: str = "") -> dict:
    consumer_name = f"amzn-consumer-{str(int(random.random() * 10))}"
    streamARN = f"arn:aws:kinesis:{REGION}:{ACCOUNT_ID}:stream/{kinesis_source_stream}"
    try:
        consumer_reg_res = kinesis.register_stream_consumer(
            StreamARN=streamARN,
            ConsumerName=consumer_name,
        )
        print(consumer_reg_res)
        return consumer_reg_res
    except Exception as e:
        logging.error(
            {
                "message": "Error registering kinesis consumer",
                "error": str(e),
                "line": format(sys.exc_info()[-1].tb_lineno),
            }
        )
        kinesis.deregister_stream_consumer(
            StreamARN=streamARN,
            ConsumerName=consumer_name,
        )


def consumer_subscrition(
    kinesis, shard_iterators, kinesis_source_stream, consumer_reg_res
) -> dict:
    consumer_sub_res = {}
    try:
        for shard_itr in shard_iterators:
            response = kinesis.subscribe_to_shard(
                ConsumerARN=consumer_reg_res["Consumer"]["ConsumerARN"],
                ShardId=shard_itr.shard_id,
                StartingPosition={"Type": "LATEST"},
            )
            consumer_sub_res[shard_itr.shard_id : response]
            print(consumer_reg_res)
            return consumer_sub_res
    except Exception as e:
        logging.error(
            {
                "message": "Error consumer shard subscriptions",
                "error": str(e),
                "line": format(sys.exc_info()[-1].tb_lineno),
            }
        )


def rds_connection(
    host: str, database: str, username: str, password: str, port: int
) -> dict:
    dict = {}
    retry = 0
    try:
        connection = mysql.connector.connect(
            host=host, database=database, user=username, password=password, port=port
        )
        if connection.is_connected():
            connection.autocommit = False
            db_Info = connection.get_server_info()
            logging.info("Connected to MySQL Server version " + str(db_Info))
            cursor = connection.cursor()
            connection.autocommit = True
            dict["cursor"] = cursor
            dict["connection"] = connection
            return dict
            # get all records
        else:
            if retry != 5:
                logging.warning("retry database connection :" + str(retry))
                retry += 1
                rds_connection()
            else:
                logging.error(
                    {
                        "message": f"Failed to connect to {database} check credentials !!",
                        "error": str(e),
                        "line": format(sys.exc_info()[-1].tb_lineno),
                    }
                )

    except Error as e:
        logging.error(
            {
                "message": "Failed to connect to RDS database",
                "error": str(e),
                "line": format(sys.exc_info()[-1].tb_lineno),
            }
        )


def rds_put_records(rd_connection_obj: dict, data: json) -> int:

    try:
        insert_query = f'INSERT INTO `ep011-db`.`heatpump_data` (`Record_Id`,`Device_Id`, `Data_Length`, `AC_Input_0`,\
                            `AC_Input_1`, `AC_Input_2`,`T1`, `T2`, `T3`, `T4`, `T5`, `T6`, `T7`,\
                            `PCB_NTC`, `Flow_1`, `Flow_2`, `Output_Flag_1`, `Output_Flag_2`, `Output_Flag_3`,\
                            `Fan_Tach`, `Stepper_Position`, `Flow_Rate`, `Time_stamp`, `SequenceNumber`) \
                            VALUES("{data["Record_Id"]}",\
                            "{data["Device_Id"]}", {data["Data_Length"]}, \
                            {data["AC_Input_0"]}, {data["AC_Input_1"]}, {data["AC_Input_2"]},\
                            {data["T1"]} , {data["T2"]}, {data["T3"]}, {data["T4"]} , {data["T5"]}, {data["T6"]}, {data["T7"]},\
                            {data["PCB_NTC"]} , {data["Flow_1"]}, {data["Flow_2"]}, {data["Output_Flag_1"]} , {data["Output_Flag_2"]}, {data["Output_Flag_3"]},\
                            {data["Fan_Tach"]} , {data["Stepper_Position"]}, {data["Flow_Rate"]}, \'{data["Time_stamp"]}\', "{data["SequenceNumber"]}")'
        rd_connection_obj["cursor"].execute(insert_query)
        # print(insert_query)
        rd_connection_obj["connection"].commit()

    except Exception as e:
        logging.error(
            {
                "message": "Error executing insert query ",
                "error": str(e),
                "line": format(sys.exc_info()[-1].tb_lineno),
            }
        )
        return 0
    except DatabaseError as e:
        logging.error(
            {
                "message": "Error executing insert query ",
                "error": str(e),
                "line": format(sys.exc_info()[-1].tb_lineno),
            }
        )
        raise
    return 1


def poll_shards(
    kinesis, shard_iterators, kinesis_dest_stream_name: str, rds_connection: dict
) -> None:
    """This function continuously polls the shards for data. It iterates
    over the list of shard iterators, fetching records from each shard using
    the respective iterator. For each record retrieved, it logs the order
    data along with the shard ID and sequence number. It updates the shard
    iterator to the next iterator if available.

    Args:
        kinesis (boto3 client): Boto3 client for kinesis resources
        shard_iterators (List): Pair of ShardId and corresponding Iterator
    """
    logging.info(f"Polling shards---")
    consumer_response = ""
    records_response = None
    min_counter = 0
    while True:
        for shard_itr in shard_iterators:
            try:
                records_response = kinesis.get_records(
                    ShardIterator=shard_itr.iterator, Limit=10000
                )
                for record in records_response["Records"]:
                    min_counter = min_counter + 1
                    consumer_response = json.loads(record["Data"].decode("utf-8"))
                    try:
                        consumer_response["SequenceNumber"] = (
                            # hashlib.sha256(record["SequenceNumber"].encode()).hexdigest() +
                            consumer_response["Event_Id"]
                        )
                        if min_counter >= 10:
                            insert_result = rds_put_records(
                                rds_connection, consumer_response
                            )
                            if insert_result == 1:
                                logging.info(
                                    f"--- Consumer Record Event-ID{consumer_response["Event_Id"]} from Shard {shard_itr.shard_id} Inserted into DB Successfully with {min_counter} mins counter ---"
                                )
                            min_counter = 0
                    except DatabaseError as e:
                        rds_connection = rds_connection(
                            rds_host, rds_database, rds_username, rds_password, rds_port
                        )
                        insert_result = rds_put_records(
                            rds_connection, consumer_response
                        )
                        if insert_result == 1:
                            logging.debug(
                                f"--- Consumer Record Event-ID{consumer_response["Event_Id"]} from Shard {shard_itr.shard_id} Inserted into DB Successfully with {min_counter} mins counter ---"
                            )
                        min_counter = 0
                        continue
                        # return
                        # else:
                        #     continue
                    response = kinesis.put_record(
                        StreamName=kinesis_dest_stream_name,
                        Data=json.dumps(consumer_response).encode("utf-8"),
                        PartitionKey=consumer_response["Device_Id"],
                    )
                    logging.info(
                        f"Produced record {response['SequenceNumber']} to Shard {response['ShardId']}"
                    )

                if records_response["NextShardIterator"]:
                    shard_itr.iterator = records_response["NextShardIterator"]

            except Exception as e:
                logging.error(
                    {
                        "message": "Failed to stream data in consumer pipeline",
                        "error": str(e),
                        "line": format(sys.exc_info()[-1].tb_lineno),
                    }
                )

                logging.warning(
                    {
                        "message": "Failed to stream data in consumer pipeline",
                        "error": str(e),
                        "line": format(sys.exc_info()[-1].tb_lineno),
                    }
                )

    time.sleep(1)


def main():
    logging.info("Starting GetRecords Consumer")
    createBucket(bucketName="amzn-ep011-s3-bucket", region=REGION)
    create_kinesis_data_stream(stream_name=kinesis_dest_stream_name, shard_count=1)
    rds_connection_obj = rds_connection(
        rds_host, rds_database, rds_username, rds_password, rds_port
    )

    # delivery stream s3 storage.
    # create_kinesis_firehose(
    #     firehose_name="amzn-ep011-firehose",
    #     stream_name=kinesis_dest_stream_name,
    #     bucket_name="amzn-ep011-s3-bucket",
    #     role_name="amzn-ep011-firehose-role",
    #     log_group="amzn-ep011-firehose-log-group",
    #     log_stream="amzn-ep011-firehose-log-stream",
    #     account_id=ACCOUNT_ID,
    #     region=REGION,
    #     secrete_name="rds-secrets",
    # )
    # while 1:
    shard_iterators = fetch_shards_and_iterators(kinesis, kinesis_source_stream_name)
    # consumer_reg_res = register_consumer(kinesis, kinesis_source_stream_name)
    # while consumer_reg_res["Consumer"]["ConsumerStatus"] != "ACTIVE":
    # #     continue
    # consumer_sub_res = consumer_subscrition(
    #     kinesis, shard_iterators, kinesis_source_stream_name, consumer_reg_res
    # )
    # print(consumer_sub_res)
    time.sleep(1)
    # logging.disable(logging.INFO)
    # logging.basicConfig(level=logging.ERROR)
    poll_shards(kinesis, shard_iterators, kinesis_dest_stream_name, rds_connection_obj)


if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import utils.utilities as ut
import yaml
import os.path
#added later
import os
import sys


if __name__ == '__main__':
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    #parse parameter here
    n = int(sys.argv[1])
    a = 2
    cmd_parms = []
    for _ in range(n):
        cmd_parms.append(sys.argv[a])
        a += 1

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"])\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    src_list = app_conf["source_list"]
    # Check if passed from cmd line arg then override the above (e.g. source_list=OL,SB)
    src1 = sys.argv[1]
    #for src in src_list:
    for src1 in cmd_parms:
        output_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src
        src_conf = app_conf[src1]
        if src1 == 'OL':
            print('Read loyalty data from SFTP folder and write it to S3 bucket')
            ol_txn_df = spark.read\
                .format("com.springml.spark.sftp")\
                .option("host", app_secret["sftp_conf"]["hostname"])\
                .option("port", app_secret["sftp_conf"]["port"])\
                .option("username", app_secret["sftp_conf"]["username"])\
                .option("pem", os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"]))\
                .option("fileType", "csv")\
                .option("delimiter", "|")\
                .load(src_conf["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv")
            ol_txn_df = ol_txn_df.withColumn("ins_dt", current_date())
            ol_txn_df.show(5, False)
            ut.write_to_s3(ol_txn_df, output_path)

        elif src1 == 'SB':
            print('Read sales data from MySQL db and write it to S3 bucket')
            jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                          "lowerBound": "1",
                          "upperBound": "100",
                          "dbtable": src_conf["mysql_conf"]["dbtable"],
                          "numPartitions": "2",
                          "partitionColumn": src_conf["mysql_conf"]["partition_column"],
                          "user": app_secret["mysql_conf"]["username"],
                          "password": app_secret["mysql_conf"]["password"]
                           }

            print("\nReading data from MySQL DB using SparkSession.read.format(),")
            txn_df = spark\
                .read.format("jdbc")\
                .option("driver", "com.mysql.cj.jdbc.Driver")\
                .options(**jdbc_params)\
                .load()
            txn_df = txn_df.withColumn("ins_dt", current_date())
            txn_df.show()
            ut.write_to_s3(txn_df, output_path)

        elif src1 == 'CP':
            print("\nReading customers data from S3 and write it to s3") # kc_extract.. KC_Extract_1_20171009
            cp_df = spark.read \
                .option("mode", "DROPMALFORMED") \
                .option("header", "false") \
                .option("delimiter", "|") \
                .option("inferSchema", "true") \
                .csv("s3a://" + src_conf["s3_conf"]["s3_bucket"] + "/" + src_conf["s3_conf"]["filename"])
            cp_df = cp_df.withColumn("ins_dt", current_date())
            ut.write_to_s3(cp_df, output_path)

        elif src1 == 'ADDR':
            cust_addr = spark\
                .read\
                .format("com.mongodb.spark.sql.DefaultSource")\
                .option("database", src_conf["mongodb_config"]["database"])\
                .option("collection", src_conf["mongodb_config"]["collection"])\
                .load()
            cust_addr = cust_addr\
                .select(col("consumer_id"),
                        col("mobile-no").alias("mobile"),
                        col("address.street").alias("address"),
                        col("address.city").alias("city"),
                        col("address.state").alias("state")
                        )
            # "consumer_id":"I034867789V","address":{"street":"aca","city":"bangalore","state":"karnataka"},"mobile-no":"7789327282"}
            cust_addr = cust_addr.withColumn("ins_dt", current_date())
            cust_addr.show()
            ut.write_to_s3(cust_addr, output_path)


# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1" com/dsm/src_data__load_2.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import utils.utilities as ut
import yaml
import os.path

if __name__ == '__main__':
    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .config('spark.jars.packages', 'com.springml:spark-sftp_2.11:1.1.1') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    print('Read loyalty data from SFTP folder and write it to S3 bucket')
    ol_txn_df = spark.read\
        .format("com.springml.spark.sftp")\
        .option("host", app_secret["sftp_conf"]["hostname"])\
        .option("port", app_secret["sftp_conf"]["port"])\
        .option("username", app_secret["sftp_conf"]["username"])\
        .option("pem", os.path.abspath(current_dir + "/../../../../" + app_secret["sftp_conf"]["pem"]))\
        .option("fileType", "csv")\
        .option("delimiter", "|")\
        .load(app_conf["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv")
    ol_txn_df = ol_txn_df.witholumn("ins_dt", current_date())
    ol_txn_df.show(5, False)

    ol_txn_df.write\
        .mode("append")\
        .partitionBy("ins_dt")\
        .parquet(app_conf["s3_conf"]["s3_bucket"] + "staging/OL")

    print('Read sales data from MySQL db and write it to S3 bucket')
    jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                  "lowerBound": "1",
                  "upperBound": "100",
                  "dbtable": app_conf["mysql_conf"]["dbtable"],
                  "numPartitions": "2",
                  "partitionColumn": app_conf["mysql_conf"]["partition_column"],
                  "user": app_secret["mysql_conf"]["username"],
                  "password": app_secret["mysql_conf"]["password"]
                   }

    print("\nReading data from MySQL DB using SparkSession.read.format(),")
    txnDF = spark\
        .read.format("jdbc")\
        .option("driver", "com.mysql.cj.jdbc.Driver")\
        .options(**jdbc_params)\
        .load()
    txnDF = txnDF.witholumn("ins_dt", current_date())
    txnDF.show()

    txnDF.write\
        .mode("append")\
        .partitionBy("ins_dt")\
        .parquet(app_conf["s3_conf"]["s3_bucket"] + "staging/SB")

    print("\nReading customers data from S3 and write it to s3")
    print("\nReading customers data from MongoDB and write it to s3")


# spark-submit --packages "com.springml:spark-sftp_2.11:1.1.1" dataframe/ingestion/others/systems/sftp_df.py
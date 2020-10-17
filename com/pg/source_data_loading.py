from pyspark.sql import SparkSession
import yaml
import os.path
import com.pg.utils.utility as utils
import pyspark.sql.functions as f


if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    src_list = app_conf["source_list"]
    for src in src_list:
        src_conf = app_conf[src]
        if src == 'SB':
            sb_df = utils.read_from_mysql(spark, app_secret, src_conf) \
                .withColumn('ins_dt', f.current_date())

            sb_df.show()
            sb_df.write\
                .partitionBy('ins_dt')\
                .mode("append")\
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_area"] + "/" + src)
        elif src == 'OL':
            ol_df = utils.read_from_sftp(spark, app_secret, src_conf) \
                .withColumn("ins_dt", f.current_date())
            ol_df.show()
            ol_df.write \
                .partitionBy('ins_dt') \
                .mode("append") \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_area"] + "/" + src)
        elif src == '1CP':
            cp_df = spark.read\
                .option("header", "true")\
                .option("delimiter", "|")\
                .csv("s3a://" + app_conf["1CP"]["s3_conf"]["s3_bucket"] + "/" + app_conf["1CP"]["s3_conf"]["filename"])
            cp_df.show()
            cp_df = cp_df.withColumn("ins_dt", f.current_date())
            cp_df.write.mode('append').partitionBy("ins_dt").parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_area"] + "/" + src)


        else:
            cust = utils.read_from_mongodb(spark, app_secret,app_conf)
            cust = cust.withColumn("ins_dt",f.current_date())
            cust.write.partitionBy("ins_dt").parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_area"] + "/" + src)



# spark-submit --packages "com.springml:spark-sftp_2.11:1.1.1,mysql:mysql-connector-java:8.0.15,org.apache.hadoop:hadoop-aws:2.7.4" com/pg/source_data_loading.py

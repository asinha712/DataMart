from pyspark.sql import SparkSession
import yaml
import os.path
import com.pg.utils.utility as utils
import pyspark.sql.functions as f
from pyspark.sql.types import StringType
import utils.utility as ut

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )



    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

# Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])


    tgt_list = app_conf["target_list"]
    for src in tgt_list:
        src_conf = app_conf[src]
        if src == 'REGIS_DIM':
            one_cp_df = spark.read \
                .parquet("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/staging_pg/"+src_conf["source_data"]) \
                .repartition(5)
            one_cp_df.show()

            one_cp_df.createOrReplaceTempView("staging_STG_1CP")
            red_df = spark.sql(src_conf["loading_query"])
            jdbcUrl = ut.get_redshift_jdbc_url(app_secret)
            print(jdbcUrl)

            red_df.coalesce(1).write \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", jdbcUrl) \
                .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
                .option("forward_spark_s3_credentials", "true") \
                .option("dbtable", "DATAMART.REGIS_DIM") \
                .mode("overwrite") \
                .save()

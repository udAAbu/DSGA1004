import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def downsampling(spark):
    cf_val = spark.read.parquet('hdfs:/user/bm106/pub/MSD/cf_validation.parquet')
    cf_train = spark.read.parquet("hdfs:/user/bm106/pub/MSD/cf_train.parquet")

    cf_val.createOrReplaceTempView("cf_val")
    val_users = set([row['user_id'] for row in spark.sql("SELECT DISTINCT user_id FROM cf_val").collect()])
    train_sample = cf_train.filter(col('user_id').isin(val_users))
    print(train_sample.count())

    train_sample.write.parquet("hdfs:/user/zn2041/train_sample.parquet")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("downsampling").getOrCreate()
    downsampling(spark)




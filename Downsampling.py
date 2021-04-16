import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def downsampling(spark):
    cf_val = spark.read.parquet('hdfs:/user/bm106/pub/MSD/cf_validation.parquet')
    cf_train = spark.read.parquet("hdfs:/user/bm106/pub/MSD/cf_train.parquet").repartition(100)

    cf_val.createOrReplaceTempView("cf_val")
    val_users = set([row['user_id'] for row in spark.sql("SELECT DISTINCT user_id FROM cf_val").collect()])
    train_sample = cf_train.filter(col('user_id').isin(val_users))

    train_sample.write.mode("overwrite").parquet("hdfs:/user/zn2041/train_sample.parquet")

if __name__ == "__main__":
    '''
    conf = SparkConf()
    conf.set("spark.executor.memory", "16G")
    conf.set("spark.driver.memory", '16G')
    conf.set("spark.executor.cores", "4")
    conf.set('spark.executor.instances','10')
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.default.parallelism", "40")
    conf.set("spark.sql.shuffle.partitions", "40")
    '''    

    spark = SparkSession.builder.appName("downsampling")\
	.config("spark.executor.memory", "32g")\
	.config("spark.driver.memory", "32g")\
	.config("spark.sql.shuffle.partitions", "40")\
	.getOrCreate()

    downsampling(spark)




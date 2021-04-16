import sys
import pyspark
from pyspark.sql import SparkSession

def downsampling(spark, hdfs_path, local_path):
    df = spark.read.parquet(hdfs_path)
    tiny_sample = df.sample(False, 0.01, seed=0)
    tiny_sample.write.mode('overwrite').parquet(local_path)
    print("sample finished")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("downsampling").getOrCreate()
    hdfs_path = sys.argv[1]
    local_path = sys.argv[2]
    downsampling(spark, hdfs_path, local_path)




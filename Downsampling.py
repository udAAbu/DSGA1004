import sys
from pyspark.sql import SparkSession

def downsampling(spark, hdfs_path, new_path):
    df = spark.read.parquet(hdfs_path)
    df.write.parquet(new_path)
    print(df.show(5))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("downsampling").getOrCreate()
    hdfs_path = sys.argv[1]
    new_path = sys.argv[2]
    downsampling(spark, hdfs_path, new_path)




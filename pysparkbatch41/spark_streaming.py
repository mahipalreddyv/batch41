from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *


def sparkStreaming():
    spark=SparkSession.builder.appName("structured Streaming").getOrCreate()
    schema1=StructType([StructField("zip",StringType(),True),
                       StructField("city",StringType(),True),
                       StructField("State",StringType,True)])

    lines = spark.readStream.format("csv").schema(schema1).load(r"D:\Hadoop_iquiz\sparkstraming_input\*.csv")
    query=lines.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()

if __name__ == '__main__':
    sparkStreaming()
from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col,split

def wordcount():
    import sys
    inputpath = sys.argv
    print("command path",inputpath)
    outputpath ="file:///D:/batch41_results_temp/"
    spark=SparkSession.builder.appName("pyspark word count").getOrCreate()
    readTextFile=spark.read.text(inputpath)
    word_split=readTextFile.select(explode(split(col("value"),",")).alias("word"))
    wordcount_df=word_split.groupby("word").count().orderBy("count")
    wordcount_df.show()
    wordcount_df.write.mode("overWrite").option("header",True).format("csv").save(outputpath)

if __name__ == '__main__':
    wordcount()



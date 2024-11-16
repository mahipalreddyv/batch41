from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col,split

def wordcount():
    import sys
    inputpath1 = sys.argv[1]
    outputpath = sys.argv[2]
    print("inputpath  => ",inputpath1)
    print("outputpath => ",outputpath)

    spark=SparkSession.builder.appName("pyspark word count").getOrCreate()
    readTextFile=spark.read.text(inputpath1)
    readTextFile.show()
    word_split=readTextFile.select(explode(split(col("value"),",")).alias("word"))
    wordcount_df=word_split.groupby("word").count().orderBy("count")
    wordcount_df.write.mode("overWrite").option("header",True).format("csv").save(outputpath)

if __name__ == '__main__':
    wordcount()



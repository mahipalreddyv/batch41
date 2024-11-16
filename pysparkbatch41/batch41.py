from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col,split

def wordcount():
    import sys
    inputpath = sys.argv[1]
    oputputpath = sys.argv[2]
    print("inputpath1",inputpath)
    outputpath="file:///D:/batch41_results_temp/"
    spark=SparkSession.builder.appName("pyspark word count").getOrCreate()
    readTextFile=spark.read.format("csv").option("header",True).csv(oputputpath)
    resDF=readTextFile.select("PARTNER_CODE").groupby("PARTNER_CODE").count().orderBy("count")
    resDF.show()
    resDF.write.mode("overWrite").option("header",True).format("csv").save(outputpath)

if __name__ == '__main__':
    wordcount()



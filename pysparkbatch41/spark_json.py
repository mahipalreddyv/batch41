from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col,split

def jsonRead():
    import sys
    inputpath = sys.argv[1]
    #schemajsonpath = sys.argv[2]
    print("inputpath ====> ",inputpath)
    #outputpath = sys.argv[2]
    #print("oputputpath ====> ", outputpath)
    #outputpath="file:///D:/batch41_results_temp/"

    spark=SparkSession.builder.appName("pyspark json data ").getOrCreate()
    json_schema = spark.read.json("file:///D:/Hadoop_iquiz/jsonfiles/batch_33_json/batch41.json").schema

    readjsonDF=spark.read.schema(json_schema).json(inputpath)
    expr = """ events.client as clientName
        ,events.beaconVersion as bversion
        ,events.data.displayAd.amazonA9HB  as amazonA9HB
        ,events.data.displayAd.instanceID as instanceid
    	,events.data.displayAd.inView as inView
        ,events.data.milestones.indexExchangeRequested 
        ,events.data.milestones.adRequested as adRequest 
    	,events.data.milestones.amazonA9BidsRequested as amazonA9BidsRequested
    	""".split(",")
    print(json_schema)
    finalDF=readjsonDF.selectExpr(expr)
    finalDF.show(10,False)
    finalDF.where("clientName!='NULL'").coalesce(1).write.mode("overWrite").format("avro").partitionBy("clientName").save("file:///D:/Hadoop_iquiz/jsonfiles/batch_33_json/output_batch41")
if __name__ == '__main__':
    jsonRead()
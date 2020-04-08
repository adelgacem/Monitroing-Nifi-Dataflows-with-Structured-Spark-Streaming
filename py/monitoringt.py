from ast import literal_eval
from pyspark.sql.functions import when
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import json
from datetime import datetime
import time


schema_kafka = StructType([
	StructField("Id", StringType(), True),
	StructField("NifiTime", TimestampType(), True),
	StructField("NifiStartedTime", TimestampType(), True),
	StructField("Fname", StringType(), True),
	StructField("Volume", LongType(), True),
	StructField("isStarted", IntegerType(), True),
	StructField("isLanded", IntegerType(), True),
	StructField("Maxitime", IntegerType(), True),
	StructField("FlagPath", StringType(), True)
	])

spark = SparkSession.builder.appName("Monitoring").getOrCreate()



def WINDOWS(df):


	StreamFlows = df.withWatermark("NifiStartedTime", "24 hours") \
		.groupBy(
		window(df.NifiStartedTime, "5 minutes", "5 minutes"),
		df.Id, df.Fname,df.FlagPath) \
		.agg(min("NifiStartedTime").alias("MinNifiStartedTime"), max("NifiTime").alias("MaxNifiTime"), sum("isStarted").alias("StartedFlowsNbr"), sum("isLanded").alias("CompletedFlowsNbr"), sum("Volume").alias("TotalVolume"),max("Maxitime").alias("Maxitime"))



	return StreamFlows
	

def TRAITEMENT(StreamFlows):
	StreamFlows.createOrReplaceTempView("StreamFlows")		
	StreamFlowsStatus = spark.sql("select window, Fname , Id as FlowId, MinNifiStartedTime as FirstStartedFlowTime, StartedFlowsNbr,Maxitime,FlagPath, \
			CompletedFlowsNbr, TotalVolume , '*****' AS Velocity  , 0 AS delay , \
			CASE \
			WHEN CompletedFlowsNbr=0 THEN null \
			ELSE  MaxNifiTime END AS LastCompletedFlowTime, \
			CASE \
			WHEN CompletedFlowsNbr=0 and StartedFlowsNbr>=1  THEN 'Started' \
			WHEN StartedFlowsNbr > CompletedFlowsNbr THEN 'Running' \
			WHEN StartedFlowsNbr == CompletedFlowsNbr AND MaxNifiTime > MinNifiStartedTime + INTERVAL 15 SECOND THEN 'Landed' \
			ELSE 'Unknown' END AS LastStatus, \
			CASE \
			WHEN CompletedFlowsNbr=0 and StartedFlowsNbr>=1  THEN 2 \
			WHEN StartedFlowsNbr > CompletedFlowsNbr THEN 1 \
			WHEN StartedFlowsNbr == CompletedFlowsNbr AND MaxNifiTime > MinNifiStartedTime + INTERVAL 15 SECOND THEN 0 \
			ELSE 3 END AS LastStatusValue \
			from StreamFlows")


	StreamFlowsStatus.createOrReplaceTempView("StreamSpeedFlows")
	StreamFlowsSpeed = spark.sql("select window, FlowId, Fname, FirstStartedFlowTime, LastCompletedFlowTime , Maxitime, TotalVolume, LastStatus, LastStatusValue,ErrorFlowsNbr, StartedFlowsNbr, CompletedFlowsNbr,FlagPath,  \
			(to_unix_timestamp(LastCompletedFlowTime, 'yyyy-MM-dd HH:mm:ss')/60)-(to_unix_timestamp(FirstStartedFlowTime, 'yyyy-MM-dd HH:mm:ss')/60) as delay, \
			CASE \
			WHEN (to_unix_timestamp(LastCompletedFlowTime, 'yyyy-MM-dd HH:mm:ss')/60)-(to_unix_timestamp(FirstStartedFlowTime, 'yyyy-MM-dd HH:mm:ss')/60) < Maxitime \
			THEN '0' \
			ELSE '1' \
			END AS Velocity \
			from StreamSpeedFlows")
			
	FlowsStatusSpeedValue = StreamFlowsSpeed.withColumn("value", to_json(struct([StreamFlowsSpeed[x] for x in StreamFlowsSpeed.columns])))
	WriteFlowsStatusSpeedValue = FlowsStatusSpeedValue \
		.selectExpr("CAST(value AS STRING)") \
		.writeStream \
		.format("kafka") \
		.option("kafka.bootstrap.servers", "${kafka_server}:6667") \
		.option("checkpointLocation", "${hdfs_path}") \
		.outputMode("update") \
		.option("topic", "${write_topic}") \
		.start()
	
	


def main():
	dsraw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "${kafka_server}:6667").option("subscribe", "${read_topic}").option("startingOffsets", "latest").option("failOnDataLoss", "false").load()
	ds = dsraw.selectExpr("CAST(value AS STRING)")
	df = ds.select(from_json(col("value"), schema_kafka).alias("MyFlows"))
	df = df.selectExpr("MyFlows.Id","MyFlows.NifiTime", "MyFlows.NifiStartedTime", "MyFlows.Fname", "MyFlows.Volume", "MyFlows.isStarted", "MyFlows.isLanded", "MyFlows.Maxitime", "MyFlows.FlagPath")
	
	StreamFlows = WINDOWS(df)
	TRAITEMENT(StreamFlows)
	spark.streams.awaitAnyTermination()
	

if __name__ == '__main__':
	main()
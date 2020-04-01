# Monitroing Nifi Dataflows with Structured Spark Streaming

Gols :

1.	Visual Application Ingestion Monitoring
2.	Application SLA Integration
3.	Central Program Pipeline 

Basic Principal

![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/d1.png) 
Diagram.1

The rule is simple, we just send some (we’ll start with success outputs) events from NIFI to a running program, the latter will check the event content and verify to which Ingestion it’s belong, and what action to do.
Let’s Start
Here is of a common NIFI Dataflow Pipeline. Off Corse we generally have much more steps, but the actual presented principal will be the same.

![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/d2.png) 
Diagram.2

Nifi use the First processor to List a Database Tables, The Second one will Fetch the Data from each table, and the last one will Put results into HDFS.
First Goals
We’ll need to follow this demo Ingestion called “DEM01” into a centralized Dashboard, separating each Ingestion by an “ID”. The other ingestion are Real Production Ingestions 😊. The first column is the Functional Name, others are described in the title and will be explained later.


![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/d3.png) 
Diagram.3

In this stage or version of the Program, “we’ll focus on the number of succeeded ingested tables” to consider the status of the ingestion. Thus:
-	If the Number of arrived tables (Noted as “Arrived” in the last panel and calculated by the Put-HDFS success events) is equal to “Tables Number” calculated by the List-Table success events) we consider that the ingestion is done (Landed).
-	If the “Arrived” (Put HDFS) number is higher than what was expected, the Ingestion status will stay as “Running” and ;
-	In both cases if the SLA defined Time is not reached, the velocity it will be noted as “OnTime”, otherwise its considered as “Delayed” (we can define more conditions later to consider an ingestion as Failed by routing errors too from NIFI but this will be done in V2 of the program).
-	If a special case happens as we see in the Panel (this can happen if a bad restart of NIFI was done or somebody cleared manually flow files into NIFI the “Need attention” status is displayed.
Remark : The code will only send numeric exit Status and the colors are managed into Grafana.
Challenge and Solution Concept
T
he main problem is that ingestions solutions are not aware about what can be called as “a new ingestion” comparing to another one … each time a scheduled task is done, nifi will realize the action without saying “ah it’s new ingestion phase”. But how to separate old events (flow files) entry into nifi from new one’s ?  A very complicated DATES exercises can be very stressful. 

Luckily, Structured Spark Streaming group natively data events by specific criteria (w’ll use startup dates for that).
Imagine each Database Ingestion as a Trip, each table as a Passenger. If you restart the same ingestion several times, how is it possible to dissociate the same passenger from different Trips ?

The solution is to group all passengers by “Departures Window Time Frame”.

Let’s says DBDEM01 have 3 tables TB1, TB2 and DB3.
If I start the ingestion of DBDEMO 2, 3 or more times during the same day, Let’s say at 09:00 and 10:00 , well just force Nifi to send each event to Spark (via kafka), and tell to Spark to reorganize events  :
-	List Database send Events : 

DBDM01/TB1 09h00m00s…
DBDM01/TB2 09h00m00s…
DBDM01/TB3 09h00m01s…
DBDM01/TB1 10h00m00s…
DBDM01/TB2 10h00m00s…
DBDM01/TB3 10h00m02s…


![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/d4.png) 

Diagram.4

When we’ll check into Arrivals states, we’ll ask to Spark to regroup those events(passengers) By Startup Frames. This will facilitate the work to count passengers by Trip or Flight ID, and decide with status to give into the monitoring dashboard.
Spark will group two trips by their “ID” and Startup window time frame (we decided that 2mn is enough to say same ingestions (same ID) cannot start during less than 2mn two times).
For the given example the result will be the following
Functional Name	ID	Departure Time	Nb of Discovered Tables (Passengers)	Ingested Volume	Nb of Ingested Tables (arrived passengers)	SLA (Time defined as acceptable to wait)	Actual Velocity TAG	Status	Arrived Time (all tables so finish Time)
DEM01	1001	09h00	3	90 GB	2	6h	Delayed	InProgress	
DEM01	1001	10h00	3	100	3	6h	OnTime	LANDED	2020-01-26 16:00:19
And if you decide to display only the last status you will have to the last Ingestion status only.

# Solution Description Diagram

![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/d5.png)  

Diagram.5

1.	NIFI send results status to somewhere (Kafka) of the monitored steps (In our example the startup with the list database table and the end which is the putHDFS processor).
For now, we'll focus only on SUCCES status (It is a good goal to stream all steps and Errors too, but this can be done into a second stage).
2.	We chare the SLA table into a different Kafka Topic, which will be used to define for each Ingestion ID what are the conditions of success, and the what is the Time to consider that an ingestion has reached the maximum acceptable time. This is a functional decision to take with the business (after considering the technical prerequisites, an advice : negotiate for the maximum acceptable time window).  A nifi dedicated data flow can be used for that (do not stress your Database consuming each 5mn is enough while those info are manually updated each time a new ingestion is created to be monitored).
3.	The code (Spark Structured Streaming) will process each Event to Update the status step of the ingestion(s) :
   (Started, Running, Landed, Need Attention (Error or other cases)), and determine if the ingestion is "On Time" or "Delayed" by comparing the duration Time and the SLA described into the SLA Reference Table.

4.	The code will publish the results and store them (into another output Kafka Topic)
5.	Another NIFI dataflow read from the output Topic to update results into ElasticSearch (we could go directly from Spark but it’s simpler to isolate and respect a microservices approach while we have already NIFI let’s use it’s simplicity to the this tasks, this will help in the furtur to maintain the code and in case of troubleshooting it’s much more easier).
6.	See results into Grafana
7.	Some Users need a File as a flag to consider that Ingestion is finished and into Hadoop/OOZIE for example configure to start workflows.
8.	Why not directly start Other Jobs or do any other action without using a FLAG as into step 7 (but it’s out of the scope 😊).

Here is another view for the same kinematic but showing efforts while adding the :
-	“Nifi efforts to perform by streaming events”
-	Storing events 
-	Processing events

![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/d5-5.png) 
# How to Add events into NIFI

![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/d6.png) 
Diagram.6

The "STARTED" Update Attributes configuration
![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/update_started.png) 
The "Landed" Update Attributes configuration
![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/update_running.png) 

The "SQLDONE" Update Attrbutes configuration is not used in the Release 1 of our program.

The ReplaceText processor
 
where replaementValue is equal to the following :

{"Id":"${Id}","NifiTime":"${now():format("yyyy-MM-dd HH:mm:ss")}","NifiStartedTime":"${NifiStartedTime}","Fname":"${Fname}","Volume":${Volume},"isStarted":${isStarted},"isLanded":${isLanded},"Maxitime":${EXPIRED_TIME},"FlagPath":"${PATH_HDFS_FLAG}"}

The Pubilsh Kafka processor depends on your Kafka version & configurations (secured or not, port ..etc).

Now that we do have NIFI status events into Kafka Topic 1, what to do with those data ?

 

# Structured Spark Streaming Code description
1.	Conditions 
1.	In this simple code :), we consider that an ingestion can run several times a day but not to times in a time window less than 2mn. This is the minimum Window Time where the same ingestion doesn't have to start twice.
2.	Each End to End Ingestion (List database until put HDFS) need to spend more than 1 minute.  
3.	if (a) and (b) conditions are not respected you will need to change the Window frequency into the code (condition (a)) and changeg the time limite defining the status  (condition b))
Actual Code (To Attach)
kafka_schema_Topic1 = StructType([
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

# Production Validation 

* Memory Usage : As wetermarking is in place, old events are filtred (see image bellow)
Watermarking capture from one Dataflow event :
 

* ElasticSearch Best Practises are to respect and out of the scope. 
# Monitor your code 
- Restart it in case of crash when using OOZIE or another scheduler to restart the code if needed.
  You can check simply with monitor activiry from NIFI and post to another Kafka Topic to KILL and Restart the actual runinng code, but this is out of the scope and is more related to monitoring streaming applications.

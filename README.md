# Monitroing Nifi Dataflows with Structured Spark Streaming

Goals :

1.	Visual Application Ingestion Monitoring
2.	Application SLA Integration
3.	Central Program Pipeline 

Basic Principal

![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/d1.png) 
Diagram.1

The rule is simple :), we have to send events from NIFI to Kafka (we’ll start with success outputs from PutHdfs), and the running application will check the Event-contents and verify to which Ingestion it’s belong, it will update the ingestion with the right status. That's all.

Let’s Start :
Here is a common NIFI Dataflow Pipeline. Off Corse we generally have much more steps, but the presented principal will be the same.

![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/d2.png) 
Diagram.2

The 1rst Nifi processor will list a Database Tables, the Second will Fetch the Data from each table, and the last one will Put results into HDFS.
Other more complicated patterns can be treated with the same approache, but we'll start whith this simple case.
First Goals
We would like to follow the status of the Ingestion called “DEM01” into a centralized Dashboard, separating Ingestions by a specific identifier called “ID”. The first column is the Functional Name, others are described in the title and will be explained later.


![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/d3.png) 
Diagram.3

In this stage or version of the Program, “we’ll focus on the number of succeeded ingested tables” to determine the status of the ingestion. Thus:
-	If the Number of arrived tables is equal to “Tables Number” (total of succedded copies which belong to the same started ingestion) we can consider the ingestion steps are done (Landed). 
-	If the SLA (defined acceptable time) is not exceeded, it will be noted as “OnTime”, otherwise its considered as “Delayed” ingestion (we can define more conditions later to consider an ingestion as Failed by routing errors, musuring volumes variations ..etc but this will be done in the second stage of the program).
-	If a special case happens as we see in the Panel (this can happen if Nifi craches or manually clear flow files into NIFI) the “Need attention” status is displayed.

Remark : The code will send numeric exit Status, and the colors are managed into Grafana.
Challenge and Solution Concept
The main problem is that ingestions solutions are not aware about what can be called as “a new ingestion” comparing to another one … each time a scheduled task is done, nifi will realize the transfert without saying “ah it’s new ingestion phase”... So "how can we separate old events (flow files) entry from new one’s ?"  A very complicated DATES exercises can be very stressful...

Luckily, Structured Spark Streaming group natively data events by specific criteria (w’ll use The Nifi Start dates for that).
L'ets imagine each Database Ingestion as a Trip, and each table as a Passenger. If you restart the same ingestion several times, how can we dissociate the same passenger from different Trips ?

The solution is to group all passengers by “Departures Window Time Frame”.

Let’s says DBDEM01 have 3 tables TB1, TB2 and DB3.
If I restart the ingestion of DBDEMO1 two, three or more times during the same day, Let’s say at 09:00 and 10:00 , Nifi will send each event (via kafka), and let Spark do the job of events reorganizing :
-	List Database will send those Events : 

DBDM01/TB1 09h00m00s…
DBDM01/TB2 09h00m00s…
DBDM01/TB3 09h00m01s…
DBDM01/TB1 10h00m00s…
DBDM01/TB2 10h00m00s…
DBDM01/TB3 10h00m02s…


![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/d4.png) 

Diagram.4

When we’ll check into the Arrivals states, Spark groups those events(passengers) By Startup Frames. This will facilitate the work to count passengers by Trip or Flight ID, and decide which status to display into the monitoring dashboard.
Spark will group two trips by their “ID” and Startup window time frame (2mn are enough for me to say same ingestions (same ID) cannot start during less than 2mn two times, but you can adpat this value later).
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

![alt tag](https://github.com/adelgacem/Monitroing-Nifi-Dataflows-with-Structured-Spark-Streaming/blob/master/image/replacetext.png) 


{"Id":"${Id}","NifiTime":"${now():format("yyyy-MM-dd HH:mm:ss")}","NifiStartedTime":"${NifiStartedTime}","Fname":"${Fname}","Volume":${Volume},"isStarted":${isStarted},"isLanded":${isLanded},"Maxitime":${Maxitime},"FlagPath":"${FlagPath}"}

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


# how to execute your code
you need to add in your spark-submit

--jars spark-streaming-kafka-0-8_2.11-2.3.0.2.6.5.0-292.jar,spark-streaming-kafka-0-8-assembly_2.11-2.3.0.2.6.5.0-292.jar,spark-sql-kafka-0-10_2.11-2.3.0.2.6.5.0-292.jar,hdfs://Your_HDFS_PATH/JARY/kafka-clients-0.10.2.2.jar

from os import environ
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t
from datetime import datetime
import math

environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell'

def convDateTime(x):
    strDT =  f"{datetime.strftime(datetime.date(datetime.now()),'%Y-%m-%d')} {x}" 
    
    tmpMonth = int(strDT[5:7])
    tmpDay = int(strDT[8:10])
    tmpHour = int(strDT[11:13])
        
    if tmpHour < 24:
        return datetime.strptime(strDT,'%Y-%m-%d %H:%M:%S')
    elif tmpHour == 24:
        newHour = '00'
        newDay = str(tmpDay + 1)
        if newDay != '32':
            resStr = strDT[:8] + newDay + strDT[10:11] + newHour + strDT[13:]
        else:
            newDay = '01'
            newMonth = str(tmpMonth + 1)
            resStr = strDT[:5] + newMonth + strDT[7:8] + newDay + strDT[10:11] + newHour + strDT[13:]
            
        return datetime.strptime(resStr,'%Y-%m-%d %H:%M:%S')
    elif tmpHour == 25:
        newHour = '01'
        newDay = str(tmpDay + 1)
        if newDay != '32':
            resStr = strDT[:8] + newDay + strDT[10:11] + newHour + strDT[13:]
        else:
            newDay = '01'
            newMonth = str(tmpMonth + 1)
            resStr = strDT[:5] + newMonth + strDT[7:8] + newDay + strDT[10:11] + newHour + strDT[13:]
            
        return datetime.strptime(resStr,'%Y-%m-%d %H:%M:%S')

def secsFrom1970(x):
    try:
        epoch = datetime.utcfromtimestamp(0)
        return (x - epoch).total_seconds()
    except:
        return "Error"

def measureDist(lat1, lon1, lat2, lon2) :
    R = 6378.137  # Radius of earth in KM
    dLat = lat2 * math.PI / 180 - lat1 * math.PI / 180
    dLon = lon2 * math.PI / 180 - lon1 * math.PI / 180
    a = math.sin(dLat/2) * math.sin(dLat/2) + \
        math.cos(lat1 * math.PI / 180) * math.cos(lat2 * math.PI / 180) * \
        math.sin(dLon/2) * math.sin(dLon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c
    return d * 1000  # meters



bootstrap_servers = 'ec2-54-172-56-67.compute-1.amazonaws.com:19092'

topic = 'bus'
udfSecsFrom1970 = f.udf(secsFrom1970, t.FloatType())
udfMeasureDist = f.udf(measureDist, t.FloatType())

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

df_tram_tmp = spark.read.format('parquet').load('./Schedule/df_bus.parquet')

rdd1 = df_tram_tmp.rdd.map(list)
rdd2 = rdd1.map(lambda x: [x[0], x[1], x[2], x[3], convDateTime(x[4]), x[5], x[6]])
rdd3 = rdd2.map(lambda x: [x[0], x[1], x[2], x[3], x[4], x[5], x[6], secsFrom1970(x[4])])
df_tram = rdd3.toDF(['line','busStopName', 'sLat', 'sLon', 'sTime', 'sVehicleID', 'busStopID', 'sTimer'])

raw_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", topic) \
    .load()

schema = t.StructType() \
    .add("Lines", t.StringType()) \
    .add("Lon", t.FloatType()) \
    .add("VehicleNumber", t.StringType()) \
    .add("Time", t.TimestampType()) \
    .add("Lat", t.FloatType()) \
    .add("Brigade", t.StringType())

stream_tram = raw_df.select( \
    raw_df.key.cast('string'),
    f.from_json(raw_df.value.cast('string'), schema))

stream_tram = stream_tram.withColumnRenamed('from_json(CAST(value AS STRING))', 'json')

stream_tram = stream_tram \
    .withColumnRenamed('key', 'vVehicleID') \
    .withColumn('vLon', stream_tram.json.Lon) \
    .withColumn('vLat', stream_tram.json.Lat) \
    .withColumn('vTime', stream_tram.json.Time)

stream_tram = stream_tram.drop(stream_tram.json)
stream_tram = stream_tram.withColumn('vTimer', udfSecsFrom1970(f.col('vTime')))

accDiff = 0.00005
cond1 = [df_tram.sVehicleID == stream_tram.vVehicleID, \
        df_tram.sLat - stream_tram.vLat < accDiff, \
        df_tram.sLat - stream_tram.vLat > -accDiff, \
        df_tram.sLon - stream_tram.vLon < accDiff, \
        df_tram.sLon - stream_tram.vLon > -accDiff ]


query1 = stream_tram\
    .join(df_tram, cond1)\
    .withColumn('del', f.abs(df_tram.sTimer - stream_tram.vTimer))\
    .withColumn('timestamp', f.unix_timestamp(f.col('vTime'), "MM/dd/yyyy hh:mm:ss aa").cast(t.TimestampType())) \
    .withWatermark("timestamp", "1 minutes") \
    .groupBy(f.col('vVehicleID'), f.col('busStopName'), f.col('busStopID'), f.col('line'), f.col('timestamp')).agg(f.min('del').alias('delay'))

# Wyjście do Kafki
q1 = query1\
    .selectExpr("CAST(vVehicleID AS STRING) AS key", "to_json(struct(*)) AS value")\
    .writeStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("topic", "busDelay") \
    .option("checkpointLocation", "chckptn") \
    .start()
q1.awaitTermination(7200)
q1.stop()

# #WYJŚCIE NA KONSOLĘ
# q1 = query1.writeStream.outputMode("update").format("console").start()
# q1.awaitTermination(60)
# q1.stop()

# WYJŚCIE DO CASSANDRY
# q1 = query1.writeStream\
#     .option("checkpointLocation", '/tmp/check_point/')\
#     .format("org.apache.spark.sql.cassandra")\
#     .option("spark.cassandra.connection.host", cassandraHost)\
#     .option("keyspace", "single")\
#     .option("table", "delays")\
#     .outputMode(outputMode='complete')\
#     .start()
# q1.awaitTermination(60)
# q1.stop()

# cond2 = [df_tram.sVehicleID == stream_tram.vVehicleID, \
#         df_tram.sTime == stream_tram.vTime ]

# query2 = stream_tram.join(df_tram, cond2).withColumn('distanceInMeters', udfMeasureDist(f.col('sLat'), f.col('sLon'), f.col('vLat'), f.col('vLon')))
# q2 = query2.writeStream.outputMode("append").format("console").start()
# q2.awaitTermination(120)
# q2.stop()


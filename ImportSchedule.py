from urllib.request import urlopen 
from pyspark.sql import SparkSession, Row
from pyspark.sql import SQLContext
from pyspark.sql import functions as f
from collections import OrderedDict
import json
import requests
import numpy
from datetime import datetime

spark = SparkSession.builder.appName("Import").getOrCreate()
sc = spark.sparkContext

def convert_to_row(d: dict) -> Row:
    return Row(**OrderedDict(d.items()))

def get_data_from_ztm(url, resDf):
    response = urlopen(url)
    string_obj = response.read().decode('utf-8')
    dict_obj = json.loads(string_obj)
    
    l1 = dict_obj.get('result')
    l2 = list()
    l3 = list()
    d1 = dict()

    for v in l1:
        l2.append(v.get('values'))

    for v2 in l2:
        for elem in v2:
            d1[elem['key']] = elem['value']
        l3.append(d1.copy())
        
    rdd = spark.sparkContext.parallelize(l3)
    df = rdd.map(convert_to_row).toDF()
    
    if resDf:
        return df
    else:
        return rdd

def get_bus_stops_ZTM(resDf):
    
    url = 'https://api.um.warszawa.pl/api/action/dbstore_get/?id=ab75c33d-3a26-4342-b36a-6e5fef0a3ac3&apikey=a9ee09f1-6975-46fc-ae46-df9b805ab6f6'
    response = urlopen(url)
    string_obj = response.read().decode('utf-8')
    dict_obj = json.loads(string_obj)
    
    l1= dict_obj.get('result')
    l2 = list()
    l3 = list()
    d1 = dict()

    for v in l1:
        l2.append(v.get('values'))

    for v2 in l2:
        for elem in v2:
            d1[elem['key']] = elem['value']
        l3.append(d1.copy())
        
    rdd = spark.sparkContext.parallelize(l3).map(convert_to_row)
    df = rdd.toDF()
    
    if resDf:
        return df
    else:
        return rdd

def get_lines_on_bus_stop(busstopId, busstopNr):
    url = 'https://api.um.warszawa.pl/api/action/dbtimetable_get/?id=88cd555f-6f31-43ca-9de4-66c479ad5942&busstopId=' + busstopId + '&busstopNr=' + busstopNr + '&apikey=a9ee09f1-6975-46fc-ae46-df9b805ab6f6'
    response = urlopen(url)
    string_obj = response.read().decode('utf-8')
    dict_obj = json.loads(string_obj)
    
    l1= dict_obj.get('result')
    l2 = list()
    l3 = list()
    d1 = dict()

    for v in l1:
        l2.append(v.get('values'))

    for v2 in l2:
        for elem in v2:
            d1[elem['key']] = elem['value']
        l3.append(d1.copy())
    
#     rdd = spark.sparkContext.parallelize(l3)#.map(convert_to_row)
    arr = numpy.array(l3)
    
    return arr

def get_schedule_of_line(busstopId, busstopNr, line):
    url = 'https://api.um.warszawa.pl/api/action/dbtimetable_get/?id=e923fa0e-d96c-43f9-ae6e-60518c9f3238&busstopId=' + busstopId + '&busstopNr=' + busstopNr + '&line=' + line + '&apikey=a9ee09f1-6975-46fc-ae46-df9b805ab6f6'
    response = urlopen(url)
    string_obj = response.read().decode('utf-8')
    dict_obj = json.loads(string_obj)
    
    l1= dict_obj.get('result')
    l2 = list()
    l3 = list()
    d1 = dict()

    for v in l1:
        l2.append(v.get('values'))

    for v2 in l2:
        for elem in v2:
            d1[elem['key']] = elem['value']
        l3.append(d1.copy())
    
#     rdd = spark.sparkContext.parallelize(l3)#.map(convert_to_row)
    arr = numpy.array(l3)
    
    return arr

def convDateTime(x):
    strDT =  f"{datetime.strftime(datetime.date(datetime.now()),'%Y-%m-%d')} {x}" 
    
    tmpMonth = int(strDT[5:7])
    tmpDay = int(strDT[8:10])
    tmpHour = int(strDT[11:13])
        
    if tmpHour != 24:
        return datetime.strptime(strDT,'%Y-%m-%d %H:%M:%S')
    else:
        newHour = '00'
        newDay = str(tmpDay + 1)
        if newDay != '32':
            resStr = strDT[:8] + newDay + strDT[10:11] + newHour + strDT[13:]
        else:
            newDay = '01'
            newMonth = str(tmpMonth + 1)
            resStr = strDT[:5] + newMonth + strDT[7:8] + newDay + strDT[10:11] + newHour + strDT[13:]
            
        return datetime.strptime(resStr,'%Y-%m-%d %H:%M:%S')

def secsFrom1970(x):
    epoch = datetime.utcfromtimestamp(0)
    return (x - epoch).total_seconds()

df_busStops = get_bus_stops_ZTM(True)
df_busStops = df_busStops.drop('id_ulicy', 'obowiazuje_od')

url = 'https://api.um.warszawa.pl/api/action/dbstore_get/?id=ab75c33d-3a26-4342-b36a-6e5fef0a3ac3&apikey=a9ee09f1-6975-46fc-ae46-df9b805ab6f6'
df1 = get_data_from_ztm(url, True)
df1 = df1.select('zespol', 'slupek')

rdd1 = df1.rdd.map(list)
rdd2 = rdd1.map(lambda x: [[x[0], x[1], line['linia']] for line in get_lines_on_bus_stop(x[0], x[1])])
rdd2 = rdd2.flatMap(lambda x: x)
df2 = rdd2.toDF(['zespol', 'slupek', 'linia'])
df3 = df_busStops.join(df2, on = ['zespol', 'slupek'], how = 'outer')
rdd3 = rdd2.map(lambda x: [[x[0], x[1], x[2], elem['brygada'], elem['czas']] for elem in get_schedule_of_line(x[0], x[1], x[2])])
rdd3 = rdd3.flatMap(lambda x: x)
df4 = rdd3.toDF(['zespol', 'slupek', 'linia', 'brygada', 'czas'])
df5 = df3.join(df4, on = ['zespol', 'slupek', 'linia'], how = 'outer')
df5 = df5.filter(df5.linia.rlike('^\d+'))
df5 = df5.filter(df5.szer_geo != 'null').filter(df5.dlug_geo != 'null')
rdd4 = df5.rdd.map(list)
rdd5 = rdd4.map(lambda x: [x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], f"{x[2]}/{x[7]}"])
rdd6 = rdd5.map(lambda x: [x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], f"{x[0]}/{x[1]}"])
rdd7 = rdd6.map(lambda x: [x[0], x[1], int(x[2]), x[3], float(x[4]), float(x[5]), x[6], x[7], x[8], x[9], x[10]])
print("Started long operation")
df_all_1 = rdd7.toDF(['busStopGroupID', 'busStopNr', 'line', 'busStopName', 'sLat', 'sLon', 'dir', 'brigade', 'sTime', 'vehicleID', 'busStopID'])
df_all_2 = df_all_1.drop('busStopGroupID', 'busStopNr', 'dir',  'brigade')
df_all = df_all_2.dropna()
df_bus = df_all.filter(df_all.line >= 100)
df_tram = df_all.filter(df_all.line < 100)

df_bus.write.save('df_bus.parquet')
df_tram.write.save('df_tram.parquet')
df_all.write.save('df_all.parquet')
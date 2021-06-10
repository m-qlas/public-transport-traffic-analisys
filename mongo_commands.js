{ "_id" : ObjectId("60bd458292cad83c80fee9fc"), "vVehicleID" : "24/8", "busStopName" : "Gocławek", "busStopID" : "2014/03", "line" : 24, "timestamp" : "2021-06-06T23:40:02.000Z", "delay" : 18408 }
db.tram.insertOne({
    vVehicleID: '999',
    busStopName: 'Testowy przystanek',
    busStopID: '9999/99',
    delay: 123456
})

db.tram.find(
    {"line" : 33},
    {"timestamp": 0}
).sort(
    {"delay": -1}
).limit(10)

db.tram.aggregate(
    [
        {$match:{$and:[{delay: {$lt: 10000}},{line: {$lt: 100}}]}},
        {$group: {_id: {vehicleID: '$vVehicleID', busStopName: '$busStopName', busStopID: '$busStopID', line: '$line'}, delay: {$min: '$delay'}}},
        {$group: 
            {
                _id: "$_id.busStopName",
                avgDelay: {$avg: "$delay"}
            }
        },
        {$sort: {avgDelay: 1}},
        {$limit: 10}
])

db.bus.aggregate(
    [
        {$match:{$and:[{delay: {$lt: 10000}},{line: {$gt: 100}}]}},
        {$group: {_id: {vehicleID: '$vVehicleID', busStopName: '$busStopName', busStopID: '$busStopID', line: '$line'}, delay: {$min: '$delay'}}},
        {$group: 
            {
                _id: "$_id.busStopName",
                avgDelay: {$avg: "$delay"}
            }
        },
        {$sort: {avgDelay: 1}},
        {$limit: 10}
])

db.tram.aggregate(
    [
        {$match:{$and:
                [
                    {delay: {$lt: 10000}},
                    {line: {$lt: 100}}
                ]
            }
        },
        {$group: 
            {
                _id: "$line",
                avgDelay: {$avg: "$delay"}
            }
        },
        {$sort: {avgDelay: -1}}
])


db.tram.aggregate(
    [
        {$match:{$and:[{delay: {$lt: 10000}},{line: {$lt: 100}}]}},
        {$group: {_id: {timestamp: '$timestamp'}, avgDelay: {$avg: '$delay'}}},
        {$sort: {timestamp: 1}}
])

db.tram.aggregate(
    [
        {$match:{$and:[{delay: {$lt: 10000}},{line: {$lt: 100}}]}},
        {$project: 
            {
                delay: 1,
                date:{$dateFromString:{dateString:'$timestamp'}},
                hour: {$hour: {date: '$date'}}
        }}
])

db.tram.aggregate(
    [
        {$match:{$and:[{delay: {$lt: 10000}},{line: {$lt: 100}}]}},
        {$project: {vVehicleID: 1, busStopName: 1, busStopID: 1, line: 1, delay: 1, hour:{$hour:{$dateFromString:{dateString:'$timestamp'}}}}},
        {$group: {_id: {vehicleID: '$vVehicleID', busStopName: '$busStopName', busStopID: '$busStopID', line: '$line', hour: '$hour'}, delay: {$min: '$delay'}}},
        {$group: {_id: '$_id.hour', avgDelayPerHour:{$avg:'$delay'}}},
        {$sort: {_id: 1}}
])

db.bus.aggregate(
    [
        {$match:{$and:[{delay: {$lt: 10000}},{line: {$gt: 100}}]}},
        {$project: {vVehicleID: 1, busStopName: 1, busStopID: 1, line: 1, delay: 1, hour:{$hour:{$dateFromString:{dateString:'$timestamp'}}}}},
        {$group: {_id: {vehicleID: '$vVehicleID', busStopName: '$busStopName', busStopID: '$busStopID', line: '$line', hour: '$hour'}, delay: {$min: '$delay'}}},
        {$group: {_id: '$_id.hour', avgDelayPerHour:{$avg:'$delay'}}},
        {$sort: {_id: 1}}
])

db.tram.aggregate(
    [
        {$match:{$and:[{delay: {$lt: 10000}},{line: {$lt: 100}}]}},
        {$group: {_id: {timestamp: '$timestamp'}, delay: {$min: '$delay'}}},
        {$group: 
            {
                _id: "$_id.busStopName",
                avgDelay: {$avg: "$delay"}
            }
        },
        {$sort: {avgDelay: 1}},
        {$limit: 10}
])
//Grupowanie po reżyserze, sortowane po liczbie filmów malejąco
db.movies.aggregate(
    [
        {
            $group:
            {
                _id:"$directed_by",
                liczbaFilmow:{$sum:1}
            }
        },
        {$sort: {liczbaFilmow: -1}},
        {$limit: 5}
    ])

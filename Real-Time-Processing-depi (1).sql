WITH 
Congestion AS (
    SELECT
        System.Timestamp AS EventTime,
        location,
        AVG(avg_speed) AS avg_speed,
        SUM(traffic_volume) AS total_vehicle_count,
        SUM(car_count) AS car_count,
        SUM(truck_count) AS truck_count,
        SUM(bike_count) AS bike_count,
        weather,
        'Congestion Detected' AS AlertType
    FROM
        [mariam-hub] TIMESTAMP BY timestamp
    GROUP BY
        TumblingWindow(minute, 1), location, weather
    HAVING
        SUM(traffic_volume) > 25 
),

HighSpeed AS (
    SELECT
        System.Timestamp AS EventTime,
        location,
        avg_speed,
        traffic_volume AS total_vehicle_count,
        car_count,
        truck_count,
        bike_count,
        weather,
        'High Speed Detected' AS AlertType
    FROM
        [mariam-hub] TIMESTAMP BY timestamp
    WHERE
        avg_speed > 100
),

Accidents AS (
    SELECT
        System.Timestamp AS EventTime,
        location,
        avg_speed,
        traffic_volume AS total_vehicle_count,
        car_count,
        truck_count,
        bike_count,
        weather,
        'Possible Accident/Anomaly' AS AlertType
    FROM
        [mariam-hub] TIMESTAMP BY timestamp
    WHERE
        avg_speed < 40 AND traffic_volume > 20 AND
        (weather IN ('Rainy', 'Foggy', 'Windy'))
)

-- Send results to Power BI
SELECT * 
INTO [Real-Time-traffic]
FROM Congestion
UNION ALL
SELECT * FROM HighSpeed
UNION ALL
SELECT * FROM Accidents;

-- Send results to Blob Storage
SELECT * 
INTO [TrafficOutput]
FROM Congestion
UNION ALL
SELECT * FROM HighSpeed
UNION ALL
SELECT * FROM Accidents;
-- ============================================
--   Congestion Detection Logic
-- ============================================

WITH CongestionDetected AS (
    SELECT
        System.Timestamp AS EventTime,
        location,
        AVG(avg_speed) AS avg_speed,
        SUM(traffic_volume) AS total_vehicle_count,
        'Traffic Congestion Detected' AS AlertType,
        CASE
            WHEN AVG(avg_speed) < 65 AND SUM(traffic_volume) > 300 THEN 3
            WHEN AVG(avg_speed) < 70 AND SUM(traffic_volume) > 250 THEN 2
            ELSE 0
        END AS AlertLevel
    FROM
        [mariam-hub] TIMESTAMP BY timestamp
    GROUP BY
        TUMBLINGWINDOW(minute, 1),
        location
    HAVING
        SUM(traffic_volume) > 250
),


-- ============================================
--   High & Low Speed Alerts (Simpler Thresholds)
-- ============================================
TrafficAlerts AS (
    SELECT
        System.Timestamp AS EventTime,
        location,
        AVG(avg_speed) AS avg_speed,
        CASE
            WHEN AVG(avg_speed) > 95 THEN 'Critical Speed Alert'
            WHEN AVG(avg_speed) BETWEEN 80 AND 95 THEN 'High Speed Warning'
            ELSE 'Normal Speed'
        END AS AlertType,
        CASE
            WHEN AVG(avg_speed) > 95 THEN 3
            WHEN AVG(avg_speed) BETWEEN 80 AND 95 THEN 2
            ELSE 0
        END AS AlertLevel
    FROM
        [mariam-hub] TIMESTAMP BY timestamp
    GROUP BY
        TUMBLINGWINDOW(second, 30),
        location
)
-- Output: Speed Alerts (Level 2+)
SELECT
    EventTime,
    location,
    avg_speed,
    AlertType,
    AlertLevel
INTO
    AlertsOutput
FROM
    TrafficAlerts
WHERE AlertLevel >= 2;

-- Output:Congestion Alerts (Level 2+)
SELECT
    EventTime,
    location,
    avg_speed,
    total_vehicle_count,
    AlertType,
    AlertLevel
INTO
    congestionutput
FROM
    CongestionDetected
WHERE AlertLevel >= 2;



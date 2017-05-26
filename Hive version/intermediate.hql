INSERT INTO hiveint
SELECT 
    time,
    tag,
    Counts,
    sum(counts) OVER (
        PARTITION BY tag 
        ORDER BY time ASC 
        RANGE BETWEEN ${hiveconf:window} PRECEDING and CURRENT ROW) as counts
FROM hiverawtags;

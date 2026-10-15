# Assignment #3: Report

**Name: Ed Cruz **
**Student ID: 801337361**
**Email: ecruz13@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`:

The commands you ran, in order:

1. Generate data
```bash
python3 datagen.py 801337361
```
2. Start the cluster
```bash
docker compose up -d
```
3. Run the job
```bash
docker cp main.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output
```
4. Stop the cluster
```bash
docker compose down
```
---

## Part 1: load and clean

How you implemented rule 1 (dropoff after pickup) and rule 2 (fare present and numeric):

**Rule 1:** Added a boolean column `bad_time` that is true when `dropoff_ts <= pickup_ts`. Filtered out all rows where this condition is true.

**Rule 2:** Used `col("fare").try_cast(DoubleType())` to convert the fare string to a double. Non-numeric values (like "N/A" or empty strings) become null. Added a boolean column `bad_fare` that is true when the fare is null. Filtered out all rows where this condition is true.

Why `fare` is read as a string, and what would have happened with a `DOUBLE` column:

The `fare` column is read as a string to keep bad values visible. If it were read as a `DOUBLE` column, all non-numeric would have been converted automatically values to `null`. This would make it cleaning more complicated. By reading as a string and using `try_cast`, we can count how many bad fares exist.


The five counts:

| rows_total | bad_time | bad_fare | removed | rows_kept |
|------------|----------|----------|---------|-----------|
| 100000     | 1434     | 1510     | 2944    | 97056     |

Two or three bad rows, as they appear in `trips.csv`:

```
T000014,D066,4,19,2026-09-28 14:07:06,2026-09-28 14:16:59,1.69,,cash
T000015,D141,16,6,2026-09-05 14:32:03,2026-09-05 13:54:25,7.82,31.38,card
T001188,D126,2,18,2026-09-25 07:36:55,2026-09-25 07:45:55,2.41,N/A,app
```

---

## Part 2a: trips and revenue per zone per day

First ten rows, and one sentence on what they show:

```
+----------+-------+---------------+-----+-------+
|day       |zone_id|zone_name      |trips|revenue|
+----------+-------+---------------+-----+-------+
|2026-09-01|1      |Uptown Center  |313  |6220.69|
|2026-09-01|2      |First Ward     |252  |4994.21|
|2026-09-01|3      |Fourth Ward    |195  |3668.98|
|2026-09-01|4      |South End      |272  |5344.46|
|2026-09-01|5      |Dilworth       |191  |3743.34|
|2026-09-01|6      |Plaza Midwood  |171  |3455.7 |
|2026-09-01|7      |NoDa           |147  |2963.27|
|2026-09-01|8      |University City|186  |3702.05|
|2026-09-01|9      |Ballantyne     |118  |2138.22|
|2026-09-01|10     |SouthPark      |160  |3203.84|
+----------+-------+---------------+-----+-------+

```
This shows the daily revenue and trip count for each pickup zone, allowing analysis of which zones are most active and profitable on different days.


## Part 2b: each driver's busiest day

First ten rows, and one sentence on what they show:

```
+---------+----------+-----+
|driver_id|day       |trips|
+---------+----------+-----+
|D001     |2026-09-05|19   |
|D002     |2026-09-22|25   |
|D003     |2026-09-11|20   |
|D004     |2026-09-20|23   |
|D005     |2026-09-16|18   |
|D006     |2026-09-04|19   |
|D007     |2026-09-03|17   |
|D008     |2026-09-10|20   |
|D009     |2026-09-09|22   |
|D010     |2026-09-20|21   |
+---------+----------+-----+

```
This shows each driver busiest day (day with most trips), with ties broken by earliest date, showing which day each driver had the most activity.


## Part 3: activity per borough (Spark SQL)

Your SQL query and its result:

```sql
SELECT 
    z.borough,
    COUNT(*) as trips,
    ROUND(SUM(t.fare), 2) as revenue,
    ROUND(AVG(t.distance_km), 2) as avg_distance_km
FROM trips t
JOIN zones z ON t.pickup_zone = z.zone_id
GROUP BY z.borough
ORDER BY revenue DESC
```

```
+-------+-----+---------+---------------+
|borough|trips|revenue  |avg_distance_km|
+-------+-----+---------+---------------+
|Central|40180|798508.32|5.39           |
|South  |17071|336520.7 |5.34           |
|North  |15030|299125.88|5.4            |
|East   |13882|274529.52|5.36           |
|West   |10893|217113.85|5.4            |
+-------+-----+---------+---------------+
```

---

## The two plans

`explain()` output of part 2a (DataFrame API):

```
=== Physical plan of part 2a (DataFrame API) ===
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [day#65 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(day#65 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=1561]
      +- Project [day#65, zone_id#9, zone_name#10, trips#115L, round(revenue#116, 2) AS revenue#131]
         +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
            :- HashAggregate(keys=[day#65, pickup_zone#2], functions=[count(1), sum(fare#64)])
            :  +- Exchange hashpartitioning(day#65, pickup_zone#2, 200), ENSURE_REQUIREMENTS, [plan_id=1554]
            :     +- HashAggregate(keys=[day#65, pickup_zone#2], functions=[partial_count(1), partial_sum(fare#64)])
            :        +- Project [pickup_zone#2, try_cast(fare#7 as double) AS fare#64, cast(pickup_ts#4 as date) AS day#65]
            :           +- Filter (((((isnotnull(dropoff_ts#5) AND isnotnull(pickup_ts#4)) AND isnotnull(fare#7)) AND (dropoff_ts#5 > pickup_ts#4)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
            :              +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,fare#7] Batched: false, DataFilters: [isnotnull(dropoff_ts#5), isnotnull(pickup_ts#4), isnotnull(fare#7), (dropoff_ts#5 > pickup_ts#4)..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(dropoff_ts), IsNotNull(pickup_ts), IsNotNull(fare), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,fare:string>
            +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1557]
               +- Filter isnotnull(zone_id#9)
                  +- FileScan csv [zone_id#9,zone_name#10] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,zone_name:string>
```

`explain()` output of part 3 (Spark SQL):

```
=== Physical plan of part 3 (Spark SQL) ===
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [revenue#206 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(revenue#206 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=1611]
      +- HashAggregate(keys=[borough#11], functions=[count(1), sum(fare#64), avg(distance_km#6)])
         +- Exchange hashpartitioning(borough#11, 200), ENSURE_REQUIREMENTS, [plan_id=1608]
            +- HashAggregate(keys=[borough#11], functions=[partial_count(1), partial_sum(fare#64), partial_avg(distance_km#6)])
               +- Project [distance_km#6, fare#64, borough#11]
                  +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
                     :- Project [pickup_zone#2, distance_km#6, try_cast(fare#7 as double) AS fare#64]
                     :  +- Filter (((((isnotnull(dropoff_ts#5) AND isnotnull(pickup_ts#4)) AND isnotnull(fare#7)) AND (dropoff_ts#5 > pickup_ts#4)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
                     :     +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,distance_km#6,fare#7] Batched: false, DataFilters: [isnotnull(dropoff_ts#5), isnotnull(pickup_ts#4), isnotnull(fare#7), (dropoff_ts#5 > pickup_ts#4)..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(dropoff_ts), IsNotNull(pickup_ts), IsNotNull(fare), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,distance_km:double,fare:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1603]
                        +- Filter isnotnull(zone_id#9)
                           +- FileScan csv [zone_id#9,borough#11] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,borough:string>
```

Your comparison: where the file scans are, which operator is the join and of which kind,
where the exchanges are and why, and what the two plans have in common although they were
written in different languages.

Both plans place their file scans at the bottom leaf nodes with same source filters, execute an inner BroadcastHashJoin, and use three exchanges (BroadcastExchange to distribute the lookup table, HashPartitioning to group rows for aggregation, and RangePartitioning for final sorting). Even though they use different interfaces languages, they share identical data sources, cleaning steps, and core execution patterns that are optimized by the catalyst.


---

## At scale

With 200 million trips instead of 100,000: what would you keep as it is, and what would you
change first? One paragraph.

The data cleaning logic would remain the same, as it uses efficient DataFrame operations that scale well. The grouping and aggregation queries would stay the same as they also scale well with Spark's distributed processing. However, I would split the data in partitions for the trips table by day to optimize the zone revenue query. For large datasets, I would also consider using columnar storage formats like Parquet for intermediate results to reduce memory usage and improve input/output performance.




---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.

Initially I had an issue when using `try_cast()`. I tried to import it, and it caused an import error because `try_cast()` does not exist inside the `pyspark.sql.functions module`.
After reading some documentation for PySpark, I realized that `try_cast()` was designed as a method belonging to the Column class, rather than a standalone function. Because it is already built into every column created with `col()`, there is nothing to import, so `try_cast()` can be called directly on the column itself.



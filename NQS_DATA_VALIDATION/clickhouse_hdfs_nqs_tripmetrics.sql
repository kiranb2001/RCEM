Tripmetrics to NQS Temporary Mapping
------------------------------------
SELECT
    tripstatus,
    count(distinct imsi) AS roamer_count,
    sum(scorenumerator) AS numerator_score,
    sum(scoredenominator) AS denominator_score
FROM rcem_nqs_dly_tmp
WHERE (serviceid = 0) and (ingredientid = -1) AND (eventtype = -1) AND (rat = -1) group by tripstatus order by tripstatus

select tripstatus,count(distinct imsi) as roamer_count,sum(overallavgscore) as numerator_score, count(*) as denominator_score from trip_metrics_closed where tripstatus in (2,3) and bintime = (select max(bintime) from trip_metrics_open where tripstatus in (0,1)) and overallbucketid<>0 and triptype in ('C', 'N') group by tripstatus


NQS Temporary to NQS Final Table
--------------------------------
SELECT
    tripstatus,
    count(distinct imsi) AS roamer_count,
    sum(scorenumerator) AS numerator_score,
    sum(scoredenominator) AS denominator_score
FROM rcem_nqs_dly_tmp
WHERE (serviceid = 0) and (ingredientid = -1) AND (eventtype = -1) AND (rat = -1) group by tripstatus

SELECT
    tripstatus,
    uniqCombined64Merge(imsi_list) AS total_roamer_count,
	uniqCombined64Merge(imsi_list_success) AS sucess_roamer_count,
	uniqCombined64Merge(imsi_list_failed) AS failed_roamer_count,
	sumMerge(sum_score_numerator) AS numerator_score,
	sumMerge(sum_score_denominator_success) AS denominator_score
FROM rcem_nqs_dly
WHERE (serviceid = 0) AND (ingredientid = -1) AND (eventtype = -1) AND (rat = -1)


HDFS NQS DATA
--------------

SELECT
    tripstatus
    wisdom_count_distinct_long(imsilist) AS roamer_count,
    sum(scorenumerator) AS numerator_score,
    sum(scoredenominator) AS denominator_score
FROM rcem_nqs_dly
WHERE (serviceid = 0) and (ingredientid = -1) AND (eventtype = -1) AND (rat = -1) group by tripstatus


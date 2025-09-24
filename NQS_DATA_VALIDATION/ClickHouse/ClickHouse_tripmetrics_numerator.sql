WITH tripmetrics_numerator AS (
    SELECT
        partnercountryid,
        partnernetworkid,
        tripstatus,
        triptype,
        overallavgscore,
        overallavgscoreexcludingsor,
        regavgscore,
        regsuccessratioscore,
        reglatencyscore,
        regavgscoreexcludingsor,
        regsuccessratioscoreexcludingsor,
        reglatencyscoreexcludingsor,
        CAST((mocallsuccess3g + mtcallsuccess3g) AS DOUBLE)
            / NULLIF(CAST((mocallattempts3g + mtcallattempts3g) AS DOUBLE), 0) AS nerratio3g,
        CAST((mocallsuccessvolte + mtcallsuccessvolte) AS DOUBLE)
            / NULLIF(CAST((mocallattemptsvolte + mtcallattemptsvolte) AS DOUBLE), 0) AS nerratiovolte,
        CAST((mocallsuccess5g + mtcallsuccess5g) AS DOUBLE)
            / NULLIF(CAST((mocallattempts5g + mtcallattempts5g) AS DOUBLE), 0) AS nerratio5g,
        -- Combined nerratio: use ifNull on components to avoid NULL sums
        CAST((IFNULL(mocallsuccess3g,0) + IFNULL(mtcallsuccess3g,0) + IFNULL(mocallsuccessvolte,0) + IFNULL(mtcallsuccessvolte,0) + IFNULL(mocallsuccess5g,0) + IFNULL(mtcallsuccess5g,0)) AS DOUBLE)
            / NULLIF(CAST((IFNULL(mocallattemptsvolte,0) + IFNULL(mtcallattemptsvolte,0) + IFNULL(mocallattempts3g,0) + IFNULL(mtcallattempts3g,0) + IFNULL(mocallattempts5g,0) + IFNULL(mtcallattempts5g,0)) AS DOUBLE), 0) AS nerratio,
        CAST((IFNULL(mocallanswered3g,0) + IFNULL(mtcallanswered3g,0)) - ((IFNULL(mocalldrop3g,0) + IFNULL(mtcalldrop3g,0))) AS DOUBLE)
            / NULLIF(CAST((IFNULL(mocallanswered3g,0) + IFNULL(mtcallanswered3g,0)) AS DOUBLE), 0) AS ccrratio3g,
        CAST((IFNULL(mocallansweredvolte,0) + IFNULL(mtcallansweredvolte,0)) - ((IFNULL(mocalldropvolte,0) + IFNULL(mtcalldropvolte,0))) AS DOUBLE)
            / NULLIF(CAST((IFNULL(mocallansweredvolte,0) + IFNULL(mtcallansweredvolte,0)) AS DOUBLE), 0) AS ccrratiovolte,
        CAST(((IFNULL(mocallanswered5g,0) + IFNULL(mtcallanswered5g,0) + IFNULL(mocallansweredvolte,0) + IFNULL(mtcallansweredvolte,0) + IFNULL(mocallanswered3g,0) + IFNULL(mtcallanswered3g,0)) -
             (IFNULL(mocalldrop5g,0) + IFNULL(mtcalldrop5g,0) + IFNULL(mocalldropvolte,0) + IFNULL(mtcalldropvolte,0) + IFNULL(mocalldrop3g,0) + IFNULL(mtcalldrop3g,0))) AS DOUBLE)
            / NULLIF(CAST((IFNULL(mocallanswered5g,0) + IFNULL(mtcallanswered5g,0) + IFNULL(mocallansweredvolte,0) + IFNULL(mtcallansweredvolte,0) + IFNULL(mocallanswered3g,0) + IFNULL(mtcallanswered3g,0)) AS DOUBLE), 0) AS ccrratio,
        IFNULL(mtcallpddunderthreshold3g,0) + IFNULL(mocallpddunderthreshold3g,0) AS totalpddunderthreshold3g,
        IFNULL(mtcallpddunderthresholdvolte,0) + IFNULL(mocallpddunderthresholdvolte,0) AS totalpddunderthresholdvolte,
        IFNULL(mtcallpddunderthreshold5g,0) + IFNULL(mocallpddunderthreshold5g,0) AS totalpddunderthreshold5g,
        (IFNULL(mtcallpddunderthreshold3g,0) + IFNULL(mocallpddunderthreshold3g,0) + IFNULL(mtcallpddunderthresholdvolte,0) + IFNULL(mocallpddunderthresholdvolte,0) + IFNULL(mtcallpddunderthreshold5g,0) + IFNULL(mocallpddunderthreshold5g,0)) AS totalpddunderthreshold,
        callavgscore3g,
        callavgscorevolte,
        callavgscore5g,
        callavgscore,
        CAST((IFNULL(sessionattempts3g,0) - IFNULL(sessionfailures3g,0)) AS DOUBLE)
            / NULLIF(CAST(IFNULL(sessionattempts3g,0) AS DOUBLE), 0) AS data_establishment_3g,
        CAST((IFNULL(sessionattempts4g,0) - IFNULL(sessionfailures4g,0)) AS DOUBLE)
            / NULLIF(CAST(IFNULL(sessionattempts4g,0) AS DOUBLE), 0) AS data_establishment_4g,
        CAST((IFNULL(sessionattempts5g,0) - IFNULL(sessionfailures5g,0)) AS DOUBLE)
            / NULLIF(CAST(IFNULL(sessionattempts5g,0) AS DOUBLE), 0) AS data_establishment_5g,
        CAST((IFNULL(sessionattempts4g,0) + IFNULL(sessionattempts3g,0) + IFNULL(sessionattempts5g,0)) - (IFNULL(sessionfailures5g,0) + IFNULL(sessionfailures4g,0) + IFNULL(sessionfailures3g,0)) AS DOUBLE)
            / NULLIF(CAST((IFNULL(sessionattempts5g,0) + IFNULL(sessionattempts4g,0) + IFNULL(sessionattempts3g,0)) AS DOUBLE), 0) AS data_establishment,
        CAST(IFNULL(sessionattempts3g,0) - IFNULL(sessionfailures3g,0) - IFNULL(sessiondropped3g,0) AS DOUBLE)
            / NULLIF(CAST(IFNULL(sessionattempts3g,0) - IFNULL(sessionfailures3g,0) AS DOUBLE), 0) AS data_retention_3g,
        CAST(IFNULL(sessionattempts4g,0) - IFNULL(sessionfailures4g,0) - IFNULL(sessiondropped4g,0) AS DOUBLE)
            / NULLIF(CAST(IFNULL(sessionattempts4g,0) - IFNULL(sessionfailures4g,0) AS DOUBLE), 0) AS data_retention_4g,
        CAST(IFNULL(sessionattempts5g,0) - IFNULL(sessionfailures5g,0) - IFNULL(sessiondropped5g,0) AS DOUBLE)
            / NULLIF(CAST(IFNULL(sessionattempts5g,0) - IFNULL(sessionfailures5g,0) AS DOUBLE), 0) AS data_retention_5g,
        CAST((IFNULL(sessionattempts4g,0) + IFNULL(sessionattempts3g,0) + IFNULL(sessionattempts5g,0) - (IFNULL(sessionfailures4g,0) + IFNULL(sessionfailures3g,0) + IFNULL(sessionfailures5g,0)) -
             (IFNULL(sessiondropped4g,0) + IFNULL(sessiondropped3g,0) + IFNULL(sessiondropped5g,0))) AS DOUBLE)
            / NULLIF(CAST((IFNULL(sessionattempts4g,0) + IFNULL(sessionattempts3g,0) + IFNULL(sessionattempts5g,0) - (IFNULL(sessionfailures4g,0) + IFNULL(sessionfailures3g,0) + IFNULL(sessionfailures5g,0))) AS DOUBLE), 0) AS data_retention,
        datathroughputscore3g,
        datathroughputscore4g,
        datathroughputscore5g,
        datathroughputscore,
        dataavgscore3g,
        dataavgscore4g,
        dataavgscore5g,
        dataavgscore,
        CAST(IFNULL(mosmssuccess3g,0) AS DOUBLE)
            / NULLIF(CAST(IFNULL(mosmsattempts3g,0) AS DOUBLE), 0) AS mosmsoverall3g,
        CAST(IFNULL(mosmssuccessvolte,0) AS DOUBLE)
            / NULLIF(CAST(IFNULL(mosmsattemptsvolte,0) AS DOUBLE), 0) AS mosmsoverallvolte,
        CAST(IFNULL(mosmssuccess5g,0) AS DOUBLE)
            / NULLIF(CAST(IFNULL(mosmsattempts5g,0) AS DOUBLE), 0) AS mosmsoverall5g,
        CAST((IFNULL(mosmssuccess5g,0) + IFNULL(mosmssuccess3g,0) + IFNULL(mosmssuccessvolte,0)) AS DOUBLE)
            / NULLIF(CAST((IFNULL(mosmsattempts5g,0) + IFNULL(mosmsattemptsvolte,0) + IFNULL(mosmsattempts3g,0)) AS DOUBLE), 0) AS mosmsoverall,
        CAST(IFNULL(mosmslatencyunderthreshold3g,0) AS DOUBLE)
            / NULLIF(CAST(IFNULL(mosmssuccess3g,0) AS DOUBLE), 0) AS mosmslatencyunderthreshold3g,
        CAST(IFNULL(mosmslatencyunderthresholdvolte,0) AS DOUBLE)
            / NULLIF(CAST(IFNULL(mosmssuccessvolte,0) AS DOUBLE), 0) AS mosmslatencyunderthresholdvolte,
        CAST(IFNULL(mosmslatencyunderthreshold5g,0) AS DOUBLE)
            / NULLIF(CAST(IFNULL(mosmssuccess5g,0) AS DOUBLE), 0) AS mosmslatencyunderthreshold5g,
        -- Combined mosmslatencyunderthreshold: use ifNull to avoid NULL in sum
        ((CAST(IFNULL(mosmslatencyunderthreshold3g,0) AS DOUBLE)
            / NULLIF(CAST(IFNULL(mosmssuccess3g,0) AS DOUBLE), 0))
         + (CAST(IFNULL(mosmslatencyunderthresholdvolte,0) AS DOUBLE)
            / NULLIF(CAST(IFNULL(mosmssuccessvolte,0) AS DOUBLE), 0))
         + (CAST(IFNULL(mosmslatencyunderthreshold5g,0) AS DOUBLE)
            / NULLIF(CAST(IFNULL(mosmssuccess5g,0) AS DOUBLE), 0))) AS mosmslatencyunderthreshold,
        mosmsavgscore3g,
        mosmsavgscorevolte,
        mosmsavgscore5g,
        mosmsavgscore
FROM trip_metrics_closed
    WHERE roamertype = 2 AND tripstatus IN (2)  and bintime=1756598400 and triptype='N' and partnercountryid<>0 and partnernetworkid<>0
)
SELECT
     partnercountryid,
    partnernetworkid,
    tripstatus,
    triptype,
    -- Ratios
    SUM(nerratio3g) AS f_nerratio3g,
    SUM(nerratiovolte) AS f_nerratiovolte,
    SUM(nerratio5g) AS f_nerratio5g,
    SUM(nerratio) AS f_nerratio,
    SUM(ccrratio3g) AS f_ccrratio3g,
    SUM(ccrratiovolte) AS f_ccrratiovolte,
    SUM(ccrratio) AS f_ccrratio,
    SUM(totalpddunderthreshold3g) AS f_totalpddunderthreshold3g,
    SUM(totalpddunderthresholdvolte) AS f_totalpddunderthresholdvolte,
    SUM(totalpddunderthreshold5g) AS f_totalpddunderthreshold5g,
    SUM(totalpddunderthreshold) AS f_totalpddunderthreshold,
    SUM(data_establishment_3g) AS f_data_establishment_3g,
    SUM(data_establishment_4g) AS f_data_establishment_4g,
    SUM(data_establishment_5g) AS f_data_establishment_5g,
    SUM(data_establishment) AS f_data_establishment,
    SUM(data_retention_3g) AS f_data_retention_3g,
    SUM(data_retention_4g) AS f_data_retention_4g,
    SUM(data_retention_5g) AS f_data_retention_5g,
    SUM(data_retention) AS f_data_retention,
    SUM(mosmsoverall3g) AS f_mosmsoverall3g,
    SUM(mosmsoverallvolte) AS f_mosmsoverallvolte,
    SUM(mosmsoverall5g) AS f_mosmsoverall5g,
    SUM(mosmsoverall) AS f_mosmsoverall,
    SUM(mosmslatencyunderthreshold3g) AS f_mosmslatencyunderthreshold3g,
    SUM(mosmslatencyunderthresholdvolte) AS f_mosmslatencyunderthresholdvolte,
    SUM(mosmslatencyunderthreshold5g) AS f_mosmslatencyunderthreshold5g,
    SUM(mosmslatencyunderthreshold) AS f_mosmslatencyunderthreshold,
    -- Scores
    SUM(overallavgscore) AS f_overallavgscore,
    SUM(overallavgscoreexcludingsor) AS f_overallavgscoreexcludingsor,
    SUM(regavgscore) AS f_regavgscore,
    SUM(regsuccessratioscore) AS f_regsuccessratioscore,
    SUM(reglatencyscore) AS f_reglatencyscore,
    SUM(regavgscoreexcludingsor) AS f_regavgscoreexcludingsor,
    SUM(regsuccessratioscoreexcludingsor) AS f_regsuccessratioscoreexcludingsor,
    SUM(reglatencyscoreexcludingsor) AS f_reglatencyscoreexcludingsor,
    SUM(mosmsavgscore3g) AS f_mosmsavgscore3g,
    SUM(mosmsavgscorevolte) AS f_mosmsavgscorevolte,
    SUM(mosmsavgscore5g) AS f_mosmsavgscore5g,
    SUM(mosmsavgscore) AS f_mosmsavgscore,
    SUM(datathroughputscore3g) AS f_datathroughputscore3g,
    SUM(datathroughputscore4g) AS f_datathroughputscore4g,
    SUM(datathroughputscore5g) AS f_datathroughputscore5g,
    SUM(datathroughputscore) AS f_datathroughputscore,
    SUM(dataavgscore3g) AS f_dataavgscore3g,
    SUM(dataavgscore4g) AS f_dataavgscore4g,
    SUM(dataavgscore5g) AS f_datavgscore5g,
    SUM(dataavgscore) AS f_datavgscore,
    SUM(callavgscore3g) AS f_callavgscore3g,
    SUM(callavgscorevolte) AS f_callavgscorevolte,
    SUM(callavgscore5g) AS f_callavgscore5g,
    SUM(callavgscore) AS f_callavgscore
FROM tripmetrics_numerator
GROUP BY partnercountryid, partnernetworkid, tripstatus,triptype order by partnercountryid, partnernetworkid, tripstatus asc

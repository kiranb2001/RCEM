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
        CAST((mocallsuccess3g + mtcallsuccess3g + mocallsuccessvolte + mtcallsuccessvolte + mocallsuccess5g + mtcallsuccess5g) AS DOUBLE)
            / NULLIF(CAST((mocallattemptsvolte + mtcallattemptsvolte + mocallattempts3g + mtcallattempts3g + mocallattempts5g + mtcallattempts5g) AS DOUBLE), 0) AS nerratio,
        CAST((mocallanswered3g + mtcallanswered3g) - (mocalldrop3g + mtcalldrop3g) AS DOUBLE)
            / NULLIF(CAST((mocallanswered3g + mtcallanswered3g) AS DOUBLE), 0) AS ccrratio3g,
        CAST((mocallansweredvolte + mtcallansweredvolte) - (mocalldropvolte + mtcalldropvolte) AS DOUBLE)
            / NULLIF(CAST((mocallansweredvolte + mtcallansweredvolte) AS DOUBLE), 0) AS ccrratiovolte,
        CAST(((mocallanswered5g + mtcallanswered5g + mocallansweredvolte + mtcallansweredvolte + mocallanswered3g + mtcallanswered3g) -
             (mocalldrop5g + mtcalldrop5g + mocalldropvolte + mtcalldropvolte + mocalldrop3g + mtcalldrop3g)) AS DOUBLE)
            / NULLIF(CAST((mocallanswered5g + mtcallanswered5g + mocallansweredvolte + mtcallansweredvolte + mocallanswered3g + mtcallanswered3g) AS DOUBLE), 0) AS ccrratio,
        mtcallpddunderthreshold3g + mocallpddunderthreshold3g AS totalpddunderthreshold3g,
        mtcallpddunderthresholdvolte + mocallpddunderthresholdvolte AS totalpddunderthresholdvolte,
        mtcallpddunderthreshold5g + mocallpddunderthreshold5g AS totalpddunderthreshold5g,
        (mtcallpddunderthreshold3g + mocallpddunderthreshold3g + mtcallpddunderthresholdvolte + mocallpddunderthresholdvolte + mtcallpddunderthreshold5g + mocallpddunderthreshold5g) AS totalpddunderthreshold,
        callavgscore3g,
        callavgscorevolte,
        callavgscore5g,
        callavgscore,
        CAST((sessionattempts3g - sessionfailures3g) AS DOUBLE)
            / NULLIF(CAST(sessionattempts3g AS DOUBLE), 0) AS data_establishment_3g,
        CAST((sessionattempts4g - sessionfailures4g) AS DOUBLE)
            / NULLIF(CAST(sessionattempts4g AS DOUBLE), 0) AS data_establishment_4g,
        CAST((sessionattempts5g - sessionfailures5g) AS DOUBLE)
            / NULLIF(CAST(sessionattempts5g AS DOUBLE), 0) AS data_establishment_5g,
        CAST((sessionattempts4g + sessionattempts3g + sessionattempts5g - (sessionfailures5g + sessionfailures4g + sessionfailures3g)) AS DOUBLE)
            / NULLIF(CAST((sessionattempts5g + sessionattempts4g + sessionattempts3g) AS DOUBLE), 0) AS data_establishment,
        CAST(sessionattempts3g - sessionfailures3g - sessiondropped3g AS DOUBLE)
            / NULLIF(CAST(sessionattempts3g - sessionfailures3g AS DOUBLE), 0) AS data_retention_3g,
        CAST(sessionattempts4g - sessionfailures4g - sessiondropped4g AS DOUBLE)
            / NULLIF(CAST(sessionattempts4g - sessionfailures4g AS DOUBLE), 0) AS data_retention_4g,
        CAST(sessionattempts5g - sessionfailures5g - sessiondropped5g AS DOUBLE)
            / NULLIF(CAST(sessionattempts5g - sessionfailures5g AS DOUBLE), 0) AS data_retention_5g,
        CAST((sessionattempts4g + sessionattempts3g + sessionattempts5g - (sessionfailures4g + sessionfailures3g + sessionfailures5g) -
             (sessiondropped4g + sessiondropped3g + sessiondropped5g)) AS DOUBLE)
            / NULLIF(CAST((sessionattempts4g + sessionattempts3g + sessionattempts5g - (sessionfailures4g + sessionfailures3g + sessionfailures5g)) AS DOUBLE), 0) AS data_retention,
        datathroughputscore3g,
        datathroughputscore4g,
        datathroughputscore5g,
        datathroughputscore,
        dataavgscore3g,
        dataavgscore4g,
        dataavgscore5g,
        dataavgscore,
        CAST(mosmssuccess3g AS DOUBLE)
            / NULLIF(CAST(mosmsattempts3g AS DOUBLE), 0) AS mosmsoverall3g,
        CAST(mosmssuccessvolte AS DOUBLE)
            / NULLIF(CAST(mosmsattemptsvolte AS DOUBLE), 0) AS mosmsoverallvolte,
        CAST(mosmssuccess5g AS DOUBLE)
            / NULLIF(CAST(mosmsattempts5g AS DOUBLE), 0) AS mosmsoverall5g,
        CAST((mosmssuccess5g + mosmssuccess3g + mosmssuccessvolte) AS DOUBLE)
            / NULLIF(CAST((mosmsattempts5g + mosmsattemptsvolte + mosmsattempts3g) AS DOUBLE), 0) AS mosmsoverall,
        CAST(mosmslatencyunderthreshold3g AS DOUBLE)
            / NULLIF(CAST(mosmssuccess3g AS DOUBLE), 0) AS mosmslatencyunderthreshold3g,
        CAST(mosmslatencyunderthresholdvolte AS DOUBLE)
            / NULLIF(CAST(mosmssuccessvolte AS DOUBLE), 0) AS mosmslatencyunderthresholdvolte,
        CAST(mosmslatencyunderthreshold5g AS DOUBLE)
            / NULLIF(CAST(mosmssuccess5g AS DOUBLE), 0) AS mosmslatencyunderthreshold5g,
        ((CAST(mosmslatencyunderthreshold3g AS DOUBLE)
            / NULLIF(CAST(mosmssuccess3g AS DOUBLE), 0))
         + (CAST(mosmslatencyunderthresholdvolte AS DOUBLE)
            / NULLIF(CAST(mosmssuccessvolte AS DOUBLE), 0))
         + (CAST(mosmslatencyunderthreshold5g AS DOUBLE)
            / NULLIF(CAST(mosmssuccess5g AS DOUBLE), 0))) AS mosmslatencyunderthreshold,
        mosmsavgscore3g,
        mosmsavgscorevolte,
        mosmsavgscore5g,
        mosmsavgscore
    FROM trip_metrics_closed
    WHERE roamertype = 2
      AND tripstatus IN (2)
      AND triptype NOT IN ('G','C')
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
GROUP BY partnercountryid, partnernetworkid, tripstatus, triptype
ORDER BY partnercountryid, partnernetworkid, tripstatus ASC;

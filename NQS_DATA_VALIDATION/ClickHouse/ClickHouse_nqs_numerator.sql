WITH labeled_score_daily_data AS (
    SELECT
        partnercountryid,
        partnernetworkid,
        tripstatus,
        CASE
            WHEN serviceid = 0 AND eventtype = -1 AND rat = -1 AND ingredientid = -1 THEN 'overallscoresordisabled'
            WHEN serviceid = 5 AND eventtype = -1 AND rat = -1 AND ingredientid = -1 THEN 'overallscoresorenabled'
            WHEN serviceid = 1 AND eventtype = -1 AND rat = -1 AND ingredientid = -1 THEN 'registrationscoresordisabled'
            WHEN serviceid = 6 AND eventtype = -1 AND rat = -1 AND ingredientid = -1 THEN 'registrationscoresorenabled'
            WHEN serviceid = 1 AND eventtype = -1 AND rat = -1 AND ingredientid = 1 THEN 'registrationsuccessratioscoresordisabled'
            WHEN serviceid = 6 AND eventtype = -1 AND rat = -1 AND ingredientid = 1 THEN 'registrationsuccessratioscoresorenabled'
            WHEN serviceid = 1 AND eventtype = -1 AND rat = -1 AND ingredientid = 0 THEN 'registrationdelayratioscoresordisabled'
            WHEN serviceid = 6 AND eventtype = -1 AND rat = -1 AND ingredientid = 0 THEN 'registrationdelayratioscoresorenabled'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = -1 AND ingredientid = -1 THEN 'voice'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = 1 AND ingredientid = -1 THEN 'voice3g'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = 6 AND ingredientid = -1 THEN 'voice4g'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = 10 AND ingredientid = -1 THEN 'voice5g'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = -1 AND ingredientid = 2 THEN 'voiceNER'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = 1 AND ingredientid = 2 THEN 'voiceNER3g'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = 6 AND ingredientid = 2 THEN 'voiceNER4g'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = 10 AND ingredientid = 2 THEN 'voiceNER5g'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = -1 AND ingredientid = 4 THEN 'voiceCCR'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = 1 AND ingredientid = 4 THEN 'voiceCCR3g'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = 6 AND ingredientid = 4 THEN 'voiceCCR4g'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = 10 AND ingredientid = 4 THEN 'voiceCCR5g'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = -1 AND ingredientid = 3 THEN 'voicePDD'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = 1 AND ingredientid = 3 THEN 'voicePDD3g'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = 6 AND ingredientid = 3 THEN 'voicePDD4g'
            WHEN serviceid = 3 AND eventtype = -1 AND rat = 10 AND ingredientid = 3 THEN 'voicePDD5g'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = -1 AND ingredientid = -1 THEN 'data'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = 1 AND ingredientid = -1 THEN 'data3g'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = 6 AND ingredientid = -1 THEN 'data4g'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = 10 AND ingredientid = -1 THEN 'data5g'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = -1 AND ingredientid = 7 THEN 'dataEstablishment'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = 1 AND ingredientid = 7 THEN 'dataEstablishment3g'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = 6 AND ingredientid = 7 THEN 'dataEstablishment4g'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = 10 AND ingredientid = 7 THEN 'dataEstablishment5g'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = -1 AND ingredientid = 9 THEN 'dataThroughput'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = 1 AND ingredientid = 9 THEN 'dataThroughput3g'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = 6 AND ingredientid = 9 THEN 'dataThroughput4g'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = 10 AND ingredientid = 9 THEN 'dataThroughput5g'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = -1 AND ingredientid = 8 THEN 'dataRetention'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = 1 AND ingredientid = 8 THEN 'dataRetention3g'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = 6 AND ingredientid = 8 THEN 'dataRetention4g'
            WHEN serviceid = 4 AND eventtype = -1 AND rat = 10 AND ingredientid = 8 THEN 'dataRetention5g'
            WHEN serviceid = 2 AND eventtype = 0 AND rat = -1 AND ingredientid = -1 THEN 'mosms'
            WHEN serviceid = 2 AND eventtype = 0 AND rat = -1 AND ingredientid = 5 THEN 'moSMSSuccessRatio'
            WHEN serviceid = 2 AND eventtype = 0 AND rat = -1 AND ingredientid = 6 THEN 'moSMSDeliveryTime'
            WHEN serviceid = 2 AND eventtype = 0 AND rat = 1 AND ingredientid = -1 THEN 'mosms3g'
            WHEN serviceid = 2 AND eventtype = 0 AND rat = 1 AND ingredientid = 5 THEN 'moSMSSuccessRatio3g'
            WHEN serviceid = 2 AND eventtype = 0 AND rat = 1 AND ingredientid = 6 THEN 'moSMSDeliveryTime3g'
            WHEN serviceid = 2 AND eventtype = 0 AND rat = 6 AND ingredientid = -1 THEN 'mosmsvolte'
            WHEN serviceid = 2 AND eventtype = 0 AND rat = 6 AND ingredientid = 5 THEN 'moSMSSuccessRatiovolte'
            WHEN serviceid = 2 AND eventtype = 0 AND rat = 6 AND ingredientid = 6 THEN 'moSMSDeliveryTimevolte'
            WHEN serviceid = 2 AND eventtype = 0 AND rat = 10 AND ingredientid = -1 THEN 'mosms5g'
            WHEN serviceid = 2 AND eventtype = 0 AND rat = 10 AND ingredientid = 5 THEN 'moSMSSuccessRatio5g'
            WHEN serviceid = 2 AND eventtype = 0 AND rat = 10 AND ingredientid = 6 THEN 'moSMSDeliveryTime5g'
            ELSE 'others'
        END AS service_label,
        sumMerge(sum_score_numerator) AS nqs_numerator_count
    FROM rcem_nqs_dly
    WHERE tripstatus = 2
      AND partnercountryid <> 0
      AND partnernetworkid <> 0
    GROUP BY partnercountryid, partnernetworkid, serviceid, eventtype, rat, ingredientid, tripstatus
)

SELECT
    partnercountryid,
    partnernetworkid,
    tripstatus,

    SUM(CASE WHEN service_label = 'overallscoresordisabled' THEN nqs_numerator_count ELSE 0 END) AS overallscoresordisabled_num,
    SUM(CASE WHEN service_label = 'overallscoresorenabled' THEN nqs_numerator_count ELSE 0 END) AS overallscoresorenabled_num,

    SUM(CASE WHEN service_label = 'registrationscoresordisabled' THEN nqs_numerator_count ELSE 0 END) AS registrationscoresordisabled_num,
    SUM(CASE WHEN service_label = 'registrationscoresorenabled' THEN nqs_numerator_count ELSE 0 END) AS registrationscoresorenabled_num,

    SUM(CASE WHEN service_label = 'registrationsuccessratioscoresordisabled' THEN nqs_numerator_count ELSE 0 END) AS registrationsuccessratioscoresordisabled_num,
    SUM(CASE WHEN service_label = 'registrationsuccessratioscoresorenabled' THEN nqs_numerator_count ELSE 0 END) AS registrationsuccessratioscoresorenabled_num,

    SUM(CASE WHEN service_label = 'registrationdelayratioscoresordisabled' THEN nqs_numerator_count ELSE 0 END) AS registrationdelayratioscoresordisabled_num,
    SUM(CASE WHEN service_label = 'registrationdelayratioscoresorenabled' THEN nqs_numerator_count ELSE 0 END) AS registrationdelayratioscoresorenabled_num,

    SUM(CASE WHEN service_label = 'voice' THEN nqs_numerator_count ELSE 0 END) AS voice_num,
    SUM(CASE WHEN service_label = 'voice3g' THEN nqs_numerator_count ELSE 0 END) AS voice3g_num,
    SUM(CASE WHEN service_label = 'voice4g' THEN nqs_numerator_count ELSE 0 END) AS voice4g_num,
    SUM(CASE WHEN service_label = 'voice5g' THEN nqs_numerator_count ELSE 0 END) AS voice5g_num,

    SUM(CASE WHEN service_label = 'voiceNER' THEN nqs_numerator_count ELSE 0 END) AS voiceNER_num,
    SUM(CASE WHEN service_label = 'voiceNER3g' THEN nqs_numerator_count ELSE 0 END) AS voiceNER3g_num,
    SUM(CASE WHEN service_label = 'voiceNER4g' THEN nqs_numerator_count ELSE 0 END) AS voiceNER4g_num,
    SUM(CASE WHEN service_label = 'voiceNER5g' THEN nqs_numerator_count ELSE 0 END) AS voiceNER5g_num,

    SUM(CASE WHEN service_label = 'voiceCCR' THEN nqs_numerator_count ELSE 0 END) AS voiceCCR_num,
    SUM(CASE WHEN service_label = 'voiceCCR3g' THEN nqs_numerator_count ELSE 0 END) AS voiceCCR3g_num,
    SUM(CASE WHEN service_label = 'voiceCCR4g' THEN nqs_numerator_count ELSE 0 END) AS voiceCCR4g_num,
    SUM(CASE WHEN service_label = 'voiceCCR5g' THEN nqs_numerator_count ELSE 0 END) AS voiceCCR5g_num,

    SUM(CASE WHEN service_label = 'voicePDD' THEN nqs_numerator_count ELSE 0 END) AS voicePDD_num,
    SUM(CASE WHEN service_label = 'voicePDD3g' THEN nqs_numerator_count ELSE 0 END) AS voicePDD3g_num,
    SUM(CASE WHEN service_label = 'voicePDD4g' THEN nqs_numerator_count ELSE 0 END) AS voicePDD4g_num,
    SUM(CASE WHEN service_label = 'voicePDD5g' THEN nqs_numerator_count ELSE 0 END) AS voicePDD5g_num,

    SUM(CASE WHEN service_label = 'data' THEN nqs_numerator_count ELSE 0 END) AS data_num,
    SUM(CASE WHEN service_label = 'data3g' THEN nqs_numerator_count ELSE 0 END) AS data3g_num,
    SUM(CASE WHEN service_label = 'data4g' THEN nqs_numerator_count ELSE 0 END) AS data4g_num,
    SUM(CASE WHEN service_label = 'data5g' THEN nqs_numerator_count ELSE 0 END) AS data5g_num,

    SUM(CASE WHEN service_label = 'dataEstablishment' THEN nqs_numerator_count ELSE 0 END) AS dataEstablishment_num,
    SUM(CASE WHEN service_label = 'dataEstablishment3g' THEN nqs_numerator_count ELSE 0 END) AS dataEstablishment3g_num,
    SUM(CASE WHEN service_label = 'dataEstablishment4g' THEN nqs_numerator_count ELSE 0 END) AS dataEstablishment4g_num,
    SUM(CASE WHEN service_label = 'dataEstablishment5g' THEN nqs_numerator_count ELSE 0 END) AS dataEstablishment5g_num,

    SUM(CASE WHEN service_label = 'dataThroughput' THEN nqs_numerator_count ELSE 0 END) AS dataThroughput_num,
    SUM(CASE WHEN service_label = 'dataThroughput3g' THEN nqs_numerator_count ELSE 0 END) AS dataThroughput3g_num,
    SUM(CASE WHEN service_label = 'dataThroughput4g' THEN nqs_numerator_count ELSE 0 END) AS dataThroughput4g_num,
    SUM(CASE WHEN service_label = 'dataThroughput5g' THEN nqs_numerator_count ELSE 0 END) AS dataThroughput5g_num,

    SUM(CASE WHEN service_label = 'dataRetention' THEN nqs_numerator_count ELSE 0 END) AS dataRetention_num,
    SUM(CASE WHEN service_label = 'dataRetention3g' THEN nqs_numerator_count ELSE 0 END) AS dataRetention3g_num,
    SUM(CASE WHEN service_label = 'dataRetention4g' THEN nqs_numerator_count ELSE 0 END) AS dataRetention4g_num,
    SUM(CASE WHEN service_label = 'dataRetention5g' THEN nqs_numerator_count ELSE 0 END) AS dataRetention5g_num,

    SUM(CASE WHEN service_label = 'mosms' THEN nqs_numerator_count ELSE 0 END) AS mosms_num,
    SUM(CASE WHEN service_label = 'moSMSSuccessRatio' THEN nqs_numerator_count ELSE 0 END) AS moSMSSuccessRatio_num,
    SUM(CASE WHEN service_label = 'moSMSDeliveryTime' THEN nqs_numerator_count ELSE 0 END) AS moSMSDeliveryTime_num,
    SUM(CASE WHEN service_label = 'mosms3g' THEN nqs_numerator_count ELSE 0 END) AS mosms3g_num,
    SUM(CASE WHEN service_label = 'moSMSSuccessRatio3g' THEN nqs_numerator_count ELSE 0 END) AS moSMSSuccessRatio3g_num,
    SUM(CASE WHEN service_label = 'moSMSDeliveryTime3g' THEN nqs_numerator_count ELSE 0 END) AS moSMSDeliveryTime3g_num,
    SUM(CASE WHEN service_label = 'mosmsvolte' THEN nqs_numerator_count ELSE 0 END) AS mosmsvolte_num,
    SUM(CASE WHEN service_label = 'moSMSSuccessRatiovolte' THEN nqs_numerator_count ELSE 0 END) AS moSMSSuccessRatiovolte_num,
    SUM(CASE WHEN service_label = 'moSMSDeliveryTimevolte' THEN nqs_numerator_count ELSE 0 END) AS moSMSDeliveryTimevolte_num,
    SUM(CASE WHEN service_label = 'mosms5g' THEN nqs_numerator_count ELSE 0 END) AS mosms5g_num,
    SUM(CASE WHEN service_label = 'moSMSSuccessRatio5g' THEN nqs_numerator_count ELSE 0 END) AS moSMSSuccessRatio5g_num,
    SUM(CASE WHEN service_label = 'moSMSDeliveryTime5g' THEN nqs_numerator_count ELSE 0 END) AS moSMSDeliveryTime5g_num

FROM labeled_score_daily_data where service_label != 'others'
GROUP BY partnercountryid, partnernetworkid,tripstatus
ORDER BY partnercountryid, partnernetworkid,tripstatus

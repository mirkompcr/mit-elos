
/*
WITH
  cabg_adm AS (
    SELECT DISTINCT hadm_id AS cabg_id
    FROM `physionet-data.mimiciv_hosp.drgcodes`
    WHERE drg_code IN ("231", "232" ,"233" , "234" ,"235" ,"236")
)
, cirrhosis AS (
    SELECT DISTINCT hadm_id AS cirrhosis_id
    FROM `physionet-data.mimiciv_hosp.diagnoses_icd`
    WHERE icd_code IN ("K7030","K7031", "K717", "K743", "K744", "K745", "K7460", "K7469","5712", "5715", "5716")
)
, esrd AS (
    SELECT DISTINCT hadm_id AS ESRD_id
    FROM `physionet-data.mimiciv_hosp.diagnoses_icd`
    WHERE icd_code IN ("N186", "5856")
)
, maxhr AS (
    SELECT stay_id,
           max(hr) AS max_hr
    FROM `physionet-data.mimiciv_derived.sofa`
    GROUP BY stay_id
)
, sofa24 AS (
    SELECT stay_id,
           respiration_24hours as resp_24,
           coagulation_24hours as coag_24,
           liver_24hours as liver_24,
           cardiovascular_24hours as cv_24,
           cns_24hours as cns_24,
           renal_24hours as renal_24
    FROM `physionet-data.mimiciv_derived.sofa`
    WHERE hr = 24
)
, sofa168 AS (
    SELECT stay_id,
           respiration_24hours AS resp_168,
           coagulation_24hours AS coag_168,
           liver_24hours AS liver_168,
           cardiovascular_24hours AS cv_168,
           cns_24hours AS cns_168,
           renal_24hours AS renal_168
    FROM `physionet-data.mimiciv_derived.sofa`
    WHERE hr = 168
)
, sofalast AS (
    SELECT s1.stay_id,
           maxhr.max_hr,
           respiration_24hours AS resp_last,
           coagulation_24hours AS coag_last,
           liver_24hours AS liver_last,
           cardiovascular_24hours AS cv_last,
           cns_24hours AS cns_last,
           renal_24hours AS renal_last
    FROM `physionet-data.mimiciv_derived.sofa` AS s1
    RIGHT JOIN maxhr
    ON s1.hr=maxhr.max_hr
    AND s1.stay_id = maxhr.stay_id
)
, vent24 AS (
    SELECT s.stay_id,
           v.ventilation_status AS vent_24
    FROM (
        SELECT *
        FROM `physionet-data.mimiciv_derived.sofa`
        WHERE hr=24
    )  AS s
    INNER JOIN (
        SELECT *
        FROM `physionet-data.mimiciv_derived.ventilation`
        WHERE ventilation_status != "None"
    ) AS v
    ON s.stay_id = v.stay_id
    AND s.starttime >= DATETIME_TRUNC(v.starttime, HOUR)
    AND s.endtime <= DATETIME_TRUNC(v.endtime, HOUR)
)
, vent168 AS (
    SELECT s.stay_id,
           v.ventilation_status AS vent_168
    FROM (
        SELECT *
        FROM `physionet-data.mimiciv_derived.sofa`
        WHERE hr = 168
    ) AS s
    INNER JOIN (
        SELECT *
        FROM `physionet-data.mimiciv_derived.ventilation`
        WHERE ventilation_status != "None"
    ) AS v
    ON s.stay_id = v.stay_id
    AND s.starttime >= DATETIME_TRUNC(v.starttime, HOUR)
    AND s.endtime<=DATETIME_TRUNC(v.endtime, HOUR)
)
, ventlast AS (
    SELECT v.stay_id,
           v.ventilation_status AS vent_last
    FROM (
        SELECT sf.stay_id,
               sf.starttime,
               sf.endtime,
               sf.hr
        FROM `physionet-data.mimiciv_derived.sofa` AS sf 
        INNER JOIN maxhr
        ON maxhr.stay_id = sf.stay_id
        AND maxhr.max_hr = sf.hr + 24
    ) AS s
    LEFT JOIN (
        SELECT *
        FROM `physionet-data.mimiciv_derived.ventilation`
        WHERE ventilation_status = "InvasiveVent"
    ) AS v 
    ON s.stay_id = v.stay_id
    AND s.starttime <= v.endtime
)

-- Add admission type
-- Mapping: 
-- Emergency: ‘AMBULATORY OBSERVATION’, ‘DIRECT EMER.’, ‘URGENT’, ‘EW EMER.’, ‘DIRECT OBSERVATION’, ‘EU OBSERVATION’, ‘OBSERVATION ADMIT’
-- Elective: ‘ELECTIVE’, ‘SURGICAL SAME DAY ADMISSION’

, adm AS (
    SELECT hadm_id,
           deathtime,
           admission_type,
            CASE
            WHEN (admission_type LIKE "%ELECTIVE%" OR
            admission_type LIKE "%SURGICAL SAME DAY ADMISSION%") 
            THEN 1
            ELSE 0
            END AS adm_elective,
           admission_location,
           discharge_location
    FROM `physionet-data.mimiciv_hosp.admissions`
)

, charlson AS (
    SELECT hadm_id, charlson_comorbidity_index as charlson
    FROM `physionet-data.mimiciv_derived.charlson`
)

, first_service AS (
    SELECT hadm_id, curr_service AS first_service
        FROM (
        SELECT subject_id, hadm_id, curr_service, transfertime, 
        ROW_NUMBER() OVER(PARTITION BY subject_id, hadm_id ORDER BY transfertime ASC) AS service_seq
        FROM `physionet-data.mimiciv_hosp.services`
        )
    WHERE service_seq = 1
    )

SELECT DISTINCT *
FROM  `physionet-data.mimiciv_derived.icustay_detail` AS cohort 

LEFT JOIN sofa24
ON cohort.stay_id = sofa24.stay_id 

LEFT JOIN sofa168
ON cohort.stay_id = sofa168.stay_id

LEFT JOIN sofalast
ON cohort.stay_id = sofalast.stay_id

LEFT JOIN vent24
ON cohort.stay_id = vent24.stay_id

LEFT JOIN vent168
ON cohort.stay_id = vent168.stay_id

LEFT JOIN ventlast
ON ventlast.stay_id = cohort.stay_id

LEFT JOIN adm
ON cohort.hadm_id = adm.hadm_id

LEFT JOIN physionet-data.mimiciv_derived.sepsis3
AS s3
ON cohort.stay_id = s3.stay_id

LEFT JOIN cabg_adm
ON cohort.hadm_id = cabg_adm.cabg_id

LEFT JOIN cirrhosis
ON cohort.hadm_id = cirrhosis.cirrhosis_id

LEFT JOIN esrd
ON cohort.hadm_id = esrd.esrd_id

LEFT JOIN charlson
on cohort.hadm_id = charlson.hadm_id 

LEFT JOIN first_service
on cohort.hadm_id = first_service.hadm_id 

-- Key Commorbidities
LEFT JOIN(
  SELECT *
  FROM `db_name.my_MIMIC.pivoted_comorbidities`
)
AS comms
ON cohort.hadm_id = comms.hadm_id

-- Full code vs. DNI/NDR
LEFT JOIN(
  SELECT *
  FROM `db_name.my_MIMIC.pivoted_codes`
)
AS codes
ON cohort.stay_id = codes.stay_id 
*/


with cohort as (
  select icu.*,
    sofa_maxday.sofa_maxday, 
    TIMESTAMP_DIFF(icu.dod, icu_outtime, DAY) as Day_death2icuout,
    TIMESTAMP_DIFF(icu.dod,icu_intime, DAY) as Day_death2icuin,
    pat.gender, pat.anchor_age, pat.dod, 
    adm.language, adm.race,
    adm.deathtime, adm.discharge_location, adm.admission_location, adm.admission_type,
    sepsis.sepsis3,
    oasis.oasis,
    sofa24.sofa_adm, sofa24.cns_adm,
    sofa24.liver_adm, sofa24.resp_adm,
    sofa24.coag_adm, sofa24.cv_adm, sofa24.renal_adm,
    wt.weight_admit, ht.height
    from `physionet-data.mimiciv_derived.icustay_detail` as icu 

        left join `physionet-data.mimiciv_hosp.admissions` as adm
        on adm.HADM_ID = icu.HADM_ID

        left join (
            select stay_id, 
            sofa_24hours as sofa_adm,
            cns_24hours as cns_adm,
            liver_24hours as liver_adm, 
            respiration_24hours	as resp_adm, 
            coagulation_24hours as coag_adm,
            cardiovascular_24hours as cv_adm,
            renal_24hours as renal_adm
        from physionet-data.mimiciv_derived.sofa 
        where hr=24) 
        as sofa24
        on sofa24.stay_id = icu.stay_id

        left join (
            select stay_id, max(hr)/24 as sofa_maxday
        from physionet-data.mimiciv_derived.sofa group by stay_id)
        as sofa_maxday
        on sofa_maxday.stay_id = icu.stay_id

        left join `physionet-data.mimiciv_hosp.patients` as pat
        on pat.subject_id = icu.subject_id

        left join `physionet-data.mimiciv_derived.sepsis3` as sepsis
        on sepsis.stay_id = icu.stay_id

        left join `physionet-data.mimiciv_derived.oasis` as oasis
        on oasis.stay_id = icu.stay_id

        left join `physionet-data.mimiciv_derived.first_day_weight` as wt
        on wt.stay_id = icu.stay_id

        left join `physionet-data.mimiciv_derived.height` as ht
        on ht.stay_id= icu.stay_id

        where pat.dod is not null or adm.discharge_location ="HOSPICE"
)
    
select cohort.*, hr,respiration as resp,coagulation as coag, cardiovascular as cv, cns,renal,liver 
from physionet-data.mimiciv_derived.sofa as sofa
inner join cohort
on cohort.stay_id = sofa.stay_id
where (Day_death2icuin-sofa_maxday) <= 3
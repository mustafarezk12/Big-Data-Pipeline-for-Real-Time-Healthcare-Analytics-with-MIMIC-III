SELECT
  icu_admissions,
  COUNT(*) AS num_of_patients
FROM (
  SELECT subject_id, COUNT(icustay_id) AS icu_admissions
  FROM icustays
  GROUP BY subject_id
) t
GROUP BY icu_admissions
ORDER BY icu_admissions;
----------------------------
--Result
+-----------------+------------------+
| icu_admissions  | num_of_patients  |
+-----------------+------------------+
| 1               | 81               |
| 2               | 15               |
| 3               | 2                |
| 4               | 1                |
| 15              | 1                |
+-----------------+------------------+
5 rows selected (5.787 seconds)
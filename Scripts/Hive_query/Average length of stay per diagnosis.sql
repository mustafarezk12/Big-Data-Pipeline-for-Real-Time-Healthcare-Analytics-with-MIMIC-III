SELECT 
    d.icd9_code,
    COUNT(*) as total_cases,
    ROUND(AVG(
        (unix_timestamp(a.dischtime) - unix_timestamp(a.admittime)) / 86400
    ), 2) as avg_los_days
FROM diagnoses_icd d
JOIN admissions a 
    ON d.hadm_id = a.hadm_id AND d.subject_id = a.subject_id
WHERE (unix_timestamp(a.dischtime) - unix_timestamp(a.admittime)) >= 0
GROUP BY d.icd9_code
HAVING total_cases >= 5
ORDER BY avg_los_days DESC
LIMIT 10;
--------------------------------
--result
+--------------+--------------+---------------+
| d.icd9_code  | total_cases  | avg_los_days  |
+--------------+--------------+---------------+
| 5845         | 6            | 38.23         |
| 2767         | 5            | 34.09         |
| 45829        | 5            | 33.77         |
| 5180         | 6            | 29.55         |
| 5712         | 5            | 21.96         |
| 570          | 6            | 19.66         |
| 78959        | 5            | 19.38         |
| 70703        | 15           | 16.15         |
| 5185         | 5            | 15.87         |
| 51881        | 31           | 15.14         |
+--------------+--------------+---------------+
10 rows selected (20.889 seconds)
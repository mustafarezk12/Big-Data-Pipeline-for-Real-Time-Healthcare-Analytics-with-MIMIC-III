SELECT first_careunit, AVG(los) as avg_los
FROM icustays
GROUP BY first_careunit;

-------------------------
result:
+-----------------+---------------------+
| first_careunit  |       avg_los       |
+-----------------+---------------------+
| CCU             | 5.7539              |
| CSRU            | 3.6313499999999994  |
| MICU            | 3.9553454545454554  |
| SICU            | 5.668460869565218   |
| TSICU           | 3.589609090909091   |
+-----------------+---------------------+
5 rows selected (4.673 seconds)
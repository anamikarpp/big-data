SET hive.enfore.bucketing = true;

INSERT OVERWRITE TABLE emp_backeted
SELECT * FROM emp_basic;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO TABLE emp_paritioned_dynamic
PARTITION (dept)
SELECT id, name, dept FROM emp_basic;

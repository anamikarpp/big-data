LOAD DATA LOCAL INPATH '/datasets/emp_sales.csv'
INTO TABLE emp_partitioned_static PARTITION (dept='Sales');

LOAD DATA LOCAL INPATH '/datasets/emp_hr.csv'
INTO TABLE emp_paritioned_static PARTITION (dept='HR');

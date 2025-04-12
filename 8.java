-- Create a non-transactional table for the example.
CREATE TABLE IF NOT EXISTS employee_nt (
    empid INT,
    empname STRING,
    empcity STRING,
    salary BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Insert sample rows into the non-transactional table.
INSERT INTO TABLE employee_nt VALUES (101, 'Alice', 'New York', 100000);
INSERT INTO TABLE employee_nt VALUES (102, 'Bob', 'San Francisco', 120000);
INSERT INTO TABLE employee_nt VALUES (103, 'Carol', 'Chicago', 90000);

-- Create a backup table.
CREATE TABLE IF NOT EXISTS employee_backup_nt (
  empid INT,
  empname STRING,
  empcity STRING,
  salary BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Insert data into the backup table from the primary table.
INSERT INTO TABLE employee_backup_nt 
SELECT * FROM employee_nt;

-- Assume the file employee_data.csv is available in HDFS.
LOAD DATA INPATH '/user/hive/warehouse/employee_data.csv'
OVERWRITE INTO TABLE employee_nt;

-- Fetch all rows from the table.
SELECT * FROM employee_nt;

-- Fetch rows with specific conditions.
SELECT empid, empname FROM employee_nt WHERE salary > 100000;

-- Perform a simple join with another table (e.g., the backup table).
SELECT a.empid, a.empname, b.salary 
FROM employee_nt a JOIN employee_backup_nt b
ON a.empid = b.empid;

-- Create a temporary table to hold filtered data.
CREATE TABLE IF NOT EXISTS employee_filtered_nt AS
SELECT * FROM employee_nt WHERE empid != 102;

-- Overwrite the original table with the filtered data.
INSERT OVERWRITE TABLE employee_nt
SELECT * FROM employee_filtered_nt;

-- Drop the temporary table after use.
DROP TABLE IF EXISTS employee_filtered_nt;

-- Create a temporary table with the updated salary for empid = 101.
CREATE TABLE IF NOT EXISTS employee_updated_nt AS
SELECT empid,
       empname,
       empcity,
       CASE
           WHEN empid = 101 THEN 105000 -- Updated salary
           ELSE salary
       END AS salary
FROM employee_nt;

-- Overwrite the original table with the updated data.
INSERT OVERWRITE TABLE employee_nt
SELECT * FROM employee_updated_nt;

-- Drop the temporary table after use.
DROP TABLE IF EXISTS employee_updated_nt;

-- Step 1: Create Non-Transactional Tables
CREATE TABLE IF NOT EXISTS employee_nt (
    empid INT,
    empname STRING,
    empcity STRING,
    salary BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS employee_backup_nt (
    empid INT,
    empname STRING,
    empcity STRING,
    salary BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Step 2: Insert Data into employee_nt
INSERT INTO TABLE employee_nt VALUES (101, 'Alice', 'New York', 100000);
INSERT INTO TABLE employee_nt VALUES (102, 'Bob', 'San Francisco', 120000);
INSERT INTO TABLE employee_nt VALUES (103, 'Carol', 'Chicago', 90000);

-- Step 3: Insert Data from Query (Backup)
INSERT INTO TABLE employee_backup_nt 
SELECT * FROM employee_nt;

-- Step 4: Perform SELECT Queries
SELECT * FROM employee_nt;
SELECT empid, empname FROM employee_nt WHERE salary > 100000;

-- Step 5: Simulate DELETE by Overwriting
CREATE TABLE IF NOT EXISTS employee_filtered_nt AS
SELECT * FROM employee_nt WHERE empid != 102;
INSERT OVERWRITE TABLE employee_nt
SELECT * FROM employee_filtered_nt;
DROP TABLE IF EXISTS employee_filtered_nt;

-- Step 6: Simulate UPDATE by Overwriting
CREATE TABLE IF NOT EXISTS employee_updated_nt AS
SELECT empid,
       empname,
       empcity,
       CASE
           WHEN empid = 101 THEN 105000 -- Updated salary
           ELSE salary
       END AS salary
FROM employee_nt;
INSERT OVERWRITE TABLE employee_nt
SELECT * FROM employee_updated_nt;
DROP TABLE IF EXISTS employee_updated_nt;

hive > set hive.support.concurrency = true;
hive > set hive.enforce.bucketing = true;
hive > set hive.exec.dynamic.partition.mode = nonstrict;
hive > set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
hive > set hive.compactor.initiator.on = true;
hive > set hive.compactor.worker.threads = 1;

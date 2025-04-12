CREATE DATABASE college;
USE college;

CREATE TABLE Employee(empid INT,empname STRING,empcity STRING);

describe Employee;

insert into Employee values(200,’Sreedhar’,’Kurnool’);

select * from Employee;
//
ALTER TABLE Employee RENAME to GPREmployee;

desc GPREmployee;

ALTER TABLE GPREmployee ADD COLUMNS(Sal BIGINT);

ALTER TABLE GPREmployee CHANGE COLUMN empid empid1 BIGINT;

DROP TABLE GPREmployee;

DROP DATABASE college;
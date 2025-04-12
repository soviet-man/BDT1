student1_data.txt:
1,Rahul,85
2,Anjali,78
3,Sameer,90

student2_data.txt:
4,Neha,88
5,Arjun,92
6,Meera,75

customers.txt:
1,Ramesh,32,Ahmedabad,2000
2,Suresh,25,Delhi,1500
3,Kuresh,23,Kota,2000
4,Kalesh,25,Mumbai,6500
5,Sailesh,27,Bhopal,8500

orders.txt:
102,2009-10-08,3,3000
103,2008-05-20,4,2060
104,2011-06-15,1,5000

raw_sales.txt:
CatZ,Prod22-cZ,30,60
CatA,Prod88-cA,15,50
CatY,Prod07-cY,20,40

Commands and Outputs
->Set Operation: UNION
Load Data

pig
student1 = LOAD 'student1_data.txt' USING PigStorage(',') AS (studentid:int, studentname:chararray, percentage:int);
student2 = LOAD 'student2_data.txt' USING PigStorage(',') AS (studentid:int, studentname:chararray, percentage:int);
Perform UNION

pig
student_union = UNION student1, student2;
DUMP student_union;
Output:

(1,Rahul,85)
(2,Anjali,78)
(3,Sameer,90)
(4,Neha,88)
(5,Arjun,92)
(6,Meera,75)

->Set Operation: INNER JOIN
Load Data

pig
customers = LOAD 'customers.txt' USING PigStorage(',') AS (id:int, name:chararray, age:int, address:chararray, salary:int);
orders = LOAD 'orders.txt' USING PigStorage(',') AS (oid:int, date:chararray, customer_id:int, amount:int);
Perform INNER JOIN

pig
joined_data = JOIN customers BY id, orders BY customer_id;
DUMP joined_data;
Output:

(1,Ramesh,32,Ahmedabad,2000,104,2011-06-15,1,5000)
(3,Kuresh,23,Kota,2000,102,2009-10-08,3,3000)
(4,Kalesh,25,Mumbai,6500,103,2008-05-20,4,2060)

->Self Join Example
Load Data

pig
customers1 = LOAD 'customers.txt' USING PigStorage(',') AS (id:int, name:chararray, age:int, address:chararray, salary:int);
customers2 = LOAD 'customers.txt' USING PigStorage(',') AS (id:int, name:chararray, age:int, address:chararray, salary:int);
Perform Self Join

pig
self_joined = JOIN customers1 BY id, customers2 BY id;
DUMP self_joined;
Output:

(1,Ramesh,32,Ahmedabad,2000,1,Ramesh,32,Ahmedabad,2000)
(2,Suresh,25,Delhi,1500,2,Suresh,25,Delhi,1500)
...

->SORT Operation
Load Data

pig
rawSales = LOAD 'raw_sales.txt' USING PigStorage(',') AS (category:chararray, product:chararray, sales:long, total_sales_category:long);
->Group By

pig
grpByCatTotals = GROUP rawSales BY (total_sales_category, category);
DUMP grpByCatTotals;
Output:

((50,CatA),{(CatA,Prod88-cA,15,50)})
((60,CatZ),{(CatZ,Prod22-cZ,30,60)})
...
->Sort By

pig
sortGrpByCatTotals = ORDER grpByCatTotals BY group DESC;
DUMP sortGrpByCatTotals;
Output:

((60,CatZ),{(CatZ,Prod22-cZ,30,60)})
((50,CatA),{(CatA,Prod88-cA,15,50)})
...
->Limit Top Results

pig
topSalesCats = LIMIT sortGrpByCatTotals 2;
DUMP topSalesCats;
Output:

((60,CatZ),{(CatZ,Prod22-cZ,30,60)})
((50,CatA),{(CatA,Prod88-cA,15,50)})
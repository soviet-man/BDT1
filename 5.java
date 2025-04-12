gprec.txt
1,CE,120
2,EEE,120
3,ME,120
4,ECE,180
5,CSE,300

hdfs dfs -put /home/cloudera/Desktop/gprec.txt /
pig
A = LOAD '/gprec.txt' using PigStorage(',') as (sno:int,bname:charArray,st:int);
DUMP A;

DESCRIBE A;

B = FOREACH A GENERATE bname, st;
DUMP B;

C= ORDER A BY st DESC;
DUMP C;


create database details;

use details;

create table emp(
id int,
name char(20),
addr char(100)
);

-- DML

drop table emp;

describe emp;

alter table emp modify id int primary key;

alter table emp add age int;

alter table emp modify name char(50) not null;

-- DDL

select * from emp;

insert into emp values(4,"Guna","XCC",50);

select name,count(*) from emp group by name having count(*)>1;

delete from emp where id=1;




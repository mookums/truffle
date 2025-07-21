-- Create the Person Table
create table if not exists person (
    id uuid primary key,
    name text,
    age integer,
    weight float,
    height float,
    money integer
);

create table if not exists pet (
    id uuid primary key,
    name text,
    age integer,
    weight float
);

--- Select Columns from Person
select id, name, age from person;
select * from pet;

create table account (
    id int primary key,
    name text not null,
    email text,
    password text not null,
    status integer not null
);

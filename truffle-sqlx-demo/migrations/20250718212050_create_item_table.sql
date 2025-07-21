create table item (
    id int primary key,
    name text not null,
    price int not null default 0,
    description text,
    picture_url text
);

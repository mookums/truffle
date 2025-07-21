create table orders (
    id int primary key,
    account_id int references account(id),
    item_id int references item(id),
    count int not null default 1
);
    

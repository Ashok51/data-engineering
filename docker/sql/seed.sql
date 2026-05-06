create table if not exists staging_customers (
    customer_id int primary key,
    full_name varchar(255) not null,
    email varchar(255) not null unique,
    created_at timestamp default current_timestamp
);

create table if not exists dim_customer (
    customer_id int primary key,
    first_name varchar(255) not null,
    email varchar(255) not null unique,
    created_at timestamp default current_timestamp
);

insert into staging_customers (customer_id, full_name, email)
values
(1, 'John Doe', 'john.doe@example.com')
on conflict (customer_id) do nothing;
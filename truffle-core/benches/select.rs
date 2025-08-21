use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use truffle::Simulator;

fn criterion_benchmark(c: &mut Criterion) {
    let mut sim = Simulator::default();

    // Setup multiple tables for joins
    sim.execute("create table person (id int primary key, name text not null, age int)")
        .unwrap();
    sim.execute("create table address (id int primary key, person_id int, street text not null)")
        .unwrap();
    sim.execute("create table company (id int primary key, name text not null)")
        .unwrap();
    sim.execute("create table employment (person_id int, company_id int, salary int)")
        .unwrap();

    // Basic selects
    c.bench_function("basic select wildcard", |b| {
        b.iter(|| sim.execute(black_box("select * from person")).unwrap())
    });

    c.bench_function("basic select single field", |b| {
        b.iter(|| sim.execute(black_box("select id from person")).unwrap())
    });

    c.bench_function("basic select count", |b| {
        b.iter(|| {
            sim.execute(black_box("select count(*) from person"))
                .unwrap()
        })
    });

    // WHERE clauses
    c.bench_function("select with where equals", |b| {
        b.iter(|| {
            sim.execute(black_box("select * from person where id = 1"))
                .unwrap()
        })
    });

    c.bench_function("select with where comparison", |b| {
        b.iter(|| {
            sim.execute(black_box("select * from person where age > 25"))
                .unwrap()
        })
    });

    c.bench_function("select with where and", |b| {
        b.iter(|| {
            sim.execute(black_box(
                "select * from person where age > 18 and age < 65",
            ))
            .unwrap()
        })
    });

    c.bench_function("select with where like", |b| {
        b.iter(|| {
            sim.execute(black_box("select * from person where name like 'John%'"))
                .unwrap()
        })
    });

    // ORDER BY
    c.bench_function("select with order by", |b| {
        b.iter(|| {
            sim.execute(black_box("select * from person order by name"))
                .unwrap()
        })
    });

    c.bench_function("select with order by desc", |b| {
        b.iter(|| {
            sim.execute(black_box("select * from person order by age desc"))
                .unwrap()
        })
    });

    c.bench_function("select with multiple order by", |b| {
        b.iter(|| {
            sim.execute(black_box(
                "select * from person order by age desc, name asc",
            ))
            .unwrap()
        })
    });

    // JOINS
    c.bench_function("inner join", |b| {
        b.iter(|| {
            sim.execute(black_box(
                "select p.name, a.street from person p inner join address a on p.id = a.person_id",
            ))
            .unwrap()
        })
    });

    c.bench_function("left join", |b| {
        b.iter(|| {
            sim.execute(black_box(
                "select p.name, a.street from person p left join address a on p.id = a.person_id",
            ))
            .unwrap()
        })
    });

    // Complex combinations
    c.bench_function("join with where and order", |b| {
        b.iter(|| {
            sim.execute(black_box(
                r#"
                    select p.name, a.street from person p 
                    inner join address a on p.id = a.person_id 
                    where p.age > 21 
                    order by p.name
                "#,
            ))
            .unwrap()
        })
    });

    c.bench_function("complex select with aggregation", |b| {
        b.iter(|| {
            sim.execute(black_box(
                "select p.name, count(e.company_id)
                 from person p 
                 left join employment e on p.id = e.person_id 
                 where p.age between 25 and 55",
            ))
            .unwrap()
        })
    });

    c.bench_function("complex multi-table join with case", |b| {
        b.iter(|| {
            sim.execute(black_box(
                r#"
                select 
                    p.name,
                    p.age,
                    a.street,
                    c.name,
                    e.salary,
                    case 
                        when e.salary > 75000 then 'Senior'
                        when e.salary > 50000 then 'Mid-level' 
                        else 'Junior'
                    end as seniority_level,
                    case 
                        when p.age < 30 then 'Young Professional'
                        when p.age between 30 and 50 then 'Experienced'
                        else 'Senior Professional'
                    end as age_category,
                    case
                        when a.street like '%Main%' then 'Prime Location'
                        when a.street like '%Oak%' or a.street like '%Elm%' then 'Residential'
                        else 'Other'
                    end as location_type
                 from person p
                 inner join address a on p.id = a.person_id
                 inner join employment e on p.id = e.person_id
                 inner join company c on e.company_id = c.id
                 where p.age between 25 and 60
                   and e.salary > 40000
                   and c.name not like '%Startup%'
                   and a.street is not null
                 order by 
                    case when e.salary > 70000 then 1 else 2 end,
                    e.salary desc, 
                    p.age asc, 
                    c.name,
                    case 
                        when a.street like '%Main%' then 1
                        when a.street like '%First%' then 2
                        else 3
                    end
                "#,
            ))
            .unwrap()
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

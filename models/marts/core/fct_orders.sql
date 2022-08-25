with customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select 
        order_id,
        sum(case when status = 'success' then amount end) as amount
    from {{ ref('stg_payments') }}
    group by 1
),
final as (
    select
        o.order_id as order_id,
        o.order_date as order_date,
        c.customer_id as customer_id,
        p.amount as amount
    from orders o
    left join customers c using (customer_id)
    inner join payments p using (order_id)
)
select *
from final
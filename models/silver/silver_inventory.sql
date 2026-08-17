{{ config(
    materialized='table'
) }}

-- Step 1: pull only the product-history columns this model needs
with product_history as (

    select
        product_history_key,
        product_id,
        source_snapshot_date,
        stock_quantity,
        reorder_level,
        supplier_id,
        cost_price
    from {{ ref('silver_product_history') }}

),

-- Step 2: derive the product/store relationship - product history has no
-- store_id (the product JSON doesn't carry one), so the association comes
-- from wherever the product was actually sold
product_store as (

    select distinct
        product_id,
        store_id
    from {{ ref('silver_order_items') }}
    where product_id is not null
      and store_id is not null

),

-- Step 3: join history to the product/store relationship, producing the
-- required product + store + snapshot-date inventory grain (each stock
-- snapshot is associated with every observed product/store pairing)
inventory_snapshots as (

    select
        ph.product_id,
        ps.store_id,
        ph.source_snapshot_date as inventory_date,
        ph.stock_quantity as ending_stock,
        ph.reorder_level,
        ph.supplier_id,
        ph.cost_price
    from product_history ph
    inner join product_store ps
        on ph.product_id = ps.product_id

),

-- Step 4: pull the prior snapshot's ending stock forward as this
-- snapshot's beginning stock, per product/store
with_beginning_inventory as (

    select
        product_id,
        store_id,
        inventory_date,
        lag(ending_stock) over (
            partition by product_id, store_id
            order by inventory_date
        ) as beginning_stock,
        ending_stock,
        reorder_level,
        supplier_id,
        cost_price
    from inventory_snapshots

),

-- Step 5: sum completed/delivered order-item sales per product/store/date
-- (store_id here comes from order items, which inherited it from orders)
completed_sales as (

    select
        product_id,
        store_id,
        order_date as inventory_date,
        sum(quantity) as sold_quantity
    from {{ ref('silver_order_items') }}
    where lower(order_status) in ('completed', 'delivered')
      and product_id is not null
      and store_id is not null
      and order_date is not null
    group by product_id, store_id, order_date

),

-- Step 6: attach sales to stock - a left join is deliberate, since a
-- product/store can have inventory on a date with no completed sale
combined as (

    select
        b.product_id,
        b.store_id,
        b.inventory_date,
        b.beginning_stock,
        coalesce(s.sold_quantity, 0) as sold_quantity,
        b.ending_stock,
        b.reorder_level,
        b.supplier_id,
        b.cost_price
    from with_beginning_inventory b
    left join completed_sales s
        on b.product_id = s.product_id
       and b.store_id = s.store_id
       and b.inventory_date = s.inventory_date

),

-- Step 7: core inventory math
calculated as (

    select
        product_id,
        store_id,
        inventory_date,
        beginning_stock,
        sold_quantity,
        ending_stock,

        -- ending - beginning + sold
        (
            coalesce(ending_stock, 0)
            - coalesce(beginning_stock, 0)
            + coalesce(sold_quantity, 0)
        ) as purchased_quantity,

        case
            when ending_stock is not null and cost_price is not null
                then ending_stock * cost_price
            else null
        end as inventory_value,

        (coalesce(beginning_stock, 0) + coalesce(ending_stock, 0)) / 2.0
            as average_inventory,

        reorder_level,
        supplier_id,
        cost_price

    from combined

),

-- Step 8: compute the prior snapshot date once, reused by both the
-- gap flag and gap-days columns below instead of repeating the same
-- window function three times
with_prior_snapshot as (

    select
        c.*,
        lag(inventory_date) over (
            partition by product_id, store_id
            order by inventory_date
        ) as prior_inventory_date
    from calculated c

),

-- Step 9: final derived metrics
final as (

    select

        -- fact grain: product + store + date
        {{ dbt_utils.generate_surrogate_key([
            'product_id',
            'store_id',
            'inventory_date'
        ]) }} as inventory_key,

        product_id,
        store_id,
        inventory_date,

        beginning_stock,
        purchased_quantity,
        sold_quantity,
        ending_stock,

        inventory_value,

        case
            when average_inventory > 0 then sold_quantity / average_inventory
            else null
        end as stock_turnover_ratio,

        -- calculated later, at the supplier/product/store/date grain,
        -- from purchased_quantity
        case
            when purchased_quantity > 0 then 100.0
            else 0.0
        end as supplier_contribution_percentage,

        reorder_level,
        supplier_id,

        -- compares this snapshot to the prior one for the same product/store
        case
            when prior_inventory_date is null then false
            when datediff(day, prior_inventory_date, inventory_date) > 1 then true
            else false
        end as snapshot_gap_flag,

        case
            when prior_inventory_date is null then 0
            else datediff(day, prior_inventory_date, inventory_date)
        end as snapshot_gap_days,

        (
            ending_stock is not null
            and reorder_level is not null
            and ending_stock < reorder_level
        ) as low_stock_flag,

        (purchased_quantity < 0) as negative_inferred_purchase_flag

    from with_prior_snapshot

)

select * from final
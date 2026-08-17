{{ config(
    materialized = 'table'
) }}

-- Step 1: pull the measures and snapshot-quality fields from Silver
-- Inventory (snapshot_gap_flag / snapshot_gap_days already come from there)
with inventory as (

    select
        inventory_key,
        product_id,
        store_id,
        inventory_date,
        beginning_stock,
        purchased_quantity,
        sold_quantity,
        ending_stock,
        inventory_value,
        stock_turnover_ratio,
        supplier_contribution_percentage,
        supplier_id,
        snapshot_gap_flag,
        snapshot_gap_days
    from {{ ref('silver_inventory') }}

),

-- Step 2-5: pull just the key + natural-key columns needed from each
-- dimension to resolve surrogate keys below
products as (

    select product_key, product_id
    from {{ ref('dim_product') }}

),

stores as (

    select store_key, store_id
    from {{ ref('dim_store') }}

),

suppliers as (

    select supplier_key, supplier_id
    from {{ ref('dim_supplier') }}

),

dates as (

    select date_key, full_date
    from {{ ref('dim_date') }}

),

-- Step 6: resolve every dimension key against the inventory grain
-- (product + store + date), carrying the measures through unchanged
final as (

    select

        -- fact grain: product + store + date
        i.inventory_key,

        -- resolved dimension keys
        p.product_key,   -- Inventory.product_id   -> Dim_Products.product_id
        d.date_key,      -- Inventory.inventory_date -> Dim_date.full_date
        st.store_key,    -- Inventory.store_id     -> Dim_Stores.store_id
        s.supplier_key,  -- Inventory.supplier_id  -> Dim_Suppliers.supplier_id

        -- inventory measures
        i.beginning_stock,
        i.purchased_quantity,
        i.sold_quantity,
        i.ending_stock,
        i.inventory_value,
        i.stock_turnover_ratio,
        i.supplier_contribution_percentage,

        -- snapshot quality, carried through so reporting can distinguish
        -- on-time vs delayed snapshots
        i.snapshot_gap_flag,
        i.snapshot_gap_days

    from inventory i
    left join products p on i.product_id = p.product_id
    left join stores st on i.store_id = st.store_id
    left join suppliers s on i.supplier_id = s.supplier_id
    left join dates d on i.inventory_date = d.full_date

)

-- Step 7: final column selection for the inventory fact table
select
    inventory_key,
    product_key,
    date_key,
    store_key,
    supplier_key,
    beginning_stock,
    purchased_quantity,
    sold_quantity,
    ending_stock,
    inventory_value,
    stock_turnover_ratio,
    supplier_contribution_percentage,
    -- snapshot monitoring fields
    snapshot_gap_flag,
    snapshot_gap_days
from final
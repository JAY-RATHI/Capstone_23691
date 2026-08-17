{{ config(
    materialized='table'
) }}

-- Step 1: pull the campaign columns needed downstream
with campaigns as (

    select
        campaign_key,
        campaign_id,
        budget,
        start_date,
        end_date
    from {{ ref('dim_marketing_campaign') }}

),

-- Step 2: pull the date dimension columns needed downstream
dates as (

    select
        date_key,
        full_date
    from {{ ref('dim_date') }}

),

-- Step 3: sales base
-- there is no separate Gold FACT_Sales model, so this stands in for it
-- using Silver orders data. required attributes: CampaignKey, DateKey,
-- CustomerKey, TotalSalesAmount
sales_base as (

    select

        o.order_id,

        -- campaign key, generated from the natural campaign_id
        {{ dbt_utils.generate_surrogate_key(['o.campaign_id']) }} as campaign_key,

        -- date key, YYYYMMDD format
        to_number(to_char(o.order_date, 'YYYYMMDD')) as date_key,

        -- customer key, generated from the natural customer_id
        {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }} as customer_key,

        -- natural keys
        o.customer_id,
        o.campaign_id,
        o.order_date,

        coalesce(o.total_amount, 0.00) as total_sales_amount

    from {{ ref('silver_orders') }} o
    where o.order_id is not null

),

-- Step 4: customer purchase history
-- each customer's first-ever purchase date, needed to distinguish
-- first vs repeat purchases
customer_purchase_history as (

    select
        customer_id,
        min(order_date) as first_purchase_date
    from sales_base
    where customer_id is not null
    group by customer_id

),

-- Step 5: sales with purchase flags
-- is_first_purchase: true when the order falls on the customer's
--   first-ever purchase date
-- is_repeat_purchase: true when the customer had already purchased
--   before this order
-- (a customer is never double-counted at the campaign level since the
--  later aggregation uses distinct customer_key)
fact_sales as (

    select

        s.order_id,

        s.campaign_key,
        s.date_key,
        s.customer_key,

        s.customer_id,
        s.campaign_id,
        s.order_date,

        s.total_sales_amount,

        (s.order_date = h.first_purchase_date) as is_first_purchase,
        (s.order_date > h.first_purchase_date) as is_repeat_purchase

    from sales_base s
    left join customer_purchase_history h
        on s.customer_id = h.customer_id

),

-- Step 6: campaign customer population
-- grain: one row per campaign per customer. a customer counts as
-- campaign-influenced when they have sales tied to that campaign
-- during its active window
campaign_customers as (

    select distinct
        c.campaign_key,
        c.campaign_id,
        fs.customer_key,
        fs.customer_id,
        c.start_date,
        c.end_date
    from campaigns c
    inner join fact_sales fs
        on fs.campaign_key = c.campaign_key
        and fs.order_date between c.start_date and c.end_date
    where fs.customer_key is not null

),

-- Step 7: customer-level campaign purchase status
-- grain: one row per campaign per customer, so multiple orders from the
-- same customer don't inflate the customer-level metrics
campaign_customer_status as (

    select

        cc.campaign_key,
        cc.customer_key,

        -- customer made their first-ever purchase during the campaign
        max(case when fs.is_first_purchase = true then 1 else 0 end) as is_first_purchase,

        -- customer made a purchase after their first-ever purchase;
        -- max() collapses multiple repeat purchases to a single count
        max(case when fs.is_repeat_purchase = true then 1 else 0 end) as is_repeat_purchase

    from campaign_customers cc
    inner join fact_sales fs
        on fs.campaign_key = cc.campaign_key
        and fs.customer_key = cc.customer_key
        and fs.order_date between cc.start_date and cc.end_date
    group by cc.campaign_key, cc.customer_key

),

-- Step 8: campaign customer metrics
-- calculated at campaign/customer grain:
--   total_campaign_customers  = all distinct customers the campaign influenced
--   first_purchase_customers  = whose first-ever purchase happened during the campaign
--   repeat_purchase_customers = who made a repeat purchase during the campaign
-- total_campaign_customers is the denominator for repeat purchase rate,
-- since that's defined as the share of campaign-influenced customers
-- who made a repeat purchase
campaign_customer_metrics as (

    select

        campaign_key,

        count(distinct customer_key) as total_campaign_customers,

        count(distinct case when is_first_purchase = 1 then customer_key else null end)
            as first_purchase_customers,

        count(distinct case when is_repeat_purchase = 1 then customer_key else null end)
            as repeat_purchase_customers

    from campaign_customer_status
    group by campaign_key

),

-- Step 9: campaign/date grain
-- required FACT_MarketingPerformance grain: one row per campaign per date
campaign_dates as (

    select

        c.campaign_key,
        c.campaign_id,
        c.budget,
        c.start_date,
        c.end_date,

        d.date_key,
        d.full_date

    from campaigns c
    inner join dates d
        on d.full_date between c.start_date and c.end_date

),

-- Step 10: total sales influenced by campaign
-- spec logic: SUM(FACT_Sales.TotalSalesAmount)
--   WHERE FACT_Sales.CampaignKey = Campaign.CampaignKey
--     AND FACT_Sales.DateKey BETWEEN Campaign.StartDate AND Campaign.EndDate
-- aggregated per campaign/date since that's the fact grain
sales_influenced as (

    select

        cd.campaign_key,
        cd.date_key,
        coalesce(sum(fs.total_sales_amount), 0.00) as total_sales_influenced

    from campaign_dates cd
    left join fact_sales fs
        on fs.campaign_key = cd.campaign_key
        and fs.date_key = cd.date_key
        and fs.order_date between cd.start_date and cd.end_date
    group by cd.campaign_key, cd.date_key

),

-- Step 11: new customers acquired
-- spec logic: COUNT(DISTINCT CustomerKey)
--   WHERE s.CampaignKey = Campaign.CampaignKey
--     AND s.DateKey BETWEEN Campaign.StartDate AND Campaign.EndDate
--     AND s.CustomerKey NOT IN (
--         SELECT prior.CustomerKey FROM FACT_Sales prior
--         WHERE prior.DateKey < Campaign.StartDate)
-- NOT EXISTS is used in place of NOT IN to handle null customer values safely
new_customers as (

    select

        cd.campaign_key,
        cd.date_key,

        count(distinct case
            when fs.customer_key is not null
                 and fs.order_date between cd.start_date and cd.end_date
                 and not exists (
                     select 1
                     from fact_sales prior
                     where prior.customer_key = fs.customer_key
                       and prior.order_date < cd.start_date
                 )
                then fs.customer_key
            else null
        end) as new_customers_acquired

    from campaign_dates cd
    left join fact_sales fs
        on fs.campaign_key = cd.campaign_key
        and fs.date_key = cd.date_key
    group by cd.campaign_key, cd.date_key

),

-- Step 12: assemble every metric onto the campaign/date grain
metrics as (

    select

        cd.campaign_key,
        cd.campaign_id,
        cd.date_key,
        cd.full_date,
        cd.budget,
        cd.start_date,
        cd.end_date,

        coalesce(si.total_sales_influenced, 0.00) as total_sales_influenced,
        coalesce(nc.new_customers_acquired, 0) as new_customers_acquired,
        coalesce(ccm.total_campaign_customers, 0) as total_campaign_customers,
        coalesce(ccm.first_purchase_customers, 0) as first_purchase_customers,
        coalesce(ccm.repeat_purchase_customers, 0) as repeat_purchase_customers

    from campaign_dates cd
    left join sales_influenced si
        on cd.campaign_key = si.campaign_key
        and cd.date_key = si.date_key
    left join new_customers nc
        on cd.campaign_key = nc.campaign_key
        and cd.date_key = nc.date_key
    left join campaign_customer_metrics ccm
        on cd.campaign_key = ccm.campaign_key

),

-- Step 13: final fact grain = one row per campaign per date
final as (

    select

        -- campaign + date defines the fact grain
        {{ dbt_utils.generate_surrogate_key(['campaign_key', 'date_key']) }}
            as marketing_performance_key,

        -- dimension keys
        campaign_key,
        date_key,

        -- traceability
        campaign_id,
        full_date,

        total_sales_influenced,
        new_customers_acquired,

        -- repeat purchase rate: share of campaign-influenced customers
        -- who made a repeat purchase
        -- formula: repeat_purchase_customers / total_campaign_customers * 100
        -- both sides count distinct customers, so this can't exceed 100%
        case
            when total_campaign_customers > 0
                then 100.0 * repeat_purchase_customers
                     / nullif(total_campaign_customers, 0)
            else null
        end as repeat_purchase_rate,

        budget as total_campaign_cost,

        -- spec logic: ROI = CASE WHEN TotalCampaignCost > 0
        --   THEN (TotalSalesInfluenced - TotalCampaignCost) / TotalCampaignCost * 100
        --   ELSE NULL END
        case
            when budget > 0
                then (total_sales_influenced - budget) / budget * 100
            else null
        end as roi

    from metrics

)

-- Step 14: final output
select

    marketing_performance_key,

    campaign_key,
    date_key,

    campaign_id,
    full_date,

    total_sales_influenced,
    new_customers_acquired,

    repeat_purchase_rate,

    total_campaign_cost,
    roi

from final
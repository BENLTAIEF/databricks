-- Databricks notebook source
use dbt_dev.dbo;

-- COMMAND ----------

drop database dbt_dev.dbo cascade;

-- COMMAND ----------

select * from raw_listings where id = 3176;

-- COMMAND ----------

update raw_listings set minimum_nights = 50, updated_at=current_timestamp() where id = 3176;

-- COMMAND ----------

select * from scd_raw_listings where id = 3176;

-- COMMAND ----------

select distinct room_type from dim_listings_cleansed

-- COMMAND ----------

with all_values as (

    select
        room_type as value_field,
        count(*) as n_records

    from `dbt_dev`.`dbo`.`dim_listings_cleansed`
    group by room_type

)

select *
from all_values
where value_field not in (
    'Entire home/apt','Private room','Shared room','Hotel room'
)

-- COMMAND ----------

with child as (
    select host_id as from_field
    from `dbt_dev`.`dbo`.`dim_listings_cleansed`
    where host_id is not null
),

parent as (
    select host_id as to_field
    from `dbt_dev`.`dbo`.`dim_hosts_cleansed`
)

select distinct
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


-- COMMAND ----------

select * from dim_listings_cleansed 

-- COMMAND ----------

select * from fct_reviews a inner join dim_listings_cleansed b on a.listing_id = b.listing_id
where a.review_date < b.created_at;

-- COMMAND ----------

select * from fct_reviews;

-- COMMAND ----------


    with a as (
        
    select
        
        count(*) as expression
    from
        `dbt_dev`.`dbo`.`dim_listings_w_hosts`
    

    ),
    b as (
        
    select
        
        count(*) * 1 as expression
    from
        `dbt_dev`.`dbo`.`raw_listings`
    

    ),
    final as (

        select
            
            a.expression,
            b.expression as compare_expression,
            abs(coalesce(a.expression, 0) - coalesce(b.expression, 0)) as expression_difference,
            abs(coalesce(a.expression, 0) - coalesce(b.expression, 0))/
                nullif(a.expression * 1.0, 0) as expression_difference_percent
        from
        
            a cross join b
        
    )
    -- DEBUG:
    -- select * from final
    select
        *
    from final
    where
        
        expression_difference > 0.0
        


-- COMMAND ----------


    with a as (
        
    select
        
        count(*) as expression
    from
        `dbt_dev`.`dbo`.`dim_listings_w_hosts`
    

    ),
    b as (
        
    select
        
        count(*) * 1 as expression
    from
        `dbt_dev`.`dbo`.`raw_listings`
    

    ),
    final as (

        select
            
            a.expression,
            b.expression as compare_expression,
            abs(coalesce(a.expression, 0) - coalesce(b.expression, 0)) as expression_difference,
            abs(coalesce(a.expression, 0) - coalesce(b.expression, 0))/
                nullif(a.expression * 1.0, 0) as expression_difference_percent
        from
        
            a cross join b
        
    )
    -- DEBUG:
    -- select * from final
    select
        *
    from final
    where
        
        expression_difference > 0.0
        


-- COMMAND ----------






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and max(price) >= 1 and max(price) <= 5000
)
 as expression


    from `dbt_dev`.`dbo`.`dim_listings_w_hosts`
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







-- COMMAND ----------

select * from `dbt_dev`.`dbo`.`dim_listings_w_hosts` where price > 5000;

-- COMMAND ----------

WITH grouped_expression
AS (
	SELECT count(DISTINCT room_type), count(DISTINCT room_type) = 20 AS expression
	FROM `dbt_dev`.`dbo`.`raw_listings`
	WHERE minimum_nights IS NOT NULL
	)
	SELECT *
	FROM grouped_expression
	WHERE NOT (expression = true)


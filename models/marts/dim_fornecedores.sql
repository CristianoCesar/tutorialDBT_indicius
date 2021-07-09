with staging as (

    select *
    from {{ ref('stg_suppliers')}}

)

select * from staging
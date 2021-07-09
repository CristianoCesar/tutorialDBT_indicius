with staging as (

    select *
    from {{ ref('stg_shippers')}}

)

select * from staging
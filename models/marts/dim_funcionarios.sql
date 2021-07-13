with staging as (

    select *
    from {{ ref('stg_employees')}}

)

select * from staging
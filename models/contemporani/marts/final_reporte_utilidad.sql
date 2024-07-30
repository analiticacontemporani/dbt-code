with

utilidad as (
    select * from {{ ref('int_contemporani__utilidad') }}
)

select * from utilidad
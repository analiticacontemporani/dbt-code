with 

source as (

    select * from {{ source('contemporani_banca', 'SaldosCuentas') }}

),

renamed as (

    select
        id,
        rowversion,
        idcuenta,
        ejercicio,
        tipo,
        saldoini,
        importes1,
        importes2,
        importes3,
        importes4,
        importes5,
        importes6,
        importes7,
        importes8,
        importes9,
        importes10,
        importes11,
        importes12,
        importes13,
        importes14,
        timestamp

    from source

)

select * from renamed

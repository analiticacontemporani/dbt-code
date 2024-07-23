with 

source as (

    select * from {{ source('contemporani_banca', 'Ejercicios') }}

),

renamed as (

    select
        id,
        rowversion,
        ejercicio,
        tipoper,
        periodos,
        fecinieje,
        fecfineje,
        feciniper1,
        feciniper2,
        feciniper3,
        feciniper4,
        feciniper5,
        feciniper6,
        feciniper7,
        feciniper8,
        feciniper9,
        feciniper10,
        feciniper11,
        feciniper12,
        feciniper13,
        feciniper14,
        idctacierre,
        idpolcierre,
        timestamp

    from source

)

select * from renamed

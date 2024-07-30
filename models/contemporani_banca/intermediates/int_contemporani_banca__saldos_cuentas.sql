  /*
Autor: Rodrigo Benavides
Fecha completado: Diciembre 27, 2022
fecha actualización: Enero 7, 2023
Este query es para obtener el balance general de todas las empresas de imobilem. Se hacen subtablas para los "ejercicios" y "saldoscuentas" para transformar los datos de formato "wide" a formato "long" para que sean más faciles de agrupar y analizar.
La idea es replicar el trabajo que hace Contpaq en la nube para nosotros poder desplegarlo en un dashboard y tener mayor flexibilidad y agilidad.
Al final, hay un cálculo manual para determinar la "Utilidad o Perdida del Ejercicio". Esto luego se ve en la sección de "CAPITAL"
*/

with
  -- Esta primera tabla es para transformar los datos de los ejercicios de formato "wide" a formato "long"
fechas AS (
    select
        id,
        ejercicio,
        num_fecini,
        substr (num_fecini, 10) as mes_periodo,
    --Para quedarnos con el mes correspondiente
    safe_cast(substr (fecha_periodo, 1, 10) as date) fecha_periodo
    from (
        select
            id,
            ejercicio,
            regexp_replace(split(pair, ':')[OFFSET(0)], r'^"|"$', '') num_fecini,
            --Se utiliza la fecha de inicio del periodo (primero de mes)
            regexp_replace(split(pair, ':')[OFFSET(1)], r'^"|"$', '') fecha_periodo

        from {{ ref('stg_contemporani_banca__Ejercicios') }} t,
        UNNEST(SPLIT(REGEXP_REPLACE(TO_JSON_STRING(t), r'{|}', ''))) pair )
    where
        lower(num_fecini) like 'fec%' 
        AND substr(num_fecini, 10) IN ('1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12')
),

  -- Solamente obtener las filas con fechas y quitarnos los nulos
  -- Esta siguiente tabla temporal es para unir todos los datos de "saldoscuentas" y así poder hacer el join correspondiente. Esto para evitar hacer un loop para cada empresa. Mejor el análisis se hace con un group by con el nombre de la base de datos, en la que cadda empresa tiene una distinta.
saldoscuentas_todo as (
  select * from {{ ref('stg_contemporani_banca__SaldosCuentas') }}
),

-- Se hace un join de los saldos de las cuentas con las distintas fechas 
saldoscuentas_fecha_todo as (
    select
        idcuenta,
        tipo,
        f.fecha_periodo,
        sc.num_importe,
        SAFE_CAST(saldoini as FLOAT64) value
    from 
    (
        select
            idcuenta,
            ejercicio,
            tipo,
            REGEXP_REPLACE(SPLIT(pair, ':')[OFFSET(0)], r'^"|"$', '') num_importe,
            REGEXP_REPLACE(SPLIT(pair, ':')[OFFSET(1)], r'^"|"$', '') saldoini

        FROM 
        saldoscuentas_todo t,
        UNNEST(SPLIT(REGEXP_REPLACE(TO_JSON_STRING(t), r'{|}', ''))) pair 
    ) sc
    LEFT JOIN fechas f
        ON f.id = sc.ejercicio
        AND f.mes_periodo = SUBSTR(sc.num_importe, 9) -- Para obtener el numero de mes

    WHERE
        LOWER(sc.num_importe) LIKE 'importes%'), -- Solamente nos quedamos con las filas que tengan algun importe
  -- Aquí es donde el análisis sucede. Se hace un join con los saldos de la cuentas en distintos meses con la tabla de cuentas para obtener más información acerca de las cuentas.

bg_todo AS (
    select
        C.Id as id,
        C.Codigo as codigo,
        C.Nombre as nombre,
        fecha_periodo,

        -- Cambiando valores de letras a su significado para mayor entendimiento
        CASE
            WHEN c.tipo = 'A' THEN 'Activo Deudora'
            WHEN c.tipo = 'B' THEN 'Activo Acreedora'
            WHEN c.tipo = 'C' THEN 'Pasivo Deudora'
            WHEN c.tipo = 'D' THEN 'Pasivo Acreedora'
            WHEN c.tipo = 'E' THEN 'Capital Deudora'
            WHEN c.tipo = 'F' THEN 'Capital Acreedora'
            WHEN c.tipo = 'G' THEN 'Resultados Deudora'
            WHEN c.tipo = 'H' THEN 'Resultados Acreedora'
            WHEN c.tipo = 'I' THEN 'Estadisitca Deudora'
            WHEN c.tipo = 'J' THEN 'Estadistica Acreedora'
            WHEN c.tipo = 'K' THEN 'Orden Deudora'
            WHEN c.tipo = 'L' THEN 'Orden Acreedora'
        END AS tipo,

        -- Cambiando valores de numeros a su significado para mayor entendimiento
        CASE
            WHEN c.ctamayor = 1 THEN 'Mayor'
            WHEN c.ctamayor = 2 THEN 'No'
            WHEN c.ctamayor = 3 THEN 'Titulo' 
            WHEN c.ctamayor = 4 THEN 'Subtitulo'
        END AS cuenta_mayor,

        -- Obtener los saldos Deudores
        MAX(
            CASE
                WHEN SC.Tipo = 1 THEN 
                    CASE
                        WHEN C.Tipo = 'A'
                            OR C.Tipo = 'C'
                            OR C.Tipo = 'E'
                            OR C.Tipo = 'G'
                            OR C.Tipo = 'I'
                            OR C.Tipo = 'K' THEN SC.value
                        ELSE 0
                    END
            END
        ) saldo_ini_deudor,

        -- Obtener los saldos acreedores
        MAX(
            CASE
                WHEN SC.Tipo = 1 THEN 
                    CASE
                        WHEN C.Tipo = 'B'
                            OR C.Tipo = 'D'
                            OR C.Tipo = 'F'
                            OR C.Tipo = 'H'
                            OR C.Tipo = 'J'
                            OR C.Tipo = 'L' THEN SC.value
                        ELSE 0
                    END
            END
        ) saldo_ini_acreedor,

        MAX( CASE WHEN SC.Tipo = 2 THEN SC.value END) as cargos, -- Obtener  Cargos
        MAX(CASE WHEN SC.Tipo = 3 THEN SC.value END) as abonos,  -- Obtener Abonos

    FROM {{ ref('stg_contemporani_banca__Cuentas') }} C
    LEFT OUTER JOIN saldoscuentas_fecha_todo SC
        ON C.Id = SC.IdCuenta

    WHERE
        fecha_periodo IS NOT NULL -- Quitar cuentas que nunca tuvieron movimientos
        AND SC.value != 0 --Quitar cuentas que no tuvieron saldos o movimientos en cierto periodo
    GROUP BY 
        C.id,
        C.Codigo,
        C.tipo,
        C.ctamayor,
        C.Nombre,
        SC.fecha_periodo 
)
 
-- Union del cálculo manual y el resto de contpaq en una sola tabla
SELECT *
FROM bg_todo


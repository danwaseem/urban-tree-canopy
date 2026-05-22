/*
  mart_species_health
  -------------------
  Species-level health and NDVI summary used for ecological analysis.
  Rows with null spc_common are excluded (unidentified trees).
*/

{{
  config(materialized='table')
}}

with base as (
    select * from {{ ref('stg_tree_census') }}
    where spc_common is not null
      and spc_common != ''
)

select
    spc_common,
    max(spc_latin)                                  as spc_latin,
    health,

    count(*)                                        as tree_count,
    round(avg(ndvi_value)::numeric, 4)              as avg_ndvi,
    round(avg(tree_dbh)::numeric, 2)                as avg_tree_dbh

from base
group by
    spc_common,
    health
order by
    tree_count desc

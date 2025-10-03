{{ config(materialized='table') }}
select * from {{ source('staging', 'pp_dsf_1_scto') }}
where key not in ('uuid:a16007f9-0eda-4609-9067-97a88ae96804', 'uuid:8b6b4d93-2cb7-44a3-8c27-a693b659b527', 
'uuid:aa748437-fdc1-4fc6-acbf-ac751d7730d9')
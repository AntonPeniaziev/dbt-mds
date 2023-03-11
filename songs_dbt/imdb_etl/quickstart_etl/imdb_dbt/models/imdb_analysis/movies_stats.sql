

select * from {{ source('company_db', 'movies') }}
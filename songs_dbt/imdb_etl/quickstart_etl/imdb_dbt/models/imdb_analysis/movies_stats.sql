

select * from {{ source('company_db', 'movies') }} m join {{ source('company_db', 'actors') }} a
         on m.id = a.movie_id join {{ source('company_db', 'writers') }} w on m.id = w.movie_id


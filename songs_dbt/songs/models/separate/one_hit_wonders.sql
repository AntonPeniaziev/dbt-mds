select
    artist, count(distinct song_name) = 1 as one_hit_wonder
from
    {{ref('song_daily_stats')}}
where
    is_heavily_rotated = true
group by
    artist
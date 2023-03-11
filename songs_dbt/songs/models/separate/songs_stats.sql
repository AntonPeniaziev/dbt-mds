select
    CAST(avg(total_daily_plays) as INTEGER)  avg_daily_plays, artist, song_name, count(distinct day) heavily_rotated_days
from
    {{ ref('song_daily_stats') }}
where
    is_heavily_rotated = true
group by artist, song_name
limit 10
select
    sum(plays) as total_daily_plays, day, lower(artist) artist, lower(song_name) song_name, sum(plays) > 15 is_heavily_rotated
from
    {{ ref('songs') }}
where date_part('year', day) = 2020
group by
    day, lower(artist), lower(song_name)
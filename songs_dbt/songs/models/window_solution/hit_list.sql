
with total_song_plays as (
select
    sum(plays) total_daily_plays, sum(plays) > 15 is_heavily_rotated, day, lower(artist) artist, lower(song_name) song_name
from
    main_seed_data.songs
where plays is not null
and date_part('year', day) = 2020
group by day, lower(artist), lower(song_name)
)
,
heavily_rotated_and_avg as (
select
    *,
    case when is_heavily_rotated = true then dense_rank() over (partition by artist, song_name order by day desc) else 0 end as aux_heavily,
    avg(total_daily_plays) over (partition by artist, song_name) as avg_daily_play_count
from total_song_plays
)
,
heavily_rotated_and_avg_clean as (
select
    *,
    max(aux_heavily) over (partition by artist, song_name) as heavily_rotated_days,
from
    heavily_rotated_and_avg
)
,
days_before_last_streak as (
select
    *,
    lag(is_heavily_rotated, 1) over (partition by artist, song_name order by day) as is_heavily_on_last_play,
    day - lag(day, 1) over (partition by artist, song_name order by day) as days_since_last_play
from
    heavily_rotated_and_avg_clean
)
,
is_heavily_rotated_day_before as (
select
    *,
    case when is_heavily_rotated = true and is_heavily_on_last_play = true and days_since_last_play = 1 then true else false end as is_heavily_rotated_day_before
from
    days_before_last_streak
)
,
streak_changed as (
select
    *,
    case when is_heavily_rotated != is_heavily_rotated_day_before then 1 else 0 end as streak_changed
from
    is_heavily_rotated_day_before
)

,
streak_id as (
select
    *,
    sum(streak_changed) over (partition by artist, song_name order by day) as streak_id
from
    streak_changed
where is_heavily_rotated = true
),
streak_count as (
select
    *,
    ROW_NUMBER() over (partition by artist, song_name, streak_id order by day) as streak_count
from
    streak_id
)
,
streak_ranked as (
select
   *,
   max(streak_count) over (partition by artist, song_name) as longest_cons_days
from
   streak_count
)
,
one_hit_wonders as (
select
   artist, count(distinct song_name) = 1 as one_hit_wonder,
from
    streak_ranked
group by artist
)
,
final as (
select artist, song_name, heavily_rotated_days, CAST (avg_daily_play_count as INTEGER) avg_daily_play_count, longest_cons_days
from streak_ranked
group by artist, song_name, heavily_rotated_days,
    avg_daily_play_count, longest_cons_days

order by heavily_rotated_days desc
)
select * from final f join one_hit_wonders o on f.artist = o.artist

limit 10

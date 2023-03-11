with days_before_last_streak as (
    select *,
        lag(is_heavily_rotated, 1)
        over (partition by artist, song_name order by day)                   as is_heavily_on_last_play,
        day - lag(day, 1) over (partition by artist, song_name order by day) as days_since_last_play
    from {{ref('song_daily_stats')}}
),
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
select artist, song_name, longest_cons_days
from streak_ranked
group by artist, song_name, longest_cons_days

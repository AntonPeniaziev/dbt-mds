select *
from
    {{ref('songs_stats')}} ss
join {{ref('longest_consec')}} lc
    on ss.artist = lc.artist and ss.song_name = lc.song_name
join {{ref('one_hit_wonders')}}  ohw
    on ss.artist = ohw.artist
order by heavily_rotated_days desc
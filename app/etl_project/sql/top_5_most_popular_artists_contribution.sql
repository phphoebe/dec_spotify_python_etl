with track_artist as (
    select
        track_id,
        track_name,
        unnest(string_to_array(artist_id, ', ')) as artist_id
    from 
        tracks

), artist as (
    select
        artist_id,
        artist_name,
        artist_popularity,
        dense_rank() over (order by artist_popularity desc) as artist_popularity_rnk
    from 
        artists
)

select
    a.artist_name,
    a.artist_popularity,
    a.artist_popularity_rnk,
    count(distinct t.track_id) as total_tracks_contributed

from 
    track_artist as t
left join 
    artist as a
        on t.artist_id = a.artist_id

where 
    a.artist_popularity_rnk <= 5

group by 1,2,3

order by 3, 1
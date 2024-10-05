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
        artist_genres,
        unnest(string_to_array(regexp_replace(artist_genres, '[{}"]', '', 'g'), ',')) as genre
    from 
        artists

)
select
    a.genre,
    count(distinct t.track_id) as num_tracks,
    count(distinct a.artist_id) as num_artists,
    string_agg(distinct t.track_name, ', ') as track_names,
    string_agg(distinct a.artist_name, ', ') as artist_names

from 
    track_artist as t
left join 
    artist as a
        on t.artist_id = a.artist_id

where 
    a.genre is not null

group by 1

order by 2 desc, 3 desc
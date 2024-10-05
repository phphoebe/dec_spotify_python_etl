with track_album as (

	select
		track_id,
		album_id,
		track_popularity,
		concat(
            (track_duration_ms / 60000), 
            ':', 
            lpad(cast((track_duration_ms % 60000) / 1000 as varchar), 2, '0')
        )  as track_duration_min
	from 
        tracks

), album_metrics as (

	select 
        distinct
            a.album_name,
            a.album_release_date,
            a.album_total_tracks,
            count(t.track_id) over (partition by a.album_id) as num_tracks_in_playlist,
            cast(round(avg(t.track_popularity) over (partition by a.album_id),0) as int) as avg_album_track_popularity,
            cast(round(avg(t.track_popularity) over (), 0) as int) as avg_playlist_track_popularity
	from 
        albums as a
	left join track_album as t
		on a.album_id = t.album_id
)

select
	album_name,
	album_release_date,
	album_total_tracks,
	num_tracks_in_playlist,
	avg_album_track_popularity,
	dense_rank() over (order by avg_album_track_popularity desc) as album_track_popularity_rnk,
	avg_playlist_track_popularity,
	case 
        when avg_album_track_popularity > avg_playlist_track_popularity 
        then 1 
        else 0 
    end as is_more_popular_than_playlist_overall

from 
    album_metrics

order by 
    album_track_popularity_rnk
with track_album as (

	select
		track_id,
		album_id,
		track_popularity
	from 
        tracks

)
select
	a.album_name,
	left(a.album_release_date, 7) as album_release_year_month,
	cast(round(avg(t.track_popularity) over (
        partition by left(a.album_release_date, 7)),0) as int) as avg_release_year_month_track_popularity,
	count(t.track_id) over (
        partition by left(a.album_release_date, 7)) as num_tracks_per_release_year_month

from 
    track_album as t
left join albums as a
	on t.album_id = a.album_id

order by 
    album_release_year_month desc
select
    track_id,
    track_name,
    track_popularity,
    concat(
	(track_duration_ms / 60000), 
	':', 
	lpad(cast((track_duration_ms % 60000) / 1000 as varchar), 2, '0')
    ) as track_duration_min, 
    dense_rank() over (order by track_popularity desc) as popularity_rnk

from 
    tracks

order by 
    popularity_rnk

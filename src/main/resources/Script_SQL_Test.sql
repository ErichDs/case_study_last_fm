-- Engine: postgres
-- Author: ErichDs

with session_landing as (
	select
		user_id
		,time_stamp
		,to_char(time_stamp, 'YYYY-MM-DD') as date_session
		,to_char(time_stamp, 'HH24:MI:SS') as time_session
		,track_name
		,artist_name
	from sessions
	order by
		date_session,
		time_session
),

session_bronze as (
	select
		user_id
		,track_name
		,artist_name
		,time_stamp
		,date_session
		,lag(time_stamp) over (partition by user_id order by time_stamp) as previous_time_session
	from session_landing
),

session_silver_diff as (
	select
		user_id
		,track_name
		,artist_name
		,time_stamp
		,date_session as current_session_date
		,to_char(previous_time_session, 'YYYY-MM-DD') as previous_session_date
		,previous_time_session
		,floor(extract(epoch from (time_stamp::timestamp - previous_time_session::timestamp)) / 60) as diff_min
	from session_bronze
),

session_silver_flag as (
	select
		user_id
		,track_name
		,artist_name
		,time_stamp
		,current_session_date
		,previous_session_date
		,diff_min
		,case
			when diff_min >= 20
			then 1
			else 0
		end as fl_new_session
	from session_silver_diff
	order by
	user_id
	,time_stamp
),

sessions_gold as (
select
	user_id
	,track_name
	,artist_name
	,time_stamp
	,current_session_date
	,previous_session_date
	,diff_min
	,fl_new_session
	,concat(user_id, '_', sum(fl_new_session) over (partition by user_id order by time_stamp)) as session_id
from session_silver_flag
group by
	user_id
	,track_name
	,artist_name
	,time_stamp
	,current_session_date
	,previous_session_date
	,diff_min
	,fl_new_session
order by 
	time_stamp
),

top_sessions as (
	select
		session_id
		,count(track_name) as tracks_count
	from sessions_gold
	group by
		session_id
	order by
		tracks_count desc
	limit 50
)


select
	a.artist_name
	,a.track_name
	,count(a.track_name) as play_count
from sessions_gold a
where
	session_id in (select session_id from top_sessions)
group by
	a.artist_name
	,a.track_name
order by
	play_count desc
limit 10
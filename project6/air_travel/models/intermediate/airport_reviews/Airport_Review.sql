with int_Airport_Review as (
	select r.id, r.thread_id, r.icao, r.date_created, r.author, r.subject, r.body, s.relevance, s.sentiment
	from {{ ref('tmp_airport_reviews') }} r
	left join {{ ref('tmp_airport_reviews_with_sentiment') }} s
	on r.id = s.id
	where r.icao in (select distinct icao from {{ ref('Airport') }})
)

select *
from int_Airport_Review
where id not in (select id from int_Airport_Review group by id having count(*) > 1)

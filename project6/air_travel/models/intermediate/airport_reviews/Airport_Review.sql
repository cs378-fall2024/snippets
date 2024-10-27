with int_Airport_Review as (
	select r.id, r.thread_id, r.icao, r.date_created, r.author, r.subject, r.body, s.relevance, s.sentiment
	from {{ ref('tmp_airport_reviews') }} r
	left join {{ ref('tmp_airport_reviews_with_sentiment') }} s
	on r.id = s.id
)

select *
from int_Airport_Review

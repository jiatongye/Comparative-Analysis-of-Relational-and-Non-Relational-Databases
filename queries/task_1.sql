WITH user_review_states AS (
    SELECT r.user_id, b.state, AVG(r.stars) AS avg_rating
    FROM "Review" r
    JOIN "Business" b ON r.business_id = b.business_id
    GROUP BY r.user_id, b.state
),
user_states_count AS (
    SELECT user_id, COUNT(DISTINCT state) AS state_count, AVG(avg_rating) AS overall_avg_rating
    FROM user_review_states
    GROUP BY user_id
)
SELECT u.user_id, u.name, state_count, overall_avg_rating
FROM user_states_count usc
JOIN "User" u ON usc.user_id = u.user_id
WHERE state_count >= 3 AND overall_avg_rating > 3.5;
WITH user_votes AS (
    SELECT r.user_id, b.state, AVG(r.useful) AS avg_useful_votes, COUNT(*) AS review_count
    FROM "Review" r
    JOIN "Business" b ON r.business_id = b.business_id
    GROUP BY r.user_id, b.state
),
ranked_users AS (
    SELECT state, user_id, avg_useful_votes, review_count,
           RANK() OVER (PARTITION BY state ORDER BY avg_useful_votes DESC) AS rank
    FROM user_votes
    WHERE review_count > 10
)
SELECT state, user_id, avg_useful_votes
FROM ranked_users
WHERE rank <= 5;




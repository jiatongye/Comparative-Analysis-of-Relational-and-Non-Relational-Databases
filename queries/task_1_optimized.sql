CREATE INDEX idx_business_id_state ON "Business" (business_id, state);
CREATE INDEX idx_review_user_business_id ON "Review" (user_id, business_id);
CREATE INDEX idx_user_user_id ON "User" (user_id);

WITH user_review_states AS (
    SELECT r.user_id, b.state, AVG(r.stars) AS avg_rating
    FROM "Review" r
    JOIN "Business" b ON r.business_id = b.business_id
    WHERE r.stars > 3.5
    GROUP BY r.user_id, b.state
),
user_states_count AS (
    SELECT user_id, COUNT(DISTINCT state) AS state_count, AVG(avg_rating) AS overall_avg_rating
    FROM user_review_states
    GROUP BY user_id
    HAVING COUNT(DISTINCT state) >= 3 AND AVG(avg_rating) > 3.5 
)
SELECT u.user_id, u.name, state_count, overall_avg_rating
FROM user_states_count usc
JOIN "User" u ON usc.user_id = u.user_id;

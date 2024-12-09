SET work_mem = '256MB';

WITH review_bins AS (
    SELECT 
        user_id,
        COUNT(*) AS num_reviews,
        AVG(stars) AS avg_rating
    FROM "Review" AS reviews
    GROUP BY user_id
),
ranked_reviews AS (
    SELECT 
        user_id,
        num_reviews,
        avg_rating,
        NTILE(3) OVER (ORDER BY num_reviews) AS review_count_category_rank
    FROM review_bins
),
categorized_reviews AS (
    SELECT 
        CASE 
            WHEN review_count_category_rank = 1 THEN 'Low Review Count'
            WHEN review_count_category_rank = 2 THEN 'Medium Review Count'
            ELSE 'High Review Count'
        END AS review_count_category,
        num_reviews,
        avg_rating
    FROM ranked_reviews
)
SELECT 
    review_count_category,
    COUNT(*) AS num_users,
    AVG(avg_rating) AS avg_category_rating,
    COALESCE(CORR(avg_rating, num_reviews), 0) AS correlation_coefficient
FROM categorized_reviews
GROUP BY review_count_category
ORDER BY review_count_category;

WITH word_counts AS (
    SELECT unnest(string_to_array(lower(review.text), ' ')) AS word
    FROM "Review" as review
)
SELECT word, COUNT(*) AS frequency
FROM word_counts
WHERE word NOT IN ('the', 'is', 'and', 'a', 'to', 'of')
GROUP BY word
ORDER BY frequency DESC
LIMIT 5;



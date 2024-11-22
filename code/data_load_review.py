import json
import psycopg2

dbname = "myinner_db"
user = "mynonsuperuser"

def load_review_json_to_psql(json_file_path, table_name):
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(f"dbname={dbname} user={user}")
        cur = conn.cursor()

        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                review_id VARCHAR(22) PRIMARY KEY,
                user_id VARCHAR(22) REFERENCES "User" (user_id) ON DELETE CASCADE,
                business_id VARCHAR(22) REFERENCES "Business" (business_id) ON DELETE CASCADE,
                text TEXT,
                stars INTEGER,
                date DATE
            );
        ''')

        with open(json_file_path, 'r') as file:
            data_to_insert = []

            for line_number, line in enumerate(file, 1):
                try:
                    data = json.loads(line.strip())
                    
                    user_id = data.get('user_id')
                    cur.execute('SELECT 1 FROM "User" WHERE user_id = %s;', (user_id,))
                    if cur.fetchone() is None:
                        print(f"Skipping review on line {line_number}, user_id {user_id} does not exist.")
                        continue

                    data_to_insert.append({
                        'review_id': data.get('review_id'),
                        'user_id': user_id,
                        'business_id': data.get('business_id'),
                        'text': data.get('text'),
                        'stars': data.get('stars'),
                        'date': data.get('date')
                    })

                    if len(data_to_insert) >= 1000:
                        cur.executemany(f'''
                            INSERT INTO "{table_name}" (
                                review_id, user_id, business_id, text, stars, date
                            ) VALUES (
                                %(review_id)s, %(user_id)s, %(business_id)s, %(text)s, %(stars)s, %(date)s
                            )
                            ON CONFLICT (review_id) DO NOTHING;
                        ''', data_to_insert)
                        conn.commit()
                        data_to_insert = []

                except json.JSONDecodeError:
                    print(f"Error decoding JSON on line {line_number}, skipping line.")
                except Exception as e:
                    print(f"Error processing line {line_number}: {e}")
                    if conn:
                        conn.rollback()

            if data_to_insert:
                cur.executemany(f'''
                    INSERT INTO "{table_name}" (
                        review_id, user_id, business_id, text, stars, date
                    ) VALUES (
                        %(review_id)s, %(user_id)s, %(business_id)s, %(text)s, %(stars)s, %(date)s
                    )
                    ON CONFLICT (review_id) DO NOTHING;
                ''', data_to_insert)
                conn.commit()

        print(f"Data from {json_file_path} loaded successfully.")

    except Exception as e:
        print(f"Error connecting to the database or inserting data: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            try:
                conn.commit()
            except Exception as e:
                print(f"Error committing transaction: {e}")
        if cur:
            cur.close()
        if conn:
            conn.close()

def main():
    json_file_path = '/Users/michelle/Desktop/finalproj101/data/yelp_dataset/yelp_academic_dataset_review.json'
    table_name = 'Review'

    load_review_json_to_psql(json_file_path, table_name)

if __name__ == "__main__":
    main()

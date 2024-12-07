import psycopg2
import json

dbname = "myinner_db"
user = "mynonsuperuser"

def load_user_json_to_psql(json_file_path, table_name):
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(f"dbname={dbname} user={user}")
        cur = conn.cursor()

        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                user_id VARCHAR(22) PRIMARY KEY,
                name VARCHAR,
                review_count INTEGER,
                yelping_since DATE,
                average_stars FLOAT
            );
        ''')

        with open(json_file_path, 'r') as file:
            data_to_insert = []

            for line_number, line in enumerate(file, 1):
                try:
                    data = json.loads(line.strip())

                    data_to_insert.append({
                        'user_id': data.get('user_id'),
                        'name': data.get('name'),
                        'review_count': data.get('review_count'),
                        'yelping_since': data.get('yelping_since'),
                        'average_stars': data.get('average_stars')
                    })

                    if len(data_to_insert) >= 1000:
                        cur.executemany(f'''
                            INSERT INTO "{table_name}" (
                                user_id, name, review_count, yelping_since, average_stars
                            ) VALUES (
                                %(user_id)s, %(name)s, %(review_count)s, %(yelping_since)s, %(average_stars)s
                            )
                            ON CONFLICT (user_id) DO NOTHING;
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
                        user_id, name, review_count, yelping_since, average_stars
                    ) VALUES (
                        %(user_id)s, %(name)s, %(review_count)s, %(yelping_since)s, %(average_stars)s
                    )
                    ON CONFLICT (user_id) DO NOTHING;
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
    json_file_path = '/Users/claudiawoo/data_101_finalproj/data/yelp_academic_dataset_user.json'
    table_name = 'User'

    load_user_json_to_psql(json_file_path, table_name)

if __name__ == "__main__":
    main()

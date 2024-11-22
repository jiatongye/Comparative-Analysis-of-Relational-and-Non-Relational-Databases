import psycopg2
import json

dbname = "myinner_db"
user = "mynonsuperuser"

import json
import psycopg2

def load_json_to_psql(json_file_path, table_name):
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(f"dbname={dbname} user={user}")
        cur = conn.cursor()

        cur.execute(f'''
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                business_id VARCHAR(22) PRIMARY KEY,
                name TEXT NOT NULL,
                address TEXT,
                city TEXT,
                state TEXT,
                postal_code TEXT,
                latitude FLOAT,
                longitude FLOAT,
                stars FLOAT,
                review_count INTEGER,
                is_open BOOLEAN,
                attributes JSONB,
                categories TEXT[],
                hours JSONB
            );
        ''')

        with open(json_file_path, 'r') as file:
            data_to_insert = []

            for line_number, line in enumerate(file, 1):
                try:
                    data = json.loads(line.strip())

                    categories_list = data.get('categories', '').split(', ') if isinstance(data.get('categories'), str) else []

                    data_to_insert.append({
                        'business_id': data.get('business_id'),
                        'name': data.get('name'),
                        'address': data.get('address'),
                        'city': data.get('city'),
                        'state': data.get('state'),
                        'postal_code': data.get('postal_code'),
                        'latitude': data.get('latitude'),
                        'longitude': data.get('longitude'),
                        'stars': data.get('stars'),
                        'review_count': data.get('review_count'),
                        'is_open': bool(data.get('is_open')),
                        'attributes': json.dumps(data.get('attributes', {})), 
                        'categories': categories_list, 
                        'hours': json.dumps(data.get('hours', {}))
                    })

                    if len(data_to_insert) >= 1000:
                        cur.executemany(f'''
                            INSERT INTO "{table_name}" (
                                business_id, name, address, city, state, postal_code, latitude, longitude,
                                stars, review_count, is_open, attributes, categories, hours
                            ) VALUES (
                                %(business_id)s, %(name)s, %(address)s, %(city)s, %(state)s, %(postal_code)s,
                                %(latitude)s, %(longitude)s, %(stars)s, %(review_count)s, %(is_open)s,
                                %(attributes)s, %(categories)s, %(hours)s
                            )
                            ON CONFLICT (business_id) DO NOTHING;
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
                        business_id, name, address, city, state, postal_code, latitude, longitude,
                        stars, review_count, is_open, attributes, categories, hours
                    ) VALUES (
                        %(business_id)s, %(name)s, %(address)s, %(city)s, %(state)s, %(postal_code)s,
                        %(latitude)s, %(longitude)s, %(stars)s, %(review_count)s, %(is_open)s,
                        %(attributes)s, %(categories)s, %(hours)s
                    )
                    ON CONFLICT (business_id) DO NOTHING;
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
    json_file_path = '/Users/michelle/Desktop/finalproj101/data/yelp_dataset/yelp_academic_dataset_business.json'
    table_name = 'Business'

    load_json_to_psql(json_file_path, table_name)

if __name__ == "__main__":
    main()

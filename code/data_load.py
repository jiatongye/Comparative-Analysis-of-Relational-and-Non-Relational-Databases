import psycopg2
import json
import os

dbname = "myinner_db"
user = "mynonsuperuser"

def load_json_to_psql(json_file_path, table_name):
    conn = psycopg2.connect(f"dbname={dbname} user={user}")
    cur = conn.cursor()

    with open(json_file_path, 'r') as file:
        for line in file:
            data = json.loads(line) 
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" (id SERIAL PRIMARY KEY, data JSONB NOT NULL);')
        cur.execute('INSERT INTO "' + table_name + '" (data) VALUES (%s)', [json.dumps(data)])
        conn.commit()

    cur.close()
    conn.close()

def main():
    dir_path = os.path.dirname(os.path.abspath(__file__))
    json_dir = os.path.join(dir_path, '..', 'data')

    for file in os.listdir(json_dir):
        if file.endswith('.json'):
            table_name = file.replace('yelp_academic_dataset_', '').replace('.json', '')
            json_file_path = os.path.join(json_dir, file)

            load_json_to_psql(json_file_path, table_name)

if __name__ == "__main__":
    main()

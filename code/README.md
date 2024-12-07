Initiating PostgreSQL - Run these commands first

conda create --name myenv

conda activate myenv

conda install -y -c conda-forge postgresql

initdb -D mylocal_db

pg_ctl -D mylocal_db -l logfile start

createuser --encrypted --pwprompt mynonsuperuser

createdb --owner=mynonsuperuser myinner_db

conda install -c conda-forge psycopg2

python code/data_load.py: load json to postgres

psql -U mynonsuperuser -d myinner_db -h localhost

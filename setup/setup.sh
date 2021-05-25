docker-compose -f docker-compose.yml down
docker-compose -f docker-compose.yml rm --force -v
docker-compose -f docker-compose.yml up --force-recreate -d

sleep 30

PGPASSWORD=airflow
psql -h localhost -p 5432 -U airflow -d airflow -f db.sql

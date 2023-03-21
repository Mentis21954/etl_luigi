# example ETL workflow with pandas and luigi

Running the Luigi Scheduler (http://localhost:8082/)
- sudo ufw allow 8082/tcp
- luigid --port 8082 > /dev/null 2> /dev/null &
- python run.py
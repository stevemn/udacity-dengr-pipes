echo "Stopping Airflow components"
ps -eo 'tty,pid,comm' |grep 'airflow webserver -p 8080 -D' | sed -nE 's/\?\? +([0-9]+) +.*/\1/p' | xargs kill
ps -eo 'tty,pid,comm' |grep 'airflow scheduler -D' | sed -nE 's/\?\? +([0-9]+) +.*/\1/p' | xargs kill
echo "Airflow stopped"
echo "Shutting down cluster"
python cluster_ctrl.py stop pipeline.ini
echo "Cluster shutdown"
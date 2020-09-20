ps -eo 'tty,pid,comm' |grep 'airflow webserver -p 8080 -D' | sed -nE 's/\?\? +([0-9]+) +.*/\1/p' | xargs kill
ps -eo 'tty,pid,comm' |grep 'airflow scheduler -D' | sed -nE 's/\?\? +([0-9]+) +.*/\1/p' | xargs kill
python cluster_ctrl.py stop
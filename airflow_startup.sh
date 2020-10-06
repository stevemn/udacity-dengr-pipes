echo "Begin Cluster kickoff process."
python cluster_ctrl.py start pipeline.ini
echo "Cluster started"

echo "Setting environment"
AWS_ID=$(python ini_reader.py pipeline.ini AF_CONN_AWS id)
AWS_TYPE=$(python ini_reader.py pipeline.ini AF_CONN_AWS type)
AWS_LOGIN=$(python ini_reader.py pipeline.ini AF_CONN_AWS login)
AWS_PASSW=$(python ini_reader.py pipeline.ini AF_CONN_AWS password)

RS_ID=$(python ini_reader.py pipeline.ini AF_CONN_REDSHIFT id)
RS_TYPE=$(python ini_reader.py pipeline.ini AF_CONN_REDSHIFT type)
RS_LOGIN=$(python ini_reader.py pipeline.ini AF_CONN_REDSHIFT login)
RS_PASSW=$(python ini_reader.py pipeline.ini AF_CONN_REDSHIFT password)
RS_HOST=$(python ini_reader.py pipeline.ini AF_CONN_REDSHIFT host)
RS_SCHEMA=$(python ini_reader.py pipeline.ini AF_CONN_REDSHIFT schema)
RS_PORT=$(python ini_reader.py pipeline.ini AF_CONN_REDSHIFT port)
echo "Environment set"

echo "Starting Airflow"
airflow webserver -p 8080 -D
airflow scheduler -D
echo "Airflow started"

echo "Setting AWS connection"
airflow connections --delete --conn_id $AWS_ID 
airflow connections --add --conn_id $AWS_ID --conn_type "$AWS_TYPE" --conn_login $AWS_LOGIN --conn_password $AWS_PASSW
echo "AWS connection set"


echo "Setting Redshift connection"
airflow connections --delete --conn_id $RS_ID
airflow connections --add --conn_id $RS_ID --conn_type $RS_TYPE --conn_login $RS_LOGIN --conn_password $RS_PASSW --conn_host $RS_HOST --conn_schema $RS_SCHEMA --conn_port $RS_PORT
echo "Redshift connection set"

echo "Cluster kickoff complete"
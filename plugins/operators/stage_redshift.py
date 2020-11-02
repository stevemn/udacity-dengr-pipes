from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    DROP_SQL = "DELETE FROM {}"

    COPY_SQL = """
        COPY {}
        FROM 's3://udacity-dend/{}'
        ACCESS_KEY_ID '{{}}'
        SECRET_ACCESS_KEY '{{}}'
        FORMAT AS JSON '{}'
        region 'us-west-2'
    """
    
    @apply_defaults
    def __init__(self, table, awsConn, redshiftConn, s3Path, jsonFormat='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.aws_conn_id = awsConn
        self.redshift_conn_id = redshiftConn
        self.s3_path = s3Path
        self.json_format = jsonFormat
        self.drop_query = StageToRedshiftOperator.DROP_SQL.format(table)
        self.copy_query = StageToRedshiftOperator.COPY_SQL.format(
            table, s3Path, jsonFormat)

    def execute(self, context):
        aws = AwsHook(self.aws_conn_id)
        aws_credentials = aws.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(self.drop_query)
        redshift.run(self.drop_query)
        self.log.info(self.copy_query)
        redshift.run(self.copy_query.format(aws_credentials.access_key,
                                           aws_credentials.secret_key))
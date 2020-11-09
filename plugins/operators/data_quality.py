from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    CHECK_SQL = '''
        SELECT COUNT(*) FROM public.{}
    '''

    @apply_defaults
    def __init__(self, redshiftConn, tables, *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshiftConn
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            records = redshift.get_records(DataQualityOperator.CHECK_SQL.format(table))
            if len(records) < 1 or len(records[0]) < 1:
                self.log.warning(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                self.log.warning(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        self.log.info(f"Data quality verified for all tables")
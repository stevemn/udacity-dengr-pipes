from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    DELETE_SQL = """
        DELETE FROM {}
    """

    INSERT_SQL = """
        INSERT INTO public.{}
        {}
    """

    @apply_defaults
    def __init__(self, redshiftConn, table, query, appendData=False,
        *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshiftConn
        self.table = table
        self.overwrite = not appendData
        self.drop_query = LoadDimensionOperator.DELETE_SQL.format(table)
        self.insert_query = LoadDimensionOperator.INSERT_SQL.format(table, query)

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.overwrite:
            self.log.info(self.drop_query)
            redshift.run(self.drop_query)
        self.log.info(self.insert_query)
        redshift.run(self.insert_query)

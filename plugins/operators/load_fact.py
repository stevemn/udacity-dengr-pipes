from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    DROP_SQL = "DELETE FROM {}"

    INSERT_SQL = """
        INSERT INTO public.{}
        {}
    """

    @apply_defaults
    def __init__(self, redshiftConn, table, insertSelect, *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshiftConn
        self.drop_query = LoadFactOperator.DROP_SQL.format(table)
        self.insert_query = LoadFactOperator.INSERT_SQL.format(
            table, insertSelect)

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(self.drop_query)
        redshift.run(self.drop_query)
        self.log.info(self.insert_query)
        redshift.run(self.insert_query)

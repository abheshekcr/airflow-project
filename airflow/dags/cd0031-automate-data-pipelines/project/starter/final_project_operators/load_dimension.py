from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from udacity.common import final_project_sql_statements

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                table="",
                sql_query="",
                truncate="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table= table
        self.sql_query=sql_query
        self.truncate=truncate
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(final_project_sql_statements.user_table_create)
        redshift.run(final_project_sql_statements.song_table_create)
        redshift.run(final_project_sql_statements.artist_table_create)
        redshift.run(final_project_sql_statements.time_table_create)

        if self.truncate:
            redshift.run(f"truncate table {self.table}")

        dim_table_insert=f"insert into {self.table} ({self.sql_query})"
        self.log.info("loading dim table")
        redshift.run(dim_table_insert)

        

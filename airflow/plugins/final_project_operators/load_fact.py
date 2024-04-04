from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from udacity.common import final_project_sql_statements

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    facts_sql_template="""
    DROP TABLE IF EXISTS {destination_table};
    CREATE TABLE {destination_table} AS
    SELECT
     md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM {origin_table1}
            WHERE page='NextSong') events
            LEFT JOIN {origin_table2} songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """
    

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                origin_table1="",
                origin_table2="",
                destination_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.origin_table1 = origin_table1
        self.origin_table2=origin_table2
        self.destination_table=destination_table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)


        formatted_sql = LoadFactOperator.facts_sql_template.format(
            origin_table1=self.origin_table1,
            origin_table2=self.origin_table2,
            destination_table=self.destination_table
        )

        self.log.info("loading fact table")
        redshift.run(formatted_sql)




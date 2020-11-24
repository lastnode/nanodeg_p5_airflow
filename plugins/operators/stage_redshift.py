from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_folder="",
                 region=""
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_folder = s3_folder
        self.region = region        

    def execute(self, context):
        
        self.log.info("Starting COPY to Redshift.")

        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_folder)
        
        self.log.info(s3_path)
        
        self.log.info("Copying staging_events table.")
        
        staging_events_copy = ("""
        copy staging_events from {}
        access_key_id {}
        secret_access_key {}
        region {}
        timeformat as 'epochmillisecs'
        blanksasnull
        emptyasnull
        region 'us-west-2';
        """).format(s3_path, aws_credentials.access_key, aws_credentials.secret_key, self.region)
        
        self.log.info(staging_events_copy)







from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = "#358140"

    @apply_defaults
    def __init__(
        self,
        s3_bucket,
        s3_prefix,
        table,
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        copy="",
        *args,
        **kwargs,
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy = copy

    def execute(self):
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")

        self.log.info(
            f"Preparing staging from s3://{self.s3_bucket}/{self.s3_prefix} to {self.table} table."
        )

        self.log.info("Copying")

        copyQueries = """
            COPY {table}
            FROM 's3://{s3_bucket}/{s3_prefix}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            {copy};
        """.format(
            table=self.table,
            s3_bucket=self.s3_bucket,
            s3_prefix=self.s3_prefix,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            copy=self.copy,
        )

        redshift_hook.run(copyQueries)
        self.log.info("Copying complete.")

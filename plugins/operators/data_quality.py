from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, redshift_conn_id="", tables=[], *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            allRecords = redshift_hook.get_allRecords(f"SELECT COUNT(*) FROM {table}")
            self.log.info(f"Data Quality Check: {table} table")

            if len(allRecords) < 1:
                raise ValueError(
                    f"Data quality check failed. Reason: {table} shows no results"
                )
            elif len(allRecords[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. Reason: {table} shows no results"
                )
            else:
                self.log.info(
                    f"Data quality check passed. {table} shows {allRecords[0][0]} Records"
                )

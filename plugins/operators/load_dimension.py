from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self,
        table,
        redshift_conn_id="redshift",
        clear_data=True,
        sql="",
        *args,
        **kwargs,
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.clear_data = clear_data

    def execute(self):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.clear_data:
            self.log.info(f"Clearing data from {self.table} table")
            redshift_hook.run(f"DELETE FROM {self.table}")
            self.log.info(f"Cleared {self.table} table")

        self.log.info("Inserting into {self.table} table")

        redshift_hook.run(
            f"""INSERT INTO {self.table}
                {self.sql}
            """
        )

        self.log.info("Inserted into {self.table} table")

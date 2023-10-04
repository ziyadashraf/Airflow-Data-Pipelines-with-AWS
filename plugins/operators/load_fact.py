from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = "#F98866"

    @apply_defaults
    def __init__(self, redshift_conn_id="", table="", stmt="", *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.stmt = stmt

    def execute(self):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Fact Table Loading: {self.table}")

        insert = """INSERT INTO {} 
                    {}; 
        COMMIT;""".format(
            self.table, self.stmt
        )

        self.log.info(f"Fact Table {self.table} Loaded.")

        redshift.run(insert)

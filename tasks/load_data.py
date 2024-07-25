import luigi
import pandas as pd

import sys
import os
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), '..'
        )
    )
)
import utils as u
from .transform_data import TransformDataTask

class LoadDataTask(luigi.Task):

    def requires(self):
        return TransformDataTask()

    def output(self):
        pass

    def run(self):
        print("\033[1m>>>>> STEP 6 : LOAD DATA TO DATA WAREHOUSE \033[0m")
        
        # Read raw data
        transformed_data = pd.read_pickle(self.input().path)

        # Insert to database
        with u.createConnections() as conn:
            transformed_data.to_sql(
                name="close_encounters",
                con=conn,
                if_exists='replace'
            )
import luigi
from luigi.parameter import ParameterVisibility
import pandas as pd
import requests

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
from commons.constants import Constants
import utils as u

class ExtractCloseEncountersDataTask(luigi.Task):

    output_dir = luigi.Parameter('./temp/raw_data.pkl', visibility=ParameterVisibility.HIDDEN)
    url = luigi.Parameter(Constants.CAD_URL)

    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget(self.output_dir)

    def run(self):
        print("\033[1m>>>>> STEP 1 : GETTING RAW DATA \033[0m")
        
        # Get the data and check status code
        response = requests.get(self.url)
        print("Status code: ", response.status_code)
        if int(response.status_code) != 200:
            return None # return None if no result
        
        # Convert into pandas dataframe
        raw_data = pd.DataFrame(
            data=response.json()["data"],
            columns=response.json()["fields"]
        )
        raw_data.to_pickle(self.output().path)

        # Validate data
        u.getDataValidation(raw_data)

        return True
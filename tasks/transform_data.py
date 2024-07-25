import luigi
from luigi.parameter import ParameterVisibility
import pandas as pd
import numpy as np
from datetime import datetime

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
from .extract_data import ExtractCloseEncountersDataTask

class SelectFieldsDataTask(luigi.Task):

    output_dir = luigi.Parameter('./temp/selected_data.pkl', visibility=ParameterVisibility.HIDDEN)
    fields = luigi.Parameter(Constants.SELECTED_FIELDS)

    def requires(self):
        return ExtractCloseEncountersDataTask()

    def output(self):
        return luigi.LocalTarget(self.output_dir)

    def run(self):
        print("\033[1m>>>>> STEP 2 : SELECT SOME FIELDS FROM RAW DATA \033[0m")
        
        # Read raw data
        raw_data = pd.read_pickle(self.input().path)

        # Select & rename fields
        selected_data = (
            raw_data
            [self.fields.keys()]
            .rename(columns=self.fields)
        )
        selected_data.to_pickle(self.output().path)

        # Validate data
        u.getDataValidation(selected_data)
        
        return True
    
class FormatFieldsTypeTask(luigi.Task):

    output_dir = luigi.Parameter('./temp/formatted_data.pkl', visibility=ParameterVisibility.HIDDEN)

    def requires(self):
        return SelectFieldsDataTask()

    def output(self):
        return luigi.LocalTarget(self.output_dir)

    def run(self):
        print("\033[1m>>>>> STEP 3 : FORMAT COLUMNS TYPES \033[0m")
        
        # Read selected data
        selected_data = pd.read_pickle(self.input().path)

        # General formatting
        formatted_data = selected_data
        for col in formatted_data.columns:
            if col in Constants.DATA_REQUIREMENTS["fields"].keys():
                if Constants.DATA_REQUIREMENTS["fields"][col] not in [np.dtypes.DateTime64DType, np.dtypes.TimeDelta64DType]:
                    formatted_data[col] = formatted_data[col].astype(Constants.DATA_REQUIREMENTS["fields"][col]())
        # Adhoc formatting
        formatted_data["close_approach_time"] = pd.to_datetime(formatted_data["close_approach_time"], format='%Y-%b-%d %H:%M')
        formatted_data.to_pickle(self.output().path)

        # Validate data
        u.getDataValidation(formatted_data)
        
        return True
    
class CleanDataTask(luigi.Task):

    output_dir = luigi.Parameter('./temp/cleaned_data.pkl', visibility=ParameterVisibility.HIDDEN)

    def requires(self):
        return FormatFieldsTypeTask()

    def output(self):
        return luigi.LocalTarget(self.output_dir)

    def run(self):
        print("\033[1m>>>>> STEP 4 : CLEAN DATA FROM MISSING VALUES AND DUPLICATES \033[0m")
        
        # Read selected data
        formatted_data = pd.read_pickle(self.input().path)

        # General formatting
        cleaned_data = formatted_data
        # Drop na
        cleaned_data = cleaned_data.dropna()
        # Drop duplicates and keep first value
        cleaned_data = cleaned_data.drop_duplicates(keep="first")

        cleaned_data.to_pickle(self.output().path)

        # Validate data
        u.getDataValidation(cleaned_data)
        
        return True
    
class TransformDataTask(luigi.Task):

    date = datetime.now().date()
    output_dir = luigi.Parameter(f'./temp/transformed_data_{date}.pkl', visibility=ParameterVisibility.HIDDEN)

    def requires(self):
        return CleanDataTask()

    def output(self):
        return luigi.LocalTarget(self.output_dir)

    def run(self):
        print("\033[1m>>>>> STEP 5 : TRANSFORM DATA TO READY-TO-LOAD DATA \033[0m")
        
        # Read selected data
        cleaned_data = pd.read_pickle(self.input().path)

        transformed_data = cleaned_data
        # Change distances unit from au -> km
        DISTANCE_COLUMNS = ["distance", "minimum_distance", "maximum_distance"]
        DISTANCE_CONVERSION = 1_500_000 # 1 au = 1.5 million km
        for col in DISTANCE_COLUMNS: transformed_data[col] = transformed_data[col].multiply(DISTANCE_CONVERSION)
        
        transformed_data.to_pickle(self.output().path)
        
        return True
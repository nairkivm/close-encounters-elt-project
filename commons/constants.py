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

class Constants:
    CAD_URL = "https://ssd-api.jpl.nasa.gov/cad.api?date-min=2019-01-01&date-max=2023-12-31&diameter=True"
    DATA_REQUIREMENTS = {
        'fields':{
            'name': np.dtypes.ObjectDType,
            'close_approach_time': np.dtypes.DateTime64DType,
            'distance': np.dtypes.Float64DType,
            'minimum_distance': np.dtypes.Float64DType,
            'maximum_distance': np.dtypes.Float64DType,
            'relative_velocity': np.dtypes.Float64DType,
            'relative_velocity_at_massless_earth': np.dtypes.Float64DType,
            't_sigma_f': np.dtypes.ObjectDType,
            'absolute_magnitude': np.dtypes.Float64DType,
            'diameter': np.dtypes.Float64DType,
            'diameter_sigma': np.dtypes.ObjectDType
        }
    }
    SELECTED_FIELDS = {
        'des':'name',
        'cd':'close_approach_time',
        'dist':'distance',
        'dist_min':'minimum_distance',
        'dist_max':'maximum_distance',
        'v_rel':'relative_velocity',
        'v_inf':'relative_velocity_at_massless_earth',
        't_sigma_f':'t_sigma_f',
        'h':'absolute_magnitude',
        'diameter':'diameter',
        'diameter_sigma':'diameter_sigma'
    }
    FILL_VALUES = {
        'name':'-',
        'close_approach_time':datetime(1990,1,1),
        'distance':-1,
        'dist_min':-1,
        'dist_max':-1,
        'v_rel':-1,
        'v_inf':-1,
        't_sigma_f':'-',
        'h':-99999,
        'diameter':-1,
        'diameter_sigma':'-'
    }


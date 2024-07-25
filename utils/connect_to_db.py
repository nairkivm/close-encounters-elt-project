from sqlalchemy import create_engine 

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

def createConnections():
    conn_string = f"postgresql://{os.environ['DATABASE_USER']}:{os.environ['DATABASE_PASSWORD']}@{os.environ['DATABASE_HOST']}/{os.environ['DATABASE_NAME']}"
    db_engine = create_engine(conn_string) 
    conn = db_engine.connect()
    return conn 
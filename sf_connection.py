import snowflake.connector
import os

class SF_Connection:
    def open_conn(self):
        conn = snowflake.connector.connect(
            account='mp55757.west-europe.azure',
            user= os.environ['sf_user'],
            password = os.environ['sf_password'],
            warehouse='SELECTOR',
            database='PROD_TEAM',
            schema='MFO',
            role='MFO',
        )
        engine = conn.cursor()
        return engine
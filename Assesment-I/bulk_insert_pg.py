import io
import logging
import traceback
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine
from models import Base
from sqlalchemy import text
import os
def get_db_connection():
    """
    The get_db_connection function creates a connection to the database.
        Args:
            None

    :return: An object of type sqlalchemy
    :doc-author: prabhakarant
    """
    db_name="public"  ##can be pulled from a config file
    user="postgres"
    host_name="localhost"
    port='5432'
    db_password="root@123"
    hashed_pass=urllib.parse.quote_plus(db_password)
    engine=None
    try:
        engine=create_engine(f'postgresql+psycopg2://{user}:{hashed_pass}@{host_name}:{port}/{db_name}')
    except Exception as e:
        print("error_occured",e)
        logging.debug(f"DB Connection error occured:{e}")
    return engine


def create_tables(db_engine):
    """
    The create_tables function creates the tables in the database.
        Args:
            db_engine (object): The SQLAlchemy engine object created by create_engine() function.

    :param db_engine: Create the tables in the database
    :return: Nothing
    :doc-author: prabhakarant
    """
    try:
        Base.metadata.create_all(db_engine)

    except Exception as e:
        print(e)

def copy_csv(engine,csv_path,mode=1):

    """
    The copy_csv function copies a csv file into the database.
        Args:
            engine (sqlalchemy.engine): The sqlalchemy engine to use for connecting to the database.
            csv_path (str): The path of the CSV file to copy into the database table test_od

    :param engine: Connect to the database
    :param csv_path: Specify the path of the csv file to be copied
    :param mode: Determine which method to use for copying the csv file into the database
    :return: The number of rows inserted into the table
    :doc-author: prabhakarant
    """
    if mode==1:
        try:
            with engine.connect() as connection:
                pg_cur=connection.connection.cursor()

                ##can also use SELECT * FROM pg_indexes WHERE tablename = 'tablename'
                # to store column names of the indexes in table
                csv_file=open(csv_path,'r')
                connection.execute(text("ALTER TABLE test_od SET UNLOGGED"))  ##increases performance significantly when not logged
                connection.execute(text("ALTER TABLE test_od DISABLE TRIGGER ALL"))## disable foreignkey checks (not recommended but can increase performance)
                connection.commit()
                pg_cur.copy_expert(f"COPY test_od FROM stdin WITH (FORMAT CSV, HEADER TRUE)",csv_file)## client sided sql command to copy records from csv
                connection.commit()
                connection.execute(text(f"ALTER TABLE test_od ENABLE TRIGGER ALL"))
                connection.execute(text("ALTER TABLE test_od SET LOGGED"))
                connection.commit()
        except Exception as e:
            logging.error(f"SQL Error occurred : {e}")
        except FileNotFoundError as e:
            logging.error(f"File Error occurred : {e}")
    else:
        try:
            with engine.connect() as connection:
                ##can also use SELECT * FROM pg_indexes WHERE tablename = 'tablename'
                # to store column names of the indexes in table
                df= pd.read_csv(csv_path)

                connection.execute(text("ALTER TABLE test_od SET UNLOGGED"))
                connection.execute(text("ALTER TABLE test_od DISABLE TRIGGER ALL"))
                connection.commit()
                df.to_sql('test_od', engine, index=False, if_exists="append")
                connection.commit()
                connection.execute(text(f"ALTER TABLE test_od ENABLE TRIGGER ALL"))
                connection.execute(text("ALTER TABLE test_od SET LOGGED"))
                connection.commit()
        except:
            traceback.print_exc()
engine=get_db_connection()
create_tables(engine)
copy_csv(engine,"/Users/prabhakarant/Downloads/bulk_data1.csv",mode=1)

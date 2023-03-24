import urllib
from sqlalchemy import create_engine, MetaData
db_name="public"  ##can be pulled from a config file
user="postgres"
host_name="localhost"
port='5432'
db_password="root@123"
hashed_pass=urllib.parse.quote_plus(db_password)

engine = create_engine(f'postgresql+psycopg2://{user}:{hashed_pass}@{host_name}:{port}/{db_name}')
def get_custom_schema_engine(schema):
    """
    The get_custom_schema_engine function takes a schema name as an argument and returns a SQLAlchemy engine object
    that is connected to the specified schema. This function is useful for connecting to schemas that are not in the
    default database.

    :param schema: Specify the name of the schema to be used
    :return: An engine object that connects to the database schema
    :doc-author: prabhakarant
    """
    return create_engine(f'postgresql+psycopg2://{user}:{hashed_pass}@{host_name}:{port}/{schema}')
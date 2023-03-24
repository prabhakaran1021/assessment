import io

from flask import Flask
from flask import request
from datetime import datetime
import pandas as pd
from sqlalchemy import text

from db.extensions import get_custom_schema_engine
app = Flask(__name__)
@app.route('/file-import',methods=['post'])
def upload():
    file=request.files.get('file')
    schema=request.form.get('schema')
    user_id=request.form.get('create_user_id')
    response={}
    if file.filename.endswith('.csv'):
        try:
            df=pd.read_csv(file.stream)
            create_table_df=df.from_records(df.to_dict('records')[0:1])
            create_table_df.transpose()
            create_table_df['user_id']=user_id
            df=df.drop(create_table_df.index)
            df.transpose()
            df['user_id']=user_id
            today=datetime.today()
            time_stamp=today.strftime("%y_%m_%d_%H_%M_%S")
            table_prefix="master_study_list_"
            table_name=table_prefix + time_stamp
            engine = get_custom_schema_engine(schema)
            create_table_df.to_sql(table_name,engine,index=False)
            bytes_buf=io.BytesIO()
            df.to_csv(bytes_buf,index=False)
            bytes_buf.seek(0,0)
            with engine.connect() as connection:
                pg_cur = connection.connection.cursor()
                connection.execute(
                    text("ALTER TABLE test_od SET UNLOGGED"))  ##increases performance significantly when not logged
                connection.execute(text(
                    "ALTER TABLE test_od DISABLE TRIGGER ALL"))  ## disable foreignkey checks (not recommended but can increase performance)
                connection.commit()
                pg_cur.copy_expert(f"COPY {table_name} FROM stdin WITH (FORMAT CSV, HEADER TRUE)",
                                   bytes_buf)  ## client sided sql command to copy records from csv
                connection.commit()
                connection.execute(text(f"ALTER TABLE test_od ENABLE TRIGGER ALL"))
                connection.execute(text("ALTER TABLE test_od SET LOGGED"))
                connection.commit()
            response={"status":"success"}
        except Exception as e:
            response={"status":"failed","msg":f"Exception occurred : {e}"}
    return response

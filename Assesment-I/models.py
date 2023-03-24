from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,Integer,String
Base=declarative_base()

class Test_OD(Base):
    __tablename__='test_od'
    id=Column(Integer,primary_key=True)
    column1=Column(String(64))
    column2=Column(String(64))
    column3=Column(String(64))
    column4=Column(String(64))
    column5=Column(String(64))

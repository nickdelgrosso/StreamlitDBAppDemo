from datetime import datetime
from typing import Optional
from uuid import uuid4
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, UUID, DateTime
from repos.base import IPatientRepo


metadata = MetaData()
patients = Table('patients', metadata, 
    Column('id', String, primary_key=True),
    Column('created_on', DateTime),
    Column('name', String),
    Column('age', Integer),
)
                 

class SqlitePatientRepo(IPatientRepo):
    
    def __init__(self) -> None:
        self.engine = create_engine('sqlite:///patients.db')
        metadata.create_all(self.engine)
        self.conn = self.engine.connect()
        
    
    def save(self, name: str, age: int) -> None:
        self.conn.execute(patients.insert(), dict(id=str(uuid4()), created_on=datetime.now(), name=name, age=age))
        self.conn.commit()
        
    def get_all_patients(self) -> pd.DataFrame | None:
        df = pd.read_sql_table('patients', con=self.conn)
        return df
    
    def archive(self, id: str) -> None:
        self.conn.execute(patients.delete().where(patients.c.id == id))
        self.conn.commit()
    
    
    
if __name__ == '__main__':
    repo = SqlitePatientRepo()
    print("Before Run:")
    print(repo.get_all_patients())
    
    print("Adding another patient:")
    repo.save(name='Nick', age=35)
    df = repo.get_all_patients()
    print(df)
    
    print("Removing the last patient:")
    last_patient = df.iloc[-1]
    repo.archive(last_patient.id)
    print(repo.get_all_patients())
    
    repo.archive
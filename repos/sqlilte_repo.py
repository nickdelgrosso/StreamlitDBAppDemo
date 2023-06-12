from datetime import datetime
from typing import Optional
from uuid import uuid4
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, UUID, DateTime
from repos.base import IPatientRepo


                

class SqlitePatientRepo(IPatientRepo):
    
    def __init__(self, path: str) -> None:
        self.path = path
        self.engine = create_engine(f'sqlite:///{path}')
        
        metadata = MetaData()
        self.patients_table = Table('patients', metadata, 
            Column('id', String, primary_key=True),
            Column('created_on', DateTime),
            Column('name', String),
            Column('age', Integer),
        )
        self.metadata = metadata
        self.conn = self.engine.connect()
        
    def create_db(self) -> None:
        self.metadata.create_all(self.engine)
    
    def add_patient(self, name: str, age: int) -> None:
        self.conn.execute(self.patients_table.insert(), dict(id=str(uuid4()), created_on=datetime.now(), name=name, age=age))
        self.conn.commit()
        
    def get_all_patients(self) -> pd.DataFrame | None:
        return pd.read_sql_table('patients', con=self.conn)

    
    def archive_patient(self, id: str) -> None:
        patients = self.patients_table
        self.conn.execute(patients.delete().where(patients.c.id == id))
        self.conn.commit()
    
    
    
if __name__ == '__main__':
    repo = SqlitePatientRepo(path='patients.db')
    print("Before Run:")
    print(repo.get_all_patients())
    
    print("Adding another patient:")
    repo.add_patient(name='Nick', age=35)
    df = repo.get_all_patients()
    print(df)
    
    print("Removing the last patient:")
    last_patient = df.iloc[-1]
    repo.archive_patient(last_patient.id)
    print(repo.get_all_patients())
    
    repo.archive_patient
import contextlib
from datetime import datetime
import os
from typing import Optional
from uuid import uuid4
import pandas as pd
from sqlalchemy import Text, create_engine, MetaData, Table, Column, String, Integer, UUID, DateTime, delete, insert, join, select, text
from sqlalchemy_utils import create_view

try:
    from repos.base import IPatientRepo
except:
    from base import IPatientRepo


                

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
        
        self.sessions_table = Table('sessions', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('patient_id', String),
            Column('comment', String),
        )
        
        self.full_session_stmt = (
        select(
            self.sessions_table.c.id,
            self.patients_table.c.name, 
            self.patients_table.c.age, 
            self.sessions_table.c.comment,
        )
        .join_from(    

            self.patients_table, 
            self.sessions_table, 
            self.patients_table.c.id == self.sessions_table.c.patient_id
        )
        )
        
        create_view(name='sessions_full', selectable=self.full_session_stmt, metadata=metadata)
        
        self.metadata = metadata
        self.conn = self.engine.connect()
        
    def create_db(self) -> None:
        self.metadata.create_all(self.engine)
    
    def add_patient(self, name: str, age: int) -> None:
        query = insert(self.patients_table).values(id=str(uuid4()), created_on=datetime.now(), name=name, age=age)
        self.conn.execute(query)
        self.conn.commit()
        
    def get_all_patients(self) -> pd.DataFrame | None:
        return pd.read_sql_table('patients', con=self.conn)

    
    def archive_patient(self, id: str) -> None:
        query = delete(self.patients_table).where(self.patients_table.c.id == id)
        self.conn.execute(query)
        self.conn.commit()
        
    def add_session(self, patient_id: str, comment: str) -> None:
        query = insert(self.sessions_table).values(patient_id=patient_id, comment=comment)
        self.conn.execute(query)
        self.conn.commit()
    
    def get_all_sessions(self) -> pd.DataFrame:
        return pd.read_sql_table('sessions', con=self.conn)
        
    
    def get_full_sessions(self) -> pd.DataFrame:
        return pd.read_sql_query(self.full_session_stmt, con=self.conn)
    
    def get_full_sessions_view(self) -> pd.DataFrame:
        return pd.read_sql_query('SELECT * FROM sessions_full', con=self.conn)  # read_sql_table doesn't seem to work on views, says table does't exist.
    
    
if __name__ == '__main__':
    
    with contextlib.suppress():
        os.remove('test2.db')
    
    repo = SqlitePatientRepo(path='test2.db')
    repo.create_db()
    print("Before Run:")
    print(repo.get_all_patients(), end='\n\n')
    
    print("Adding another patient:")
    repo.add_patient(name='Nick', age=35)
    df = repo.get_all_patients()
    print(df, end='\n\n')
    
    # print("Removing the last patient:")
    last_patient = df.iloc[-1]
    # repo.archive_patient(last_patient.id)
    # print(repo.get_all_patients(), end='\n\n')
    
    
    repo.add_session(patient_id=last_patient.id, comment="Sick.  I don't know why...")
    repo.add_session(patient_id=last_patient.id, comment="Healthy!  I don't know why...")
    print(repo.get_all_sessions(), end='\n\n')
    
    print(repo.get_full_sessions(), end='\n\n')
    
    print(repo.get_full_sessions_view(), end='\n\n')
    
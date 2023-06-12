import contextlib
from datetime import datetime
import os
import sqlite3
from uuid import uuid4
import pandas as pd
try:
    from repos.base import IPatientRepo
except:
    from base import IPatientRepo



                

class SqlitePatientRepoNative(IPatientRepo):
    
    def __init__(self, path: str) -> None:
        self.path = path
        self.conn = sqlite3.connect(path)
                
    def create_db(self) -> None:
    
        self.conn.execute("""
            CREATE TABLE patients(
                id VARCHAR PRIMARY KEY,
                created_on VARCHAR,
                name VARCHAR,
                age INTEGER
            )
        """)
            
        self.conn.execute("""
            CREATE TABLE sessions(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                patient_id VARCHAR REFERENCES patients(id),
                comment VARCHAR
            )
        """)
        
        self.conn.execute("""
            CREATE VIEW sessions_full AS
            SELECT sessions.id, patient_id, name as patient_name, age as patient_age, comment 
            FROM sessions 
            JOIN patients ON sessions.patient_id = patients.id
        """)
        
        self.conn.commit()
    
    def add_patient(self, name: str, age: int) -> None:
        self.conn.execute(
            "INSERT INTO patients(id, created_on, name, age) VALUES (?, ?, ?, ?)",
            (
                str(uuid4()),
                datetime.now().isoformat(),
                name,
                age,
            )
        )
        self.conn.commit()
        
    def get_all_patients(self) -> pd.DataFrame | None:
        df = get_table_as_df(self.conn, 'patients')
        df['created_on'] = pd.to_datetime(df.created_on)
        return df

    def archive_patient(self, id: str) -> None:
        self.conn.execute("DELETE FROM patients WHERE  id = ?", (id,))
        self.conn.commit()
    
    def add_session(self, patient_id: str, comment: str) -> None:
        self.conn.execute(
            "INSERT INTO sessions(patient_id, comment) VALUES (?, ?);",
            (patient_id, comment)
        )
        
    def get_all_sessions(self) -> pd.DataFrame | None:
        return get_table_as_df(self.conn, 'sessions')
    
    def get_full_session_info(self) -> pd.DataFrame | None:
        return get_table_as_df(self.conn, 'sessions_full')
    
    
    
def get_table_as_df(conn: sqlite3.Connection, tablename: str) -> pd.DataFrame | None:
    names = [row[1] for row in conn.execute(f"PRAGMA table_info({tablename});").fetchall()]    
    data = conn.execute(f"SELECT * FROM {tablename}").fetchall()
    return pd.DataFrame(data=data, columns=names)
    

if __name__ == '__main__':
    with contextlib.suppress():
        os.remove('test.db')
        
    repo = SqlitePatientRepoNative(path='test.db')
    repo.create_db()
    repo.add_patient(name='Nick DG', age=35)
    patients = repo.get_all_patients()
    print(patients, end='\n\n')
    
    first_id = patients.iloc[0].id
    repo.add_session(patient_id=first_id, comment='Healthy.')
    print(repo.get_all_sessions(), end='\n\n')
    
    print(repo.get_full_session_info(), end='\n\n')
    
    
from datetime import datetime
import sqlite3
from uuid import uuid4
import pandas as pd
from repos.base import IPatientRepo


                

class SqlitePatientRepoNative(IPatientRepo):
    
    def __init__(self, path: str) -> None:
        self.path = path
        self.conn = sqlite3.connect(path)
                
    def create_db(self) -> None:
        try:
            self.conn.execute("""
                CREATE TABLE patients(
                    id VARCHAR PRIMARY KEY,
                    created_on VARCHAR,
                    name VARCHAR,
                    age INTEGER
                )
            """)
            self.conn.commit()
        except sqlite3.OperationalError:
            pass
        
    
    def save(self, name: str, age: int) -> None:
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
        names = [row[1] for row in self.conn.execute("PRAGMA table_info(patients);").fetchall()]
        data = self.conn.execute("SELECT * FROM patients").fetchall()
        df = pd.DataFrame(data=data, columns=names)
        df['created_on'] = pd.to_datetime(df.created_on)
        return df

    def archive(self, id: str) -> None:
        self.conn.execute("DELETE FROM patients WHERE  id = ?", (id,))
        self.conn.commit()
    
    
    
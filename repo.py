from datetime import datetime
import json
from os import makedirs
import os
from typing import Optional
from uuid import uuid4

import duckdb
import pandas as pd

    
def save(name: str, age: int):
    id = str(uuid4())
    created_on = datetime.now().isoformat()
    json_str = json.dumps({'id': id, 'created_on': created_on, 'name': name, 'age': age})
    makedirs('data/patients', exist_ok=True)
    with open(f'data/patients/{id}.json', 'w') as f:
        f.write(json_str)


def get_all_patients() -> Optional[pd.DataFrame]:
    try:
        patients = duckdb.sql("SELECT id, created_on, name, age FROM 'data/patients/*.json'").to_df()
        patients = patients.astype({'created_on': 'datetime64[ns]'})
        return patients
    except duckdb.IOException:
        return None
    
    
def archive(id: str) -> None:
    os.remove(f'data/patients/{id}.json')
from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd


class IPatientRepo(ABC):
    
    @abstractmethod
    def add_patient(self, name: str, age: int) -> None: ...
    
    @abstractmethod
    def get_all_patients(self) -> Optional[pd.DataFrame]: ...
    
    @abstractmethod
    def archive_patient(self, id: str) -> None: ...
    


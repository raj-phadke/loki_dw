from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType
from typing import Dict, List, Tuple


class BaseDataConfig(BaseModel):
    df: DataFrame


class DataMappingConfig(BaseDataConfig):
    column_mapping: Dict[str, Tuple[str, DataType]]  # (new_name, data_type)
    select_columns: List[str]

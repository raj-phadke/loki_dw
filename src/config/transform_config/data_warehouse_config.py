from pydantic import BaseModel, model_validator
from typing import Literal, List, Optional


class BaseDataWarehouseConfig(BaseModel):
    entity_name: str
    entity_type: Literal["Fact", "Dimension"]


class FactAdditivity(BaseModel):
    measure_column_name: str
    additivity: Literal["additive", "semi-additive", "non-additive"]


class FactConfig(BaseDataWarehouseConfig):
    entity_type: str = "Fact"
    entity_subtype: Literal[
        "Transactional", "Accumulating", "Accumulating_Snapshot", "Factless"
    ]
    primary_key_columns: List[str]
    foreign_key_columns: List[str]
    measure_additivity: List[FactAdditivity]
    grain: List[str]

    @model_validator(mode="before")
    def validate_basic_grain(cls, values):
        grain = values.get("grain")
        if not grain or not isinstance(grain, list) or len(grain) == 0:
            raise ValueError("Grain must be a non-empty list of column names.")
        return values


class DimensionConfig(BaseDataWarehouseConfig):
    entity_type: str = "Dimension"
    entity_subtype: Literal[
        "SlowlyChanging",
        "Junk",
        "RolePlaying",
        "Conformed",
    ]
    primary_key_columns: List[str]
    attribute_columns: List[str]
    grain: Optional[List[str]] = None

    @model_validator(mode="before")
    def validate_basic_grain(cls, values):
        grain = values.get("grain")
        primary_keys = values.get("primary_key_columns")
        # If grain is not provided, default it to primary keys
        if grain is None:
            if (
                primary_keys
                and isinstance(primary_keys, list)
                and len(primary_keys) > 0
            ):
                values["grain"] = primary_keys
            else:
                raise ValueError(
                    "Either grain or primary_key_columns must be provided."
                )
        return values

from pydantic import BaseModel, model_validator
from src.config.data_config.base_data_config import DataMappingConfig
from typing import Literal, List, Optional


class BaseDwConfig(BaseModel):
    entity_name: str
    entity_type: Literal["Fact", "Dimension"]
    base_data: DataMappingConfig | List[DataMappingConfig]


class DwFactAdditivity(BaseModel):
    measure_column_name: str
    additivity: Literal["additive", "semi-additive", "non-additive"]


class DwFactConfig(BaseDwConfig):
    base_entity_key: str
    entity_type: str = "Fact"
    entity_subtype: Literal[
        "Transactional", "Accumulating", "Accumulating_Snapshot", "Factless"
    ]
    primary_key_columns: List[str]
    foreign_key_columns: List[str]
    measure_additivity: List[DwFactAdditivity]
    grain: List[str]

    @model_validator(mode="before")
    def validate_basic_grain(cls, values):
        grain = values.get("grain")
        if not grain or not isinstance(grain, list) or len(grain) == 0:
            raise ValueError("Grain must be a non-empty list of column names.")
        return values

    @model_validator(mode="before")
    def validate_base_entity_key_in_base_data(cls, values):
        base_entity_key = values.get("base_entity_key")
        base_data = values.get("base_data")

        if base_entity_key and base_data:
            base_data_list = base_data if isinstance(base_data, list) else [base_data]
            found = False
            for dm in base_data_list:
                keys = dm.dict().keys() if hasattr(dm, "dict") else dm.keys()
                if base_entity_key in keys:
                    found = True
                    break

            if not found:
                raise ValueError(
                    f"base_entity_key '{base_entity_key}' not found in keys of base_data"
                )
        return values


class DwDimensionConfig(BaseDwConfig):
    base_entity_key: str
    entity_type: str = "Dimension"
    entity_subtype: Literal[
        "SCD1",
        "SCD2",
        "SCD3",
        "SCD4",
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

    @model_validator(mode="before")
    def validate_base_entity_key_in_base_data(cls, values):
        base_entity_key = values.get("base_entity_key")
        base_data = values.get("base_data")

        if base_entity_key is None:
            raise ValueError("base_entity_key is required.")
        if base_data is None:
            raise ValueError("base_data is required.")

        base_data_list = base_data if isinstance(base_data, list) else [base_data]
        found = False
        for dm in base_data_list:
            keys = dm.dict().keys() if hasattr(dm, "dict") else dm.keys()
            if base_entity_key in keys:
                found = True
                break

        if not found:
            available_keys = []
            for dm in base_data_list:
                keys = dm.dict().keys() if hasattr(dm, "dict") else dm.keys()
                available_keys.extend(keys)
            available_keys = list(set(available_keys))
            raise ValueError(
                f"base_entity_key '{base_entity_key}' not found in keys of base_data. "
                f"Available keys: {available_keys}"
            )

        return values

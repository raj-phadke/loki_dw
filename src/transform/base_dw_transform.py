from abc import ABC, abstractmethod
from dataclasses import dataclass
import logging
from pyspark.sql import DataFrame
from src.config.transform_config.dw_config import BaseDwConfig
from src.utils.dw_transform_utils import DwTransformUtils


@dataclass
class DwEntity:
    entity_name: str
    entity: DataFrame


class BaseDWTransform(ABC):
    """
    Abstract base class for defining Data Warehouse Transformations
    """

    def __init__(self, config: BaseDwConfig) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = config
        self.utils = (
            DwTransformUtils()
        )  # Creating an instance of utils to be used by concrete implementations
        self.entity_name = (
            self.config.entity_type.capitalize()
            + "_"
            + self.config.entity_name.capitalize()
        )
        self.transformed_data = {}

    @abstractmethod
    def create_dw_entity(self) -> DwEntity:
        """
        Abstract method to create the data warehouse entity
        """
        pass

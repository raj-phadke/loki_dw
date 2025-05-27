from abc import abstractmethod
from src.config.data_config.base_data_config import DataMappingConfig
from src.config.transform_config.dw_config import DwDimensionConfig
from src.transform.base_dw_transform import BaseDWTransform, DwEntity


class BaseDwDimensionTransform(BaseDWTransform):
    """
    Base class to create Dw Facts
    """

    def __init__(self, config: DwDimensionConfig) -> None:
        super().__init__(config=config)
        self._validate_dimension_input_data()

    def _validate_dimension_input_data(self) -> None:
        """
        Validate that base_data is a single instance of DataMappingConfig (not a list).

        Raises:
            ValueError: If base_data is not an instance of DataMappingConfig.
        """
        if not isinstance(self.config.base_data, DataMappingConfig):
            raise ValueError(
                "For DimensionConfig, base_data must be a single DataMappingConfig instance."
            )

    @abstractmethod
    def create_dimension_data(self) -> DwEntity:
        """
        Abstract method to create the dimension entity
        """
        pass

    def create_dw_entity(self) -> DwEntity:
        """
        Concrete implementation for create_dw_entity that utilizes the create_dimension_data method from subclass implementations
        """
        return self.create_dimension_data()

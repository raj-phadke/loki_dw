from abc import abstractmethod
from src.config.data_config.base_data_config import DataMappingConfig
from src.config.transform_config.dw_config import DwFactConfig
from src.transform.base_dw_transform import BaseDWTransform, DwEntity


class BaseDwFactTransform(BaseDWTransform):
    """
    Base class to create Dw Facts
    """

    def __init__(self, config: DwFactConfig) -> None:
        super().__init__(config=config)
        self._validate_fact_input_data()

    def _validate_fact_input_data(self) -> None:
        """
        Validate that base_data is a list containing only DataMappingConfig instances.

        Raises:
            ValueError: If base_data is not a list or contains non-DataMappingConfig items.
        """
        if not (
            isinstance(self.config.base_data, list)
            and all(
                isinstance(item, DataMappingConfig) for item in self.config.base_data
            )
        ):
            raise ValueError(
                "For DwFactConfig, base_data must be a list of DataMappingConfig instances."
            )

    @abstractmethod
    def create_fact_data(self) -> DwEntity:
        """
        Abstract method to create the fact entity
        """
        pass

    def create_dw_entity(self) -> DwEntity:
        """
        Concrete implementation for create_dw_entity that utilizes the create_fact_data subclass implementations
        """
        return self.create_fact_data()

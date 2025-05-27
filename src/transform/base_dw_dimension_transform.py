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

    def preprocess_data(self) -> DwEntity:
        """
        Applying common preprocessing steps to the dataframe
        """
        input_df = self.utils.lowercase_all_columns(df=self.config.base_data.df)

        # Select required columns
        select_columns = self.config.base_data.select_columns
        self.logger.info(f"Selecting specified columns: {select_columns}")
        df_selected = self.utils.select_columns(df=input_df, cols=select_columns)

        # Apply column mapping and casting
        self.logger.info("Applying column mapping")
        df_remapped = self.utils.rename_and_cast_columns_with_check(
            df=df_selected, rename_map=self.config.base_data.column_mapping
        )

        sk_column_name = f"sk_{self.entity_name.lower()}"

        # Create sk column using hash
        self.logger.info(f"Creating Dimension SK: {sk_column_name}")
        df_with_hash_column = self.utils.create_hash_column(
            df=df_remapped,
            col_list=self.config.primary_key_columns,
            output_col=sk_column_name,
        )

        # Rearrange the df
        self.logger.info("Rearranging df columns")
        df_rearranged = self.utils.rearrange_columns(
            df=df_with_hash_column, col_name=sk_column_name
        )

        return df_rearranged

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

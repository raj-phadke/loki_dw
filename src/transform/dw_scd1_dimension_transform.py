from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit
from src.transform.base_dw_dimension_transform import BaseDwDimensionTransform, DwEntity


class DwSCDType1DimensionTransform(BaseDwDimensionTransform):
    """
    Concreate implementation to create Dw SCD Type 1 Dimension
    """

    def add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """
        Method to add SCD Type1 specific metadata columns
        """
        self.logger.info("Adding metadata columns to the df")
        return df.withColumn("dw_created_at", lit(current_timestamp()))

    def create_dimension_data(self) -> DwEntity:
        """
        Concrete implementation of create_dimension_data method to create SCD Type 1 Dimension
        """
        self.logger.info(f"Staring Build: Type1 SCD Dimension {self.entity_name}")
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

        # Add metadata column to the df
        df_final = self.add_metadata_columns(df=df_rearranged)

        self.logger.info(f"Succesfully created Type1 SCD Dimension {self.entity_name}")
        return DwEntity(entity_name=self.entity_name, entity=df_final)

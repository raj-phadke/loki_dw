from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit
from src.transform.base_dw_dimension_transform import BaseDwDimensionTransform, DwEntity


class DwSCDType1DimensionTransform(BaseDwDimensionTransform):
    """
    Concreate implementation to create Dw SCD Type 1 Dimension
    """

    def add_metadata_columns(df: DataFrame) -> DataFrame:
        """
        Method to add SCD Type1 specific metadata columns
        """

        return df.withColumn("dw_created_at", lit(current_timestamp()))

    def create_dimension_data(self) -> DwEntity:
        """
        Concrete implementation of create_dimension_data method to create SCD Type 1 Dimension
        """
        self.logger.info(f"Staring Build: Type1 SCD Dimension {self.entity_name}")
        input_df = self.utils.lowercase_all_columns(df=self.config.base_data.df)

        # Select required columns
        df_selected = self.utils.select_columns(
            df=input_df, cols=self.config.base_data.select_columns
        )

        # Apply column mapping and casting
        df_remapped = self.utils.rename_and_cast_columns_with_check(
            df=df_selected, rename_map=self.config.base_data.column_mapping
        )

        sk_column_name = f"sk_{self.entity_name.lower()}"

        # Create sk column using hash
        df_with_hash_column = self.utils.create_hash_column(
            df=df_remapped,
            col_list=self.config.primary_key_columns,
            output_col=sk_column_name,
        )

        # Rearrange the df
        df_rearranged = self.utils.rearrange_columns(
            df=df_with_hash_column, col_name=sk_column_name
        )

        # Add metadata column to the df
        df_final = self.add_metadata_columns(df=df_rearranged)

        self.logger.info(f"Succesfully created Type1 SCD Dimension {self.entity_name}")
        return DwEntity(entity_name=self.entity_name, entity=df_final)

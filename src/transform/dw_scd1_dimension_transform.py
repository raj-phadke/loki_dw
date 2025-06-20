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
        df_preprocessed = self.preprocess_data()

        # Add metadata column to the df
        df_final = self.add_metadata_columns(df=df_preprocessed)

        self.logger.info(f"Succesfully created Type1 SCD Dimension {self.entity_name}")
        return DwEntity(entity_name=self.entity_name, entity=df_final)

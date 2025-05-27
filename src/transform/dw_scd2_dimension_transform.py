from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit, when
from pyspark.sql.types import TimestampType
from src.transform.base_dw_dimension_transform import BaseDwDimensionTransform, DwEntity
from src.config.transform_config.dw_config import DwDimensionConfig


class DwSCDType2DimensionTransform(BaseDwDimensionTransform):
    """
    Concreate implementation to create Dw SCD Type 1 Dimension
    """

    def __init__(self, config: DwDimensionConfig) -> None:
        super().__init__(config=config)
        self.metadata_mapping = {
            "dw_created_at": lit(current_timestamp()),
            "dw_start_date": lit(current_timestamp().cast(TimestampType())),
            "dw_end_date": lit(current_timestamp().cast(TimestampType())),
            "is_active": lit(False),
        }

    def all_columns_exist(self, df: DataFrame) -> bool:
        """
        Check if metadata columns are already present in the df
        """
        existing_cols = set(df.columns)
        return all(col in existing_cols for col in self.metadata_mapping.keys())

    def add_metadata_columns(self, df: DataFrame) -> DataFrame:
        """
        Method to add SCD Type1 specific metadata columns
        """
        self.logger.info("Adding metadata columns to the df")

        if not self.all_columns_exist(df=df):
            for col, function_map in self.metadata_mapping.items():
                df = df.withColumn(col, function_map)
        else:
            self.logger.info(
                f"Metadata columns {self.metadata_mapping.keys()} already exist in the df"
            )
        return df

    def perform_cdc(self, source_df: DataFrame, target_df: DataFrame) -> DataFrame:
        """
        Method to perform CDC
        """

        hash_columns = ["key_hash", "non_key_hash"]

        target_df = target_df or None  # Retrieve target df or get it passed in

        for col in hash_columns:
            source_df_hashed = self.utils.create_hash_column(
                df=source_df,
                col_list=self.config.primary_key_columns,
                output_col=f"source_{col}",
            )
            target_df_hashed = self.utils.create_hash_column(
                df=source_df,
                col_list=self.config.primary_key_columns,
                output_col=f"source_{col}",
            )

        source_df_hashed.createOrReplaceTempView("source_df")
        target_df_hashed.createOrReplaceTempView("target_df")

        source_extra_records = self.spark.sql("""
                                        SELECT source.*
                                        FROM source_df as source 
                                        LEFT ANTI JOIN target_df as target 
                                        ON source.source_key_hash = target.target_key_hash
                                        """)

        self.logger.info(
            f"Extra records coming from source: {source_extra_records.count()}"
        )

        common_records = self.spark.sql("""
                                        SELECT source.*, target.*
                                        FROM source_df as source 
                                        INNER JOIN target_df as target 
                                        ON source.join_key_hash = target.join_key_hash
                                        """)

        self.logger.info(
            f"Common records between source and target: {common_records.count()}"
        )

        common_records.createOrReplaceTempView("common_df")

        common_with_diff = self.spark.sql("""
                                        SELECT source.*
                                        FROM common_df 
                                        WHERE source.non_key_hash != target.non_key_hash
                                        """)

        self.logger.info(f"Common records with diff: {common_with_diff.count()}")

        common_without_diff = self.spark.sql("""
                                        SELECT target.*
                                        FROM common_df 
                                        WHERE source.non_key_hash == target.non_key_hash
                                        """)

        self.logger.info(f"Common records without diff: {common_without_diff.count()}")

        target_extra_records = self.spark.sql("""
                                        SELECT target.*
                                        FROM target_df as target 
                                        LEFT ANTI JOIN source_df as source 
                                        ON source.source_key_hash = target.target_key_hash
                                        """)

        self.logger.info(f"Extra records in the target: {target_extra_records.count()}")

        self.logger.info(
            f"Expire source dropped records is set to: {self.config.cdc_params.expire_source_dropped_records}"
        )

        if self.config.cdc_params.expire_source_dropped_records:
            target_extra_records_curated = target_extra_records.withColumn(
                "dw_end_date",
                when(
                    col("dw_end_date").isNotNull(), lit(current_timestamp())
                ).otherwise(col("dw_end_date")),
            ).withColumn("is_active", lit(False))
        else:
            target_extra_records_curated = target_extra_records

        combined_df = source_extra_records.unionByName(
            target_extra_records_curated.unionByName(
                common_with_diff.unionByName(common_without_diff)
            )
        )

        return combined_df

    def create_dimension_data(self) -> DwEntity:
        """
        Concrete implementation of create_dimension_data method to create SCD Type 2 Dimension
        """

        self.logger.info(f"Staring Build: Type2 SCD Dimension {self.entity_name}")
        df_preprocessed = self.preprocess_data()

        df_cdc = self.perform_cdc(
            source_df=df_preprocessed, target_df=None
        )  # TODO: Add logic to get target df before this

        # Add metadata column to the df
        df_final = self.add_metadata_columns(df=df_cdc)

        self.logger.info(f"Succesfully created Type1 SCD Dimension {self.entity_name}")
        return DwEntity(entity_name=self.entity_name, entity=df_final)

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, sha2
from pyspark.sql.types import DataType


class DwTransformUtils:
    @staticmethod
    def columns_exist(df: DataFrame, cols: list[str]) -> list[str]:
        """
        Validates that all specified columns exist in the DataFrame.

        Args:
            df (DataFrame): The input Spark DataFrame.
            cols (list[str]): List of column names to check.

        Returns:
            list[str]: List of columns that exist in the DataFrame.

        Raises:
            ValueError: If any of the specified columns are not found in the DataFrame.
        """
        existing_cols = df.columns
        missing_cols = [c for c in cols if c not in existing_cols]
        if missing_cols:
            raise ValueError(
                f"The following columns are missing from the DataFrame: {missing_cols}"
            )
        return [c for c in cols if c in existing_cols]

    @staticmethod
    def rename_and_cast_columns_with_check(
        df: DataFrame, rename_map: dict[str, tuple[str, DataType]]
    ) -> DataFrame:
        """
        Renames and casts columns in the DataFrame based on rename_map,
        but only for columns that exist in the DataFrame.

        Args:
            df (DataFrame): Input Spark DataFrame.
            rename_map (dict[str, tuple[str, DataType]]):
                Dictionary mapping old column names to a tuple of (new_name, DataType).

        Returns:
            DataFrame: DataFrame with renamed and cast columns.
        """
        cols_to_rename = DwTransformUtils.columns_exist(
            df=df, cols=list(rename_map.keys())
        )

        renamed_cols = []
        for col_name in df.columns:
            if col_name in cols_to_rename:
                new_name, data_type = rename_map[col_name]
                renamed_cols.append(col(col_name).cast(data_type).alias(new_name))
            else:
                renamed_cols.append(col(col_name))

        return df.select(*renamed_cols)

    @staticmethod
    def select_columns(df: DataFrame, cols: list[str]) -> DataFrame:
        """
        Selects only the specified columns from the DataFrame, ensuring they exist.

        Args:
            df (DataFrame): Input Spark DataFrame.
            cols (list[str]): List of column names to select.

        Returns:
            DataFrame: DataFrame with only the selected columns.
        """
        if cols:
            cols_to_select = DwTransformUtils.columns_exist(df=df, cols=cols)
            return df.select(*cols_to_select)
        else:
            return df

    @staticmethod
    def create_hash_column(
        df: DataFrame, col_list: list[str], output_col: str
    ) -> DataFrame:
        """
        Adds a SHA-256 hash column to the DataFrame based on the specified columns.

        Args:
            df (DataFrame): Input Spark DataFrame.
            col_list (list[str]): List of column names to include in the hash.
            output_col (str): Name of the new hash column.

        Returns:
            DataFrame: DataFrame with the new hash column.

        Raises:
            ValueError: If col_list is empty or contains non-existent columns.
        """
        if not col_list:
            raise ValueError("col_list must be a non-empty list of column names.")

        cols_to_hash = DwTransformUtils.columns_exist(df=df, cols=col_list)
        concat_expr = concat_ws("||", *[df[c] for c in cols_to_hash])
        return df.withColumn(output_col, sha2(concat_expr, 256))

    @staticmethod
    def rearrange_columns(df: DataFrame, col_name: str) -> DataFrame:
        """
        Moves a specified column to the front and sorts the remaining columns alphabetically.

        Args:
            df (DataFrame): Input Spark DataFrame.
            col_name (str): Column to move to the front.

        Returns:
            DataFrame: Reordered DataFrame.

        Raises:
            ValueError: If the specified column is not in the DataFrame.
        """
        if col_name not in df.columns:
            raise ValueError(f"Column '{col_name}' not found in DataFrame.")

        remaining_cols = sorted([c for c in df.columns if c != col_name])
        new_order = [col_name] + remaining_cols
        return df.select(*new_order)

    @staticmethod
    def lowercase_all_columns(df: DataFrame) -> DataFrame:
        """
        Renames all columns in the DataFrame to lowercase.

        Args:
            df (DataFrame): Input Spark DataFrame.

        Returns:
            DataFrame: DataFrame with all column names converted to lowercase.
        """
        renamed_cols = [df[col].alias(col.lower()) for col in df.columns]
        return df.select(*renamed_cols)

from pyspark.sql import Window, DataFrame
from pyspark.sql import functions as F

def deduplicate(df: DataFrame, keys, order_col=None) -> DataFrame:
    if not order_col:
        return df.dropDuplicates(keys)

    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return (
        df
        .withColumn("__rn__", F.row_number().over(w))
        .filter(F.col("__rn__") == 1)
        .drop("__rn__")
    )

import polars as pl 
import os
from typing import Union, List, Dict
from pathlib import Path

## Loads Enums
from atlas_ecomplexity_data_handler.enums_sources.data_sources_enums import ProductClassification

## Get Atlas Data Local Storage
ATLAS_DATA_ENV_FP = os.getenv("DATA_ATLAS_COMPLEXITY")

## Merge Parent Product Classification
def merge_parent_product_classification(
        data : pl.DataFrame,
        product_classification : str = "hs92",
        initial_level : int = 6,
    ) -> pl.DataFrame:

    # Define Data File Path
    ATLAS_DATA_FP = Path(ATLAS_DATA_ENV_FP)
    PRODUCT_CLASSIFICATION_FP = ATLAS_DATA_FP / ProductClassification.get_file_name(product_classification)

    df_product_class = pl.read_csv(PRODUCT_CLASSIFICATION_FP, ignore_errors=True)

    
    product_levels = df_product_class.select("product_level").unique().to_series().to_list()

    product_levels.sort(reverse=True)
    
    data = data.with_columns(
        pl.col("product_id").alias(f"level_{product_levels[0]}")
    )

    product_levels_sub = product_levels[1:]

    for product_level in product_levels_sub:
        
        left_on_key = f"level_{product_levels[product_levels.index(product_level) - 1]}"

        data = data.join(
            df_product_class.select("product_id", "product_parent_id"),
                left_on = left_on_key,
                right_on = "product_id"
            ).rename(
                {
                    "product_parent_id" : f"level_{product_level}",
                }
            ).join(
            df_product_class.select("product_id", f"product_{product_classification}_code","product_name_short"),
                left_on = f"level_{product_level}",
                right_on = "product_id"
            ).rename(
                {
                    "product_name_short" : f"product_name_level_{product_level}",
                    f"product_{product_classification}_code" :  f"product_{product_classification}_code_{product_level}"
                }
            )
        

    return data

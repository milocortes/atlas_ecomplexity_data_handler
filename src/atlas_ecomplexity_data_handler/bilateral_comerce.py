import polars as pl 
import os
from typing import Union, List, Dict
from pathlib import Path

## Loads Enums
from atlas_ecomplexity_data_handler.enums_sources.data_sources_enums import CountryTradePartnerProduct

## Load Filter Conditions
from atlas_ecomplexity_data_handler.utils import filter_bilateral_country_trade_condition

## Get Atlas Data Local Storage
ATLAS_DATA_ENV_FP = os.getenv("DATA_ATLAS_COMPLEXITY")


## Get Country Trade by Partner and Product
def bilateral_country_trade(
        country_origin         : Union[str, List[str], None] = None,
        country_destination    : Union[str, List[str]] = "USA",
        product_classification : str = "hs92",
        year                   : int = None 
    ) -> pl.DataFrame:
    """ 
    Get Country Trade by Partner and Product. 

    Arguments
    ---------

    country_origin : Union[str, List[str], None]
        Three letter country identification code. Augmented version of ISO 3166-1 alpha-3 codes.    
    
    country_destination : Union[str, List[str], None]
        Three letter country identification code. Augmented version of ISO 3166-1 alpha-3 codes. Values correspond to identical countries as country ISO-3 code field.
    
    product_classification: str
        International comerce product clasification. Options {hs92, hs12, sitc}
    
    Returns
    -------

    pl.DataFrame

    Examples
    --------
    >>> bilateral_country_trade(country_origin = "MEX", country_destination = "USA", product_classification = "hs92", year = 2023)
    shape: (4_357, 9)
    ┌────────────┬───────────────────┬────────────────────┬───────────────────┬───┬───────────────────┬──────┬──────────────┬──────────────┐
    │ country_id ┆ country_iso3_code ┆ partner_country_id ┆ partner_iso3_code ┆ … ┆ product_hs92_code ┆ year ┆ export_value ┆ import_value │
    │ ---        ┆ ---               ┆ ---                ┆ ---               ┆   ┆ ---               ┆ ---  ┆ ---          ┆ ---          │
    │ i64        ┆ str               ┆ i64                ┆ str               ┆   ┆ i64               ┆ i64  ┆ i64          ┆ i64          │
    ╞════════════╪═══════════════════╪════════════════════╪═══════════════════╪═══╪═══════════════════╪══════╪══════════════╪══════════════╡
    │ 484        ┆ MEX               ┆ 840                ┆ USA               ┆ … ┆ 10111             ┆ 2023 ┆ 399262       ┆ 5519073      │
    │ 484        ┆ MEX               ┆ 840                ┆ USA               ┆ … ┆ 10119             ┆ 2023 ┆ 3576616      ┆ 33182795     │
    │ 484        ┆ MEX               ┆ 840                ┆ USA               ┆ … ┆ 10210             ┆ 2023 ┆ 0            ┆ 12869635     │
    │ 484        ┆ MEX               ┆ 840                ┆ USA               ┆ … ┆ 10290             ┆ 2023 ┆ 412874578    ┆ 44837043     │
    │ 484        ┆ MEX               ┆ 840                ┆ USA               ┆ … ┆ 10310             ┆ 2023 ┆ 0            ┆ 2273832      │
    │ …          ┆ …                 ┆ …                  ┆ …                 ┆ … ┆ …                 ┆ …    ┆ …            ┆ …            │
    │ 484        ┆ MEX               ┆ 840                ┆ USA               ┆ … ┆ 970300            ┆ 2023 ┆ 2368777      ┆ 30021869     │
    │ 484        ┆ MEX               ┆ 840                ┆ USA               ┆ … ┆ 970400            ┆ 2023 ┆ 150279       ┆ 9842         │
    │ 484        ┆ MEX               ┆ 840                ┆ USA               ┆ … ┆ 970500            ┆ 2023 ┆ 2844719      ┆ 544801       │
    │ 484        ┆ MEX               ┆ 840                ┆ USA               ┆ … ┆ 970600            ┆ 2023 ┆ 292793       ┆ 4183005      │
    │ 484        ┆ MEX               ┆ 840                ┆ USA               ┆ … ┆ 999999            ┆ 2023 ┆ 37830713675  ┆ 14430743043  │
    └────────────┴───────────────────┴────────────────────┴───────────────────┴───┴───────────────────┴──────┴──────────────┴──────────────┘
    """
    
    # Define Data File Path
    ATLAS_DATA_FP = Path(ATLAS_DATA_ENV_FP)
    BILATERAL_TRADE_FP = ATLAS_DATA_FP / CountryTradePartnerProduct.get_file_name(product_classification)

    # Convert args from str to List[str]
    country_origin = country_origin if isinstance(country_origin, List) else [country_origin]
    country_destination = country_destination if isinstance(country_destination, List) else [country_destination]

    args_to_test_conditions = {
            "country_origin" : country_origin,
            "country_destination" : country_destination,
            "year" : year,
        }

    conditions = filter_bilateral_country_trade_condition(args_to_test_conditions)

    # Define plan
    q = (
        pl.scan_csv(BILATERAL_TRADE_FP, ignore_errors=True)
        .select(["country_id","country_iso3_code","partner_country_id","partner_iso3_code","product_id", f"product_{product_classification}_code","year","export_value", "import_value"])
        .filter(
            *conditions
        )
    )

    return q.collect()





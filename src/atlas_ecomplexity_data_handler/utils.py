from typing import Dict, List
import polars as pl

def filter_bilateral_country_trade_condition(recursos : Dict[str, List[str] | int]):
    match recursos:
        case {"country_origin" : ["World"], "country_destination" : country_destination, "year" : year}:
            return [
                    pl.col('partner_iso3_code').is_in(country_destination),
                    pl.col("year") == year
            ]
        case {"country_origin" : country_origin, "country_destination" : ["World"], "year" : year}:
            return [
                    pl.col('country_iso3_code').is_in(country_origin),
                    pl.col("year") == year
            ]
        case {"country_origin" : country_origin, "country_destination" : country_destination, "year" : year}:
            return [
                    pl.col('country_iso3_code').is_in(country_origin),
                    pl.col('partner_iso3_code').is_in(country_destination),
                    pl.col("year") == year
            ]     
                    

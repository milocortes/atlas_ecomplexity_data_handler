from enum import Enum

# Define Class GetFileName
class GetFileName:
    @classmethod
    def get_file_name(cls, 
                    product_classification: str) -> str:
        match product_classification:
            case cls.hs92.name:
                return cls.hs92.value
            case cls.hs12.name:
                return cls.hs12.value
            case cls.sitc.name:
                return cls.sitc.value
            case _:
                raise ValueError(f"Unknown Product Clasification: {product_classification}")
                                            

# Define Enums for the Atlas Data Sources Country Trade Partner Product
class CountryTradePartnerProduct(GetFileName, Enum):
    hs92 = "hs92_country_country_product_year_6_2020_2023.csv"
    hs12 = "hs12_country_country_product_year_6_2020_2023.csv"
    sitc = "sitc_country_country_product_year_6_2020_2023.csv"
    

# Define Enums for the Atlas Product Classification
class ProductClassification(GetFileName, Enum):
    hs92 = "product_hs92.csv"
    hs12 = "product_hs12.csv"
    sitc = "product_sitc.csv"
    





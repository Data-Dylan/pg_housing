"""
Functions for webscraping and API calls for Prince George property data.
author: Data Dylan
"""

# Software packages.
from arcgis.features import FeatureLayer
import pandas as pd

# pandas options.
pd.options.display.max_columns = 25


def get_roll_nums() -> pd.core.frame.DataFrame:
    """
    Function that retreives data from BC Assessment's Service Boundary Web 
    Map GIS data service. The main values of interest are in the ROLL_NUM
    column that can be used for scraping data from BC Assessment's website.
    
    *** 
    This function is in need of a performance improvement.
    I need to find a way to query just the Prince George area for data.
    ***

    Returns
    -------
    df : pandas.core.frame.DataFrame
        The dataframe columns are as follows:
            JUR: The juristdiction code. Currently, this is only for the 
                Prince George area (code: 226).
            ROLL_NUM: The roll number for the property. This is basically a
                UID for properties, but technically this is not totally true
                because of some nuisance regarding very specific circumstances
                that occasionally come up.
            IMPR_VALUE: This is BC Assessment's non-land derived 
                assessment value. It is added with the LAND_VALUE column to
                generate a total assesment value that is used for tax purposes
                by local municipalities.
            LAND_VALUE: This is BC Assessment's land derived 
                assessment value. It is added with the IMPR_VALUE column to
                generate a total assesment value that is used for tax purposes
                by local municipalities.
    """

    # URL for Service Boundary Web Map.
    url = "https://arcgis.bcassessment.ca/ext_wa/rest/services/SBWM/SBWM/MapServer/2/"
    
    # Columns to drop in the spatial dataframe.
    drop_cols = [
                    "OBJECTID",
                    "PARID",
                    "YEAR",
                    "AFP_OID",
                    "ROLL_TOTAL",
                    "ROLL_BLDG",
                    "ROLL_LAND",
                    "TOTAL_ASSESSED_VALUE",
                    "SHAPE"
            ]
    
    # Request data as a spatial dataframe.
    layer = FeatureLayer(url)
    df = layer.query().df
    
    # Subset the data for Prince George properties.
    df = df[df["AFP_OID"].str.startswith("226")]
    
    # Drop unwanted columns.
    df.drop(columns = drop_cols, inplace = True)
    
    # Change column values from float to int.
    df["IMPR_VALUE"] = df["IMPR_VALUE"].astype("Int32")
    df["LAND_VALUE"] = df["LAND_VALUE"].astype("Int32")
    
    # Add Prince George JUR number.
    df.insert(0, "JUR", 226)
    
    # Reset index to avoid confusion.
    df.reset_index(inplace = True, drop = True)
    
    # Return data.
    return df

test = get_roll_nums()

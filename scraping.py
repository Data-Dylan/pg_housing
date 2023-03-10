"""
Functions for webscraping and API calls for Prince George property data.
author: Data Dylan
"""

# Software packages.
from arcgis.features import FeatureLayer
import pandas as pd
import requests

# pandas options.
pd.options.display.max_columns = 25


def get_roll_nums(jur = 226) -> pd.core.frame.DataFrame:
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
            JUR: The jurisdiction code. Currently, the default argument
                    is Prince George's jurisdiction code (226).
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
    df = df[df["AFP_OID"].str.startswith(f"{jur}")]
    
    # Drop unwanted columns.
    df.drop(columns = drop_cols, inplace = True)
    
    # Change column values from float to int.
    df["IMPR_VALUE"] = df["IMPR_VALUE"].astype("Int32")
    df["LAND_VALUE"] = df["LAND_VALUE"].astype("Int32")
    
    # Add Prince George JUR number.
    df.insert(0, "JUR", jur)
    
    # Reset index to avoid confusion.
    df.reset_index(inplace = True, drop = True)
    
    # Return data.
    return df

def get_web_uid(jur: str, roll: str) -> str:
    """
    Retrieves a UID that can be used to scrape data from BCA's online 
    assessment search.

    Parameters
    ----------
    jur : str
        The jurisdiction code for the local government level. Used to for
        tax legislation purposes.
    roll : str
        

    Raises
    ------
    Raises an error if a 200 reponse code is not given in the request.

    Returns
    -------
    str
        The URL UID for BCA's online assessment search.
    """
    
    # Hidden API base URL.
    base_url = "https://www.bcassessment.ca/Property/Search/GetByRollNumber/"
    
    # Make request to the hidden API.
    r = requests.get(f"{base_url}{jur}?roll={roll}")

    # Retreive web address UID if successful. 
    if r.status_code == 200:
        uid = r.json().replace("ok-", "")
        
    # Raise error if there is a different statud code.
    else: 
        r.raise_for_status()
        raise ValueError("Successful reponse. Unknown status code.")
        
    # Return the web address UID.
    return uid

test = get_roll_nums()
        

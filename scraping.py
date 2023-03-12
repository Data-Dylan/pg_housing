"""
Functions for webscraping and API calls for Prince George property data.
author: Data Dylan


Need to try the same thing with selenium.
"""

# Software packages.
from arcgis.features import FeatureLayer
import pandas as pd
import numpy as np
import requests
from time import sleep
import scrapy
from collections import defaultdict
import re

# pandas options.
pd.options.display.max_columns = 25

def xpath_select(sel: scrapy.selector.unified.Selector, xpath: str) -> str:
    """
    Select values from XML/HTML using XPATH operators. 

    Parameters
    ----------
    sel : scrapy.selector.unified.Selector
        A scrapy selector object.
    xpath : str
        An XPATH expression.

    Returns
    -------
    str
        Returns the selected items from the XPATH expression.
    """
    x = sel.xpath(xpath)
    x = x.extract()
    return x

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
    url = "".join(["https://arcgis.bcassessment.ca/ext_wa/", 
                   "rest/services/SBWM/SBWM/MapServer/2/"])
    
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
    
    # Print message to indicate that the request has finished.
    print("The BCA juristdiction and roll numbers have been retreived.")
    
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

    # Retreive web address UID if request was successful. 
    if r.status_code == 200:
        uid = r.json().replace("ok-", "")
        
    # Raise error if there is a different status code.
    else: 
        r.raise_for_status()
        raise ValueError("Successful reponse. Unknown status code.")
        
    # Return the web address UID.
    return uid

def get_bca_data(jurs: np.ndarray,
                 rolls: np.ndarray) -> pd.core.frame.DataFrame:
    
    # Empty list-dictionary to append values to. 
    dict_data = defaultdict(list)
    
    # Amount of records to scrape.
    n = len(jurs)
    
    # Create iteration data for the main loop.
    prop_iter = zip(jurs, rolls)
    
    # Iterate through jurisdiction and roll numbers.
    for i, (jur, roll) in enumerate(prop_iter):
    
        # Base URL for the printing-friendly display of the property's web page.
        base_url = "https://www.bcassessment.ca/property/info/print/"
        
        # IDs that do not always show up on the property web page.
        dynamic_ids = [
                            "lblTotalAssessedLand",
                            "lblTotalAssessedBuilding",
                            "lblPreviousAssessedLand",
                            "lblPreviousAssessedBuilding",
                            "property-comments",
                            "lblComments"
            ]
        
        # Get web address UID.
        uid = get_web_uid(jur, roll)
        
        # Make request to get raw HTML data.
        r = requests.get(f"{base_url}{uid}")
        
        # Retreive HTML if request was successful. 
        if r.status_code == 200:
            html = r.text
        
        # Raise error if there is a different status code.
        else: 
            r.raise_for_status()
            raise ValueError("Successful reponse. Unknown status code.")
            
        # Create scrapy selector object.
        sel = scrapy.Selector(text = html)
        
        # Get IDs for fields of interest with a single value.
        ids_regex = "^lbl|^manufacture|^legal|^property-comments"
        ids = xpath_select(sel, "//@id")
        ids = list(filter(lambda x: bool(re.search(ids_regex, x)), ids))
        
        # Add the juristdiction and roll number to the dataframe.
        dict_data["jur"] = jur
        dict_data["roll"] = roll
        
        # Iterate through id values.
        for id_name in ids:
            
            # Select id, using xpath.
            value = xpath_select(sel, f"//*[@id='{id_name}']/text()")
            
            # If list is empty append NaN, otherwise append the value.
            if not value:
                dict_data[id_name].append(np.nan)
                continue
            else:
                value = value[0].strip()
                value = np.nan if value == "" else value
                dict_data[id_name].append(value)
                
        # Iterate through dynamic ids and populate any missing values.
        for id_name in dynamic_ids:
            if id_name not in ids:
                dict_data[id_name].append(np.nan)

        # Print current iteration progress.
        print(f"{i + 1} out of {n} records scraped.")
        print(f"{round((i + 1) / n, 4) * 100}% complete.")
        print()
        
        # Be respectful, pause the scraping function to give the server a break.
        sleep(3)
                
    # Create dataframe from dictionary.
    df = pd.DataFrame(dict_data)
        
    # Return the property data.
    return df
    
    
roll_df = get_roll_nums()
jurs = roll_df["JUR"].values
rolls = roll_df["ROLL_NUM"].values
bca_df = get_bca_data(jurs, rolls)

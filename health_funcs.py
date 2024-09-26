#%% Import modules

#Pulling from the web
import requests
import bs4
from seleniumwire import webdriver 
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver import Keys
from selenium.webdriver.common.action_chains import ActionChains
gecko_driver_path = '../Home/foo/bar/geckodriver.exe'

#os 
import sys
import os
from datetime import datetime
import re



#file types
import csv
from io import BytesIO
import zipfile

#manipulation
import polars as pl
import numpy as np


#%% Functions

### CMS Op price lookup
def cms_pricing(code)-> pl.DataFrame:
    '''Scrapes the medicare pricing tool to check prices
        Args:
            A OP code or list of codes
        returns:
            a polars dataframe
    '''
    ##Declaring Vars 
    cms_base_url = r'https://www.medicare.gov/procedure-price-lookup/cost/'
    code_ls = code 
    td = datetime.date.today() 
    td = td.strftime("%Y%m%d")
    Amb_series = []
    Op_series = []
    date_series = []
    code_series = []


    ##Scraping vars
    #OP
    op_tag ='div.ds-l-lg-col--6:nth-child(2) > div:nth-child(1) > div:nth-child(3) > div:nth-child(3) > div:nth-child(2) > strong:nth-child(2)'
    op_expand_button = (By.XPATH, '/html/body/div[2]/div/main/div/div/div[2]/div/section[1]/div[1]/div[2]/div/div[3]/button')

    #ambulatory
    amb_tag = 'div.ds-l-lg-col--6:nth-child(1) > div:nth-child(1) > div:nth-child(3) > div:nth-child(3) > div:nth-child(2) > strong:nth-child(2)'
    amb_expand_button = (By.XPATH, '/html/body/div[2]/div/main/div/div/div[2]/div/section[1]/div[1]/div[1]/div/div[3]/button')

    #loop
    for cpt in code_ls:
        #Creating the url
        url = cms_base_url + cpt

        #Opening the driver and link
        # chrome_options = uc.ChromeOptions()

        # driver = uc.Chrome(
        #     options = chrome_options,
        #     seleniumwire_options= {}
        # )
        driver = webdriver.Chrome()
        # driver = webdriver.Firefox()


        driver.get(url)

        ## OP Price
        #open drop down
        (WebDriverWait(driver,5)
        .until(EC.element_to_be_clickable(op_expand_button))
        .click())

        #find element
        OP_price = driver.find_element(By.CSS_SELECTOR, op_tag).text.strip()
        

        ## Ambulatory price
        (WebDriverWait(driver,5)
        .until(EC.element_to_be_clickable(amb_expand_button))
        .click())

        #find element
        Amb_price = driver.find_element(By.CSS_SELECTOR, amb_tag).text.strip()


        ## Create Series
        code_series.extend([cpt])
        if len(OP_price)>=1:
            Op_series.extend([OP_price])
        else:
            Op_series.extend(None)

        if len(Amb_price)>=1 :
            Amb_series.extend([Amb_price])
        else:
            Amb_series.extend(None)

        date_series.extend([td])

    driver.quit()

    ### Add to Excel sheet
    #Dataframe
    data = { 'Code' : code_series,
            'Outpatient_Prices' : Op_series,
            'Ambulatory_Prices' : Amb_series,
            'Date_Scraped' : date_series}
    df = pl.DataFrame(data)
    
    return df

def preprocess_value(value):
  """Preprocesses a single value from the JSON data

  Args:
      The value to preprocess

  Returns:
      The preprocessed value
  """

  if isinstance(value, str):
    # Remove quotes and non-numeric characters from potential prices
    return float(re.sub(r'[^\d\-+\.]', '', value)) if re.search(r'\d+\.\d{2}', value) else value  # Check for price format
  elif isinstance(value, dict):
    # Recursively preprocess nested dictionaries
    return preprocess_data(value.copy())
  else:
    # Handle NaNs and leave other data types unchanged
    return np.nan if value == 'NaN' else value


def preprocess_data(data):
  """Preprocesses the given JSON data

  Args:
    The JSON data to preprocess

  Returns:
    The preprocessed JSON data.
  """

  if isinstance(data, list):
    # Preprocess each item in a list
    return [preprocess_value(item) for item in data]
  
  elif isinstance(data, dict):
    # Preprocess each key-value pair and create a new dictionary
    return {k: preprocess_value(v) for k, v in data.items()}
  
  else:
    # Handle other data types (leave unchanged)
    return data


def mrf_unlock(df)-> pl.DataFrame:
    """A loop that unnest dicts and explodes list found in MRFs.

    Args:
        A polars dataframe
    
    Returns:
        A clean polars dataframe
    """

    #Dataframe
    df_test = df
    #Objects
    ls_cols = []
    dict_cols = []

    #Checking the schema
    col_type = df_test.schema
    for k,v in col_type.items():
        
        if 'struct' in str(v).lower() and 'list' not in str(v).lower():
            dict_cols.append(k)

        elif 'list' in str(v).lower() and 'struct' not in str(v).lower(): 
            ls_cols.append(k)

        elif 'struct' in str(v).lower() and listfirst(str(v)) == False:
            dict_cols.append(k)

        elif 'list' in str(v).lower() and listfirst(str(v)) == True:
            ls_cols.append(k)
        
    #loop depending on ls size
    while True:
        if (len(dict_cols)>=1) & (len(ls_cols)>=1):
            
        # if:
            for col in dict_cols:
                # print(f'dict col unnesting: {col}')
                df_test = (
                    df_test
                    .unnest(col)
                )
            for cols in ls_cols:
                # print(f'list col exploding:{col}')
                df_test = (
                    df_test
                    .explode(cols)
                )


        elif (len(dict_cols)>=1) & (len(ls_cols)<1):
            
            for col in dict_cols:
                df_test = (
                    df_test
                    .unnest(col)
                )

        elif (len(dict_cols)<1) & (len(ls_cols)>=1):

            for col in ls_cols:
                df_test = (
                    df_test
                    .explode(col)
                )
        
        elif (len(dict_cols)<1) & (len(ls_cols)<1):
            break


        #Rechecking the schema
        #Clear list then restart loop
        dict_cols.clear()
        ls_cols.clear()

        #Check schema again
        col_type = df_test.schema

        for k,v in col_type.items():
            if 'struct' in str(v).lower() and 'list' not in str(v).lower():
                dict_cols.append(k)

            elif 'list' in str(v).lower() and 'struct' not in str(v).lower(): 
                ls_cols.append(k)

            elif 'struct' and 'list' in str(v).lower():
                #checking position of both words
                list_idx = str(v).lower().find('list')
                dict_idx = str(v).lower().find('struct')

                if list_idx > dict_idx:
                    dict_cols.append(k)

                elif list_idx < dict_idx:
                    ls_cols.append(k)
                



#%% Import Modules

#Data Manipulation
import polars as pl
import json

#OS
import os
import sys


#Funcs
from health_funcs import python_unzip, mrf_unlock




#%% Reading Json
path = r"/PATH/TO/YOUR/MRF"
try:
    if str(path).endswith('.gz'):
        ##Fastest Route: Try reading as parquet first
        try:
            test_df = (pl.scan_parquet(path)
                        .select(pl.col('^(?i).*network.*$')) #Getting the in-network and out-of-network rates
                        .explode(pl.col('^(?i).*network.*$'))
                        .to_series()
                        .struct.unnest()    #Unnesting the dict
                        .collect()
                    )
            test_df = mrf_unlock(test_df)
        except Exception as e:
            print(f"There's a parsing error : {e}")
        
        try: 
        ##Unzip the and create json
            python_unzip(path)
            new_path = str(path)[:-3]
            test_df = (pl.read_json(path)
                        .select(pl.col('^(?i).*network.*$')) #Getting the in-network and out-of-network rates
                        .explode(pl.col('^(?i).*network.*$'))
                        .to_series()
                    .struct.unnest())   #Unnesting the dict
            # test_df = mrf_unlock(test_df)
        except Exception as e:
            print(f"We can't read this as a json: {e}")

    else:   
        try:
            ##Fastest Route to Read Json
            test_df = (pl.read_json(path)
                    .select(pl.col('^(?i).*network.*$')) #Getting the in-network and out-of-network rates
                    .explode(pl.col('^(?i).*network.*$'))
                    .to_series()
                    .struct.unnest()    #Unnesting the dict
                    )
            # test_df = mrf_unlock(test_df)
                    

        except Exception as e:
            print(f'Still working on the error handling here {e}')
            ##Traditional Route
            # with open(path, 'r') as f:
            #     data = json.loads(f)
            #     mrf_data = data[]

except Exception as e:
    print(f"We tried to open the json and it failed")

   

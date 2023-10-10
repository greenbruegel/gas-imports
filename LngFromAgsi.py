import json
import requests
import pandas as pd
import numpy as np

from datetime import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from datetime import date

import matplotlib.pyplot as plt 
import os

def gios_f():
    Share_point = r'C:\Users\giovanni.sgaravatti\Bruegel\Research - 2021-11 European natural gas imports\Code\LNG terminals'
    os.chdir(Share_point)
    Points_APIs = pd.read_excel(r'Dataproviders 09.05.2023.xlsx',sheet_name='LNG terminals')
    headers = {"x-key":"d8561648296cb38e6c400823755689941530"} # After July 4th 2022
    d1 = '2023-08-01'
    d2 = '2023-12-31' # To change in the new year
    dates = []
    sendOut = []
    stored = []
    max_storage = []
    max_sendout =[]
    country = []
    Name = []
    print('test')
    for item in range(len(Points_APIs)):
            url = Points_APIs.iloc[item,3]+'&from={}&to={}&size=300'.format(d1,d2)
            try:
                r = requests.get(url,headers=headers)
                if r.status_code != 200:
                    print(r.status_code)
                    print(url)

                raw_data = r.json()
                inner_data = raw_data['data']
                for x in inner_data:
                    #here we work with the APIs
                    date = x['gasDayStart'] 
                    inventory = x['inventory']
                    sendOuts = x['sendOut']
                    Max_Storage = x['dtmi']
                    Max_Sendout = x['dtrs']
                    #here we work with the excel file
                    countries = Points_APIs.iloc[item,0]
                    Names = Points_APIs.iloc[item,5]


                    dates.append(date)     
                    sendOut.append(sendOuts)
                    stored.append(inventory)
                    max_storage.append(Max_Storage)
                    max_sendout.append(Max_Sendout)
                    country.append(countries)
                    Name.append(Names)
            except Exception as e:
                print(e)

    df = pd.DataFrame()
    df['dates'] = dates
    df['sendOut'] = sendOut          ## in GWh/d
    df['stored'] = stored            ## in 10^3 m^3 LNG
    df['Max Storage'] = max_storage  ## in 10^3 m^3 LNG
    df['Max Sendout'] = max_sendout  ## in GWh/d
    df['country'] = country
    df['name'] = Name

    # Getting rid of NaNa
    df.replace('-', np.NaN,inplace=True)
    # Getting rid of observations for which we have no data
    df = df.dropna()

    df['sendOut'] = pd.to_numeric(df['sendOut'])
    df['stored'] = pd.to_numeric(df['stored'])
    df['Max Storage'] = pd.to_numeric(df['Max Storage']) 
    df['Max Sendout'] = pd.to_numeric(df['Max Sendout']) 

    return df

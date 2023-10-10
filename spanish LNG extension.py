import json
import requests
import pandas as pd
import numpy as np

from datetime import datetime
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.dates import DateFormatter
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
import seaborn as sns

import eurostat  # python wrapper for taking data.
import time

# Converter for metric tonnes to M3m 
converter = -0.001397


df_tot = df_EU27.reset_index()
iberia = df_tot[df_tot['Port_Country'].isin(['Spain','Portugal'])]
cols = iberia.iloc[:,:2]

# convert to M3m
values = iberia.iloc[:,2:]*converter
iberia = pd.concat([cols,values], axis=1)

iberia.to_excel(r'C:\Users\giovanni.sgaravatti\Bruegel\Research - 2023-04 How the EU can phase out Russian LNG\Data\Iberian LNG by source.xlsx')
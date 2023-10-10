# hourly data entsog

import os
os.getcwd()
# change the label and time according to missing data
## Norway - Dunkquerque
it_identifiers = ['fr-tso-0003itp-00045entry']

dates_it = []
# hours_it = []
values_it = []
direction_it = []
operator_it = []
point_it = []
labels_it = []

urls = []
ids = []

for idt in it_identifiers:
    print(idt)
    try:
        url = 'https://transparency.entsog.eu/api/v1/operationalData?forceDownload=true&pointDirection={}&from=2023-05-16&to=2023-05-27&indicator=Physical%20Flow&periodType=hour&timezone=CET&limit=-1&dataset=1&directDownload=true'.format(idt)
        r = requests.get(url)
        data = r.json()
        op = data['operationalData']

        for item in op:
            date = item['periodFrom'][:13]
#             hour = item['periodFrom'][11:13]
            value = item['value']
            drc = item['directionKey']
            opr = item['operatorKey']
            pt = item['pointKey']
            label = item['pointLabel']

            dates_it.append(date)
            # hours_it.append(hour)
            values_it.append(value)
            direction_it.append(drc)
            operator_it.append(opr)
            point_it.append(pt)
            labels_it.append(label)
            urls.append(url)
            ids.append(idt)
            
    except Exception as e:
        print(e)
        print(idt)  

df_it = pd.DataFrame()
df_it['dates'] = dates_it
# df_it['hours'] = hours_it
df_it['values'] = values_it
df_it['direction'] = direction_it
df_it['operator'] = operator_it
df_it['location'] = point_it
df_it['label'] = labels_it
df_it['country'] = df_it['operator'].str[:2]

df_it.label.unique()
df_it.set_index(pd.DatetimeIndex(df_it.dates), inplace=True)
del df_it['dates']
df_add = df_it[df_it['label']=='Dunkerque']
idh = pd.date_range('2023-05-16 06:00:00', '2023-05-27 05:00:00',freq='H')
df_add = df_add.reindex(idh, method='ffill')
#we use the agg function with a dictionary of aggregation methods to resample the hourly data to daily data while preserving the first value of every string_col column.
df_add_d = df_add.resample('D').agg({'values': 'sum', 'direction': 'first','operator':'first','location':'first','label':'first','country':'first'})
df_add_d.reset_index(inplace=True)
df_add_d.rename({'index':'dates'},axis=1,inplace=True)
df_add_d
df_add_d['dates'] = df_add_d['dates'].dt.strftime('%Y-%m-%d')
df.shape
df = pd.concat([df,df_add_d])
df.shape
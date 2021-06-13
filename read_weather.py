import requests
import datetime
import pickle
import json
import pandas as pd

def create_assist_date(datestart = None,dateend = None):
    # 创建日期辅助表

    if datestart is None:
        datestart = '20160101'
    if dateend is None:
        dateend = datetime.datetime.now().strftime('%Y%m%d')

    # 转为日期格式
    datestart=datetime.datetime.strptime(datestart,'%Y%m%d')
    dateend=datetime.datetime.strptime(dateend,'%Y%m%d')
    date_list = []
    date_list.append(datestart.strftime('%Y%m%d'))
    while datestart<dateend:
        # 日期叠加一天
        datestart+=datetime.timedelta(days=+1)
        # 日期转字符串存入列表
        date_list.append(datestart.strftime('%Y%m%d'))
    return date_list

date_list = create_assist_date("20210529", "20210609")
data = []
for date in date_list:
    url = 'https://api.qweather.com/v7/historical/weather?location=101090101&date=%s&key=6713ea1e280b4c5592208ac00f50b327'%date
    rep = requests.get(url)
    rep.encoding = 'utf-8'
    data.append(rep.json())
print(data[-2:])
# with open("weather.pkl", 'wb') as f:
#     pickle.dump(data, f)
df09 = pd.DataFrame(data[-1]['weatherHourly'])
df08 = pd.DataFrame(data[-2]['weatherHourly'])

with open("weather.pkl", 'rb') as f:
    data = pickle.load(f)
print(data[0]['weatherHourly'])

df = pd.DataFrame(data=data[0]['weatherHourly'])
for i in range(1, len(data)):
    # print(data[i]['weatherHourly'])
    df1 = pd.DataFrame(data=data[i]['weatherHourly'])
    df = df.append(df1)

df=df.append(df08)
df=df.append(df09)

del df['text'], df['windDir'], df['windScale']

print(df)
print(df.columns)


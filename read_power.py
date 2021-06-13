import pandas as pd
from pathlib import Path
import requests
import datetime

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

def generate_power_df(col = 'solar', start_date=None, end_date=None):
    dir = 'power_data_new'
    p = Path(dir)
    file_names = list(p.glob('*.xls'))
    df_power = None
    cols = ['时间', '铅碳储能/kW', '铅碳储能', '磷酸铁锂储能/kW', '磷酸铁锂储能', '光伏\综保控制器/kW',
            '风机\综保控制器/kW', '负载\用户负载综保/kW', '并网点\综保点控制器/kW']
    for file_name in file_names:
        df = pd.read_excel(file_name, names=cols)
        if df_power is None:
            df_power = df
        else:
            df_power = df_power.append(df)

    cols = df_power.columns
    df_power = df_power[[cols[0],cols[5],cols[6],cols[7]]]
    df_power = df_power.sort_values(by=cols[0])
    df_power.columns = ['time', 'solar', 'wind', 'load']

    df_power['time'] = pd.to_datetime(df_power['time']).map(lambda x: x.strftime("%Y-%m-%d %H"))
    df_power['load'] = -df_power['load']

    mask = (df_power['time'] >= start_date) & (df_power['time'] <= end_date)
    df_power_final = df_power.loc[mask]


    col1_s = (datetime.datetime.strptime(start_date, "%Y-%m-%d %H")+datetime.timedelta(days=-1)).strftime("%Y-%m-%d %H")
    col1_e = (datetime.datetime.strptime(end_date, "%Y-%m-%d %H")+datetime.timedelta(days=-1)).strftime("%Y-%m-%d %H")
    mask = (df_power['time'] >= col1_s) & (df_power['time'] <= col1_e)
    df_history = df_power.loc[mask][['time', 'solar','load']]
    df_history.columns = ['time', 'solar1', 'load1']
    df_history['time'] = df_history['time'].map(lambda x: (datetime.datetime.strptime(x, "%Y-%m-%d %H")+datetime.timedelta(days=1)).strftime("%Y-%m-%d %H"))
    df_power_final = pd.merge(df_power_final, df_history, on='time')

    col1_s = (datetime.datetime.strptime(start_date, "%Y-%m-%d %H")+datetime.timedelta(days=-2)).strftime("%Y-%m-%d %H")
    col1_e = (datetime.datetime.strptime(end_date, "%Y-%m-%d %H")+datetime.timedelta(days=-2)).strftime("%Y-%m-%d %H")
    mask = (df_power['time'] >= col1_s) & (df_power['time'] <= col1_e)
    df_history = df_power.loc[mask][['time', 'solar','load']]
    df_history.columns = ['time', 'solar2', 'load2']
    df_history['time'] = df_history['time'].map(lambda x: (datetime.datetime.strptime(x, "%Y-%m-%d %H")+datetime.timedelta(days=2)).strftime("%Y-%m-%d %H"))
    df_power_final = pd.merge(df_power_final, df_history, on='time')

    col1_s = (datetime.datetime.strptime(start_date, "%Y-%m-%d %H")+datetime.timedelta(days=-3)).strftime("%Y-%m-%d %H")
    col1_e = (datetime.datetime.strptime(end_date, "%Y-%m-%d %H")+datetime.timedelta(days=-3)).strftime("%Y-%m-%d %H")
    mask = (df_power['time'] >= col1_s) & (df_power['time'] <= col1_e)
    df_history = df_power.loc[mask][['time', 'solar','load']]
    df_history.columns = ['time', 'solar3', 'load3']

    df_history['time'] = df_history['time'].map(lambda x: (datetime.datetime.strptime(x, "%Y-%m-%d %H")+datetime.timedelta(days=3)).strftime("%Y-%m-%d %H"))
    df_power_final = pd.merge(df_power_final, df_history, on='time')

    cols=[]
    cols.append('time')
    for i in range(3):
        cols.append('{}{}'.format(col, i+1))
    cols.append(col)
    return df_power_final[cols]

def generate_weather_df(start_date=None, end_date=None):
    import pickle
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
        df1 = pd.DataFrame(data=data[i]['weatherHourly'])
        df = df.append(df1)
    df=df.append(df08)
    df=df.append(df09)
    del df['text'], df['windDir'], df['windScale']

    df['time'] = pd.to_datetime(df['time']).map(lambda x: x.strftime("%Y-%m-%d %H"))
    mask = (df['time'] >= start_date) & (df['time'] <= end_date)
    df_final = df.loc[mask]
    return df_final

def get_train(target='load'):
    start_date = '2021-05-29 00'
    end_date = '2021-06-06 00'
    df_weather = generate_weather_df(start_date, end_date)
    df_power = generate_power_df(target, start_date, end_date)

    df = pd.merge(df_weather, df_power, on='time')

    df['time'] = pd.to_datetime(df['time']).map(lambda x: int(x.strftime("%H")))
    # if 'load' in df.columns:
    #     df['load'] = -df['load']
    df.to_csv("{}1_train.csv".format(target), index=False)

def get_test(target='load'):
    start_date = '2021-06-06 00'
    end_date = '2021-06-09 10'
    df_weather = generate_weather_df(start_date, end_date)
    df_power = generate_power_df(target,start_date, end_date)

    df = pd.merge(df_weather, df_power, on='time')
    df['time'] = pd.to_datetime(df['time']).map(lambda x: int(x.strftime("%H")))
    # del df['time']
    # if 'load' in df.columns:
    #     df['load'] = -df['load']
    df.to_csv("{}1_test.csv".format(target), index=False)

def get_all(target='load'):
    start_date = '2021-05-29 00'
    end_date = '2021-06-09 10'
    df_weather = generate_weather_df(start_date, end_date)
    df_power = generate_power_df(target, start_date, end_date)

    df = pd.merge(df_weather, df_power, on='time')

    df['time'] = pd.to_datetime(df['time']).map(lambda x: int(x.strftime("%H")))
    # if 'load' in df.columns:
    #     df['load'] = -df['load']
    df.to_csv("{}_all.csv".format(target), index=False)

if __name__ == '__main__':

    # get_train('solar')
    # get_test('solar')
    # get_train('load')
    # get_test('load')
    # get_all('solar')
    get_all('load')
import pandas as pd
import numpy as np
import pymysql
from datetime import datetime, timedelta
from time import gmtime, strftime
import sqlalchemy
import mysql

def paper_clean(gh):
    df_pl=gh[(gh.deviceName.str.contains("PaperTowel")) |(gh.deviceName.str.contains("ToiletPaper")) ]
    #b['Time']=b['Time'].astype(str)
    b=df_pl
    new=b['Time'].str.split(":",expand=True)
    b['hour']=new[0]
    b['Time']=b['hour']+":00:00"
    b['date_']=b['Date']+" "+b['Time']
    a=b
    a['difference'] = a.groupby('deviceName')['value'].diff(1) * (-1)
    a['difference']=np.where(a.difference < -20 ,1,a.difference)
    a['Smoothed_values']=np.where(a.difference < 0,a.value.shift(1),a.value)
    return a


def date_time_operation(df):
    df['date']=df['date'].astype(str)
    new=df['date'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    new=df['Time'].str.split(":",expand=True)
    df['hour']=new[0]
    df['minute']=new[1]
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where(df['minute']<=30,'00','30')
    df['date_Hhr']=df['Date']+" "+df['hour']+":"+df['minute']+":"+'00'
    if 'WaterFlow' == df.sensorName[0]:
        df=pd.DataFrame(df.groupby(['areaId','areaName','deviceName','date_Hhr'])['sensorValue','washCountValue'].first())
        df.reset_index(inplace=True)
        df['date_Hhr']=df['date_Hhr'].astype(str)
        new=df['date_Hhr'].str.split(" ",n=1,expand=True)
        df['Date']=new[0]
        df['Time']=new[1]
        gh=df[['areaId','deviceName','Date','Time','W_usage','W_count']]
    else:
        df=pd.DataFrame(df.groupby(['areaId','areaName','deviceName','date_Hhr'])['value'].first())
        df.reset_index(inplace=True)
        df['date_Hhr']=df['date_Hhr'].astype(str)
        new=df['date_Hhr'].str.split(" ",n=1,expand=True)
        df['Date']=new[0]
        df['Time']=new[1]
        gh=df[['areaId','deviceName','Date','Time','value']]
    return gh


def Paper_Prepare_Stream(Final,Final_1,R,e_date_time):
    ghk=pd.concat([Final,Final_1],axis=0)
    rk=None
    rk=ghk.sort_values(['client_id','area_id','device_id','date','date_time'],ascending=True)
    rk.reset_index(inplace=True)
    rk=rk.sort_values(['area_id','device_id'],ascending=True)
    rk['difference'] = rk.groupby(['area_id','device_id'])['raw_value'].diff(1) * (-1)
    rk['difference']=np.where(rk.difference < -20 ,1,rk.difference)
    rk['smoothed_value']=np.where(rk.raw_value<0,rk.raw_value.shift(1),rk.raw_value)
    rk['smoothed_value']=np.where((rk.difference <0),rk.smoothed_value.shift(1),rk.smoothed_value)
    rk['smoothed_value']=np.where(rk.smoothed_value == 255,rk.smoothed_value.shift(1),rk.smoothed_value)
    rk['Lag1_Replacement'] =rk.groupby(['area_id','device_id'])['smoothed_value'].shift(-1)
    a1 = rk['smoothed_value']/100
    a2 = rk['Lag1_Replacement']/100
    r = 6.5
    rk['formula_usage'] = (a1 - a2)*((2*r+(a1+a2)*R))*100/(2*r+R)
    rk['formula_usage'] = np.where(rk['formula_usage']<-20, 0, rk.formula_usage)
    rk['usage']=rk.groupby(['area_id','device_id'])['formula_usage'].shift(1)
    rk=rk[['client_id', 'area_id', 'device_id', 'date_time', 'date', 'period_type','period', 'raw_value', 'smoothed_value', 'usage', 'created_date']]
    rkl=rk[(rk.date_time == e_date_time)]
    return rkl


def paper_prepare(b):
    db=pd.DataFrame(b.groupby(['areaId','deviceName','date_'])['value','Smoothed_values','Date','hour'].first())
    db.reset_index(inplace=True)
    db['created_date']=pd.to_datetime('now').replace(microsecond=0)
    db['usage'] = db.groupby('deviceName')['Smoothed_values'].diff(1) * (-1)
    db['usage']=np.where(db.usage < -20 ,0,db.usage)    
    return db


def Usage_Calculation(Final,Final_1):
    ghj=pd.concat([Final,Final_1],axis=0)
    Dff=pd.DataFrame(ghj.groupby(['client_id','area_id','device_id','date','hour'])['raw_value'].first())
    Dff.reset_index(inplace=True)
    Dff['difference'] = Dff.groupby('device_id')['raw_value'].diff(1) * (-1)
    Dff['difference']=np.where(Dff.difference < -20 ,1,Dff.difference)
    Dff['Smoothed_values']=np.where(Dff.difference < 0,Dff.raw_value.shift(1),Dff.raw_value)
    Dff['usage'] = Dff.groupby('device_id')['Smoothed_values'].diff(1) * (-1)
    Dff['usage']=np.where(Dff.usage < -20 ,0,Dff.usage)
    Dff=Dff[['client_id', 'area_id', 'device_id', 'date', 'hour', 'raw_value','Smoothed_values', 'usage']]
    Dff['date_']=Dff['date']+" "+Dff['hour']+":00:00"
    Dff['created_date']=pd.to_datetime('now').replace(microsecond=0)
    Rff=Dff[['client_id','area_id', 'device_id', 'date_', 'date', 'hour', 'raw_value','Smoothed_values', 'usage', 'created_date']]
    Rff.columns=['client_id','area_id', 'device_id', 'date_time', 'date', 'hour', 'raw_value','smoothed_value', 'usage', 'created_date']
    return Rff


def Trash_Prepare_Stream(Final,Final_1,e_date_time):
    ghk=pd.concat([Final,Final_1],axis=0)
    rk=None
    rk=ghk.sort_values(['client_id','area_id','device_id','date','date_time'],ascending=True)
    rk.reset_index(inplace=True)
    rk['difference'] = rk.groupby(['area_id','device_id'])['raw_value'].diff(1) * (-1)
    rk['smoothed_value']=np.where((rk.raw_value<0),rk.raw_value.shift(+1),rk.raw_value)
    rk['smoothed_value']=np.where((rk.difference <= 20)&(rk.difference >0) ,rk.smoothed_value.shift(+1),rk.smoothed_value)
    rk['difference'] = rk.groupby(['area_id','device_id'])['smoothed_value'].diff(1) * (1)
    rk['usage']=np.where(rk.difference <-20 ,rk.smoothed_value,rk.difference)
    rk=rk[['client_id', 'area_id', 'device_id', 'date_time', 'date', 'period_type','period', 'raw_value', 'smoothed_value', 'usage', 'created_date']]
    rkl=rk[(rk.date_time == e_date_time)]
    return rkl


def Trash_Half_Hour_Calc(Final,s_hour):
    new=Final['Time'].str.split(":",expand=True)
    Final['hour']=new[0]
    Final['hour']=Final['hour'].astype(int)
    k=int(Final['hour'][0])
    k=k+1
    if k < 10:
        k=str(k)
        Final['hour']="0"+k
        Final['Time']=Final['Time'].str.replace(""+s_hour+"",""'0'+k+"")
    else:
        k=str(k)
        Final['hour']=k
        Final['Time']=Final['Time'].str.replace(""+s_hour+"",k)
    Final['date_time']=Final['Date']+" "+Final['Time']
    return Final


def Trash_Half_Hour_Data_Preparation(Final,Final_1):
    Final_2=Final_1.groupby(['client_id', 'area_id', 'device_id','date', 'hour'])['date_time','raw_value'].last()
    Final_2.reset_index(inplace=True)
    hjk=pd.concat([Final,Final_2],axis=0)
    rjk=hjk.sort_values(['client_id', 'area_id', 'device_id', 'date', 'hour'],ascending=True)
    rjk['difference_1'] = rjk.groupby(['device_id'])['raw_value'].diff(1) * (-1)
    rjk['smoothed_value']=np.where(rjk.difference_1 > 1,rjk.raw_value.shift(1),rjk.raw_value)
    rjk['smoothed_value']=np.where(rjk.raw_value == 255,rjk.raw_value.shift(1),rjk['smoothed_value'])
    rjk['usage'] = rjk.groupby('device_id')['smoothed_value'].diff(1) * (1)
    rjk['usage']=np.where(rjk.usage < 0 ,rjk.smoothed_value.shift(1),rjk.usage)
    rjk=rjk[['client_id', 'area_id', 'device_id','date_time','date', 'hour','raw_value','smoothed_value', 'usage']]
    return rjk


def hrly_Toilet_Usage(output):
    Toilet_Usage=pd.DataFrame(output.groupby(['client_id','area_id','date','hour'])['usage'].sum())
    Toilet_Usage.reset_index(inplace=True)
    Toilet_Usage.columns=['client_id', 'area_id', 'date', 'hour', 'Toilet_usage']
    Toilet_Usage['created_date']=pd.to_datetime('now').replace(microsecond=0)
    return Toilet_Usage


def hrly_Paper_Usage(output):
    Paper_Usage=pd.DataFrame(output.groupby(['client_id','area_id','date','hour'])['usage'].sum())
    Paper_Usage.reset_index(inplace=True)
    Paper_Usage.columns=['client_id', 'area_id', 'date', 'hour', 'Paper_usage']
    Paper_Usage['created_date']=pd.to_datetime('now').replace(microsecond=0)
    return Paper_Usage


def hrly_Washbasin_Usage(output):
    Wash_Usage=pd.DataFrame(output.groupby(['client_id','area_id','date','hour'])['water_usage','traffic_count'].sum())
    Wash_Usage.reset_index(inplace=True)
    Wash_Usage.columns=['client_id', 'area_id', 'date', 'hour', 'water_usage','traffic_count']
    Wash_Usage['created_date']=pd.to_datetime('now').replace(microsecond=0)
    return Wash_Usage


def DataBase_Connection():
    database_username = '***'
    database_password = '****'
    database_ip       = '****'
    database_name     = '****'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))
    return database_connection
    

def get_period(dt):
    hh = int(dt.split(":")[0])
    jj= int(dt.split(":")[1])
    dl=[]
    if hh == 0 :
        ad = 0
        #ls.append(ad)
        if jj>=30:
            ad = ad+1
            dl.append(ad)
        else:
            dl.append(ad)
    elif hh > 0 :
        added=hh*2
        #dl.append(added)
        if jj>=30:
            added=added+1
            dl.append(added)
        else:
            dl.append(added)
    return dl



def PCA_loading_factor_half_hour(Final,loading_Factor):
    lf_toilet=loading_Factor[(loading_Factor.device_type.str.contains('toiletPaper'))]
    lf_paper=loading_Factor[(loading_Factor.device_type.str.contains('paperTowel'))]
    lf_water=loading_Factor[(loading_Factor.device_type.str.contains('water'))]
    lf_traffic=loading_Factor[(loading_Factor.device_type.str.contains('traffic'))]
    lf_trash=loading_Factor[(loading_Factor.device_type.str.contains('trashbin'))]
    lf_traffic=lf_traffic.iloc[0:]
    toilet_std_N=((int(Final['Toilet_usage']) - lf_toilet['mean'])/lf_toilet['std_dev'])
    paper_std_N=((int(Final['Paper_usage']) - lf_paper['mean'])/lf_paper['std_dev'])
    Traffic_std_N=((float(Final['Traffic_count']) - lf_traffic['mean'])/lf_traffic['std_dev'])
    trash_std_N=((float(Final['Trash_usage']) - lf_trash['mean'])/lf_trash['std_dev'])
    #water_std_N=((int(Final['water_usage']) - lf_water['mean'])/lf_water['std_dev'])
    #traffic_std_N=((int(Final['traffic_count']) - lf_traffic['mean'])/lf_traffic['std_dev'])
    Final['scoring']=(float(lf_toilet.loadings*toilet_std_N))+(float(lf_paper.loadings*paper_std_N))+(float(lf_traffic.loadings*Traffic_std_N))+(float(lf_trash.loadings*trash_std_N))
    Final['unclealiness_index']=(Final['scoring'] - lf_toilet['loading_factor_Min'])/(lf_toilet['loading_factor_Max']-lf_toilet['loading_factor_Min'])
    Final_=Final[['processing_id','client_id', 'area_id','date', 'date_time','period_type','period','scoring','unclealiness_index', 'created_date']]
    return Final_



def weekday_assign(dt_client):
    current_date=dt_client.timetuple()
    weekday=pd.DataFrame(np.arange(0,7),columns=['day']).astype(str)
    weekday['week']='dow'
    weekday['dow_']=weekday.week+'_'+weekday.day
    weekday['day']=weekday.day.astype(float)
    weekday['dow']=np.where(weekday.day==current_date.tm_wday,1,0)
    weekday['dow']=weekday['dow'].astype(float)
    weekday=weekday[['dow_','dow']]
    weekday.set_index(['dow_'],inplace=True)
    weekdays=weekday.T
    weekdays.reset_index(inplace=True)
    return weekdays


def traffic_usage_lag(traffic_4,Paper_1):    
    traffic_4['traffic_count']=traffic_4['traffic_count'].astype(float)
    traffic_lag_=pd.DataFrame(traffic_4.groupby(['period'])['traffic_count'].sum()).T
    if len(traffic_lag_.columns)==6:
        traffic_lag_.columns=['lag6','lag5','lag4','lag3','lag2','lag1']
    elif len(traffic_lag_.columns)==5:
        traffic_lag_.columns=['lag5','lag4','lag3','lag2','lag1']
    elif len(traffic_lag_.columns)==4:
        traffic_lag_.columns=['lag4','lag3','lag2','lag1']
        traffic_lag_['lag5']=0    
    elif len(traffic_lag_.columns)==3:
        traffic_lag_.columns=['lag3','lag2','lag1']
        traffic_lag_['lag5']=0 
        traffic_lag_['lag4']=0
    traffic_lag_.reset_index(inplace=True)
    usage_lag=pd.DataFrame(Paper_1.groupby(['period'])['usage'].sum()).T
    if len(usage_lag.columns)==6:
        usage_lag.columns=['lag6','lag5','lag4','lag3','lag2','lag1']
    elif len(usage_lag.columns)==5:
        usage_lag.columns=['lag5','lag4','lag3','lag2','lag1']
    elif len(usage_lag.columns)==4:
        usage_lag.columns=['lag4','lag3','lag2','lag1']
        usage_lag['lag5']=0    
    elif len(usage_lag.columns)==3:
        usage_lag.columns=['lag3','lag2','lag1']
        usage_lag['lag5']=0 
        usage_lag['lag4']=0
    usage_lag.reset_index(inplace=True)
    return traffic_lag_,usage_lag


def period_assign(period):
    period_=pd.DataFrame(np.arange(0,48),columns=['period']).astype(str)
    period_['pe']='period'
    period_['period_']=period_.pe+'_'+period_.period
    period_['period']=period_['period'].astype(float)
    period_['period_gh']=np.where(period_.period==np.array(period)[0],1,0)
    period_['period_gh']= period_['period_gh'].astype(float)
    period_=period_[['period_','period_gh']]
    period_.set_index(['period_'],inplace=True)
    period_type=period_.T
    period_type.reset_index(inplace=True)
    return period_type


def time_formation(pd):
    new=pd.split(" ")
    k=new[1]
    p_period=get_period(k)
    new=pd.split(" ")
    p_Date =new[0]
    p_time = new[1]
    new=p_time.split(":")
    p_hour=new[0]
    p_minute=new[1]
    if p_minute>='29':
        p_minute='30'
    else:
        p_minute='00'
    p_date_time=p_Date+" "+p_hour+":"+p_minute+":00"
    return p_date_time,p_Date,p_time
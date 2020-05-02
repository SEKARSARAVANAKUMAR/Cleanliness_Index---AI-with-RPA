#People Count Batch
import pymysql
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import requests
import sqlalchemy
import mysql
from dateutil import rrule
import fetch_


s_cn=str(input("enter client_id ex: 11 for denver"))
a_id=str(input("enter area_id ex: 11 for denver"))
a = None
try:
    db = pymysql.connect("***", "****", "****", "***")
    query1="select * from sample_client_details where clientid ="+s_cn+" and areaid in (121);"
    print("db connection sucessfull")
    with db.cursor() as cursor:
        cursor.execute(query1)
        a=pd.read_sql(query1,db)
        print(a.info())
        #print(df.describe(include='all'))
finally:
    db.close()
a.devicetype =a.devicetype.astype(str)
z=a[(a.devicetype.str.contains('People'))]
z['areaid'] = z['areaid'].astype(int)
z = z.sort_values(['areaid'],ascending=True)
z['areaid'] = z['areaid'].astype(str)

db = pymysql.connect("*****", "****", "****", "****")
print("db connection sucessfull")
for index,rows in z.iterrows():
    #print(rows)
    cn=rows.clinetname
    areaId= rows.areaid
    dev_type=rows.devicetype
    ct=rows.timezone
    client_id =rows.clientid
    tf=rows.timeLF
    tfd=rows.timeFD
    pub_time=rows.pub_time
    R=rows.roll_width
    #sd=str(input("enter start date : (ex:2019-03-01 00:00:01)"))
    #ed=str(input("enter end date : (ex:2019-03-31 23:59:59)"))
    sd="2019-11-01 00:00:00"
    ed="2019-12-29 23:59:59"
    query="select * from(select ds.deviceId,ds.deviceName,ds.deviceMacId,ds.deviceSensorId,ds.areaId,ds.areaName,ds.floorId,ds.floorName,ds.buildingId,ds.buildingName,ds.sensorName,ds.batteryValue,ds.rssiValue,ds.Status, sv.sensorData as value, CONVERT_TZ(sv.deviceTimeStamp,'+00:00','"+tf+""+ct+":00') as date from "+cn+".deviceStatus ds join "+cn+".sensor_value sv on sv.deviceSensorId=ds.deviceSensorId where sv.deviceTimeStamp>= CONVERT_TZ('"+sd+"','+00:00','"+tfd+""+ct+":00') and sv.deviceTimeStamp<= CONVERT_TZ('"+ed+"','+00:00','"+tfd+""+ct+":00') and ds.deviceName like ('People%') and ds.areaId in ("+areaId+") and sv.sensorData >=0 group by ds.deviceMacId,sv.id) a order by areaName,deviceId,date;"   
    #print(query)
    df = None
    try:

        with db.cursor() as cursor:
            cursor.execute(query)
            df=pd.read_sql(query,db)
            print(df.info())
            #print(df.describe(include='all'))
            #print(df.head())
    except:
        print("Query Error")
    try:
        io=str(df.date.min()).split(" ")
        sd=io[0]+" "+"00:00:00"
        ed="2019-12-29 23:59:59"
        now=pd.to_datetime(sd)
        end=pd.to_datetime(ed)
        
        def datetime_range_calc(start, end, delta):
            current = start
            while current < end:
                yield current
                current += delta

        v=[]
        dts = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in datetime_range_calc(now,end,timedelta(minutes=30))]
        v.append(dts)
        v=pd.DataFrame(v)
        finalized = v.T
        #period-dim
        j = pd.DataFrame(finalized)
        j['period_type']='half-hourly'
        j.columns=['date_time','period_type']
        gg=list(range(48))
        #repeating 48 period n times
        j['period'] = pd.Series(np.tile(gg, len(j)))
    except:
        print("datetime_range=_calculation function error")
    try:
        pr1=fetch_.date_time_operation(df)
        pr1['client_id']=client_id
        if pub_time == 30:
            per_type='half_hourly'
        else:
            per_type='hourly'
        #pr1['period_type']=per_type
        #dt=np.array(pr1['Time'])
        #k=dt[0]
        #pr1['period']=get_period(k)
        pr1['created_date']=pd.to_datetime('now').replace(microsecond=0)
        #pr1['period']=None
        pr1['date_time']=pr1['Date']+" "+pr1['Time']
        pr1=pr1[['client_id','areaId', 'deviceName', 'date_time','Date','value','created_date']]
        pr1.columns=['client_id','area_id','device_id','date_time','date','traffic_count','created_date']
        j['date_time']=j['date_time'].astype(str)
        Gigar=pd.merge(j,pr1,on=['date_time'],how="left")
        Gigar_2=Gigar[['client_id', 'area_id', 'device_id', 'date_time', 'date','period_type','period', 'traffic_count', 'created_date']]
        Gigar_3=fetch_.Final_clean_People_count(Gigar_2)
        Gigar_3['traffic_count']=Gigar_3['traffic_count'].astype(int)
        Final=Gigar_3[['client_id','area_id', 'device_id', 'date_time', 'date', 'traffic_count','created_date']]
        j['date_time']=j['date_time'].astype(str)
        grpd_traff = pd.DataFrame(Final.groupby(['date_time'])['traffic_count'].sum())
        grpd_traff.reset_index(inplace=True)
        uniqs = Final.device_id.unique()
        kj=""
        grpd_traff['device_id'] = kj.join(uniqs[0])
        data_loss_traffic=fetch_.data_loss_finder(grpd_traff,client_id,areaId,j)
        Gigar_fin=data_loss_traffic[['client_id','area_id', 'device_id', 'date_time', 'date', 'period_type','period', 'traffic_count','created_date']]  
        print(j.shape,Gigar_fin.shape)
        print(Gigar_fin.isnull().sum())
    except:
        print("Error : Check the data",areaId)
    try:
        zk=z[z.areaid == ""+areaId+""]
        data_ass=zk[['clientid','clinetname','areaid']]
        data_ass['devicetype']='PeopleCount'
        data_ass['start_date']=np.min(df['Date'])
        data_ass['end_date'] = np.max(df['Date'])
        data_ass['device_count']=df.deviceName.nunique()
        kl=pd.DataFrame(np.matrix(df.deviceName.unique()))
        data_ass['device_id']=np.matrix(kl[kl.columns[0:]].apply(lambda x: ', '.join(x.dropna().astype(str)),axis=1))
        data_ass['gap_count']=Gigar_2.traffic_count.isnull().sum()
        data_ass['gap_count_percentage']= (Gigar_2.traffic_count.isnull().sum() / len(Gigar_fin))*100
        data_ass['total_periods']=len(Gigar_fin)
        data_ass['traffic_greater_than_zero']=len(Gigar_fin[Gigar_fin.traffic_count>0])
        print(data_ass)
    except:
        print("check data ass")
    try:    
        database_username = '***'
        database_password = '****'
        database_ip       = '****'
        database_name     = '*****'
        database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))
        Gigar_fin.to_sql(con=database_connection, name='people_count_batch', if_exists='append',index=False)
    except:
        print("check the People_count_data sql")
    try:
        data_ass.to_sql(con=database_connection, name='data_assesment_traffic_count', if_exists='append',index=False)
    except:
        print("check the data_assessment traffic")
db.close()


#Paper Towel Batch New Code 
import fetch_
import pymysql
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import requests
import sqlalchemy
import mysql
from dateutil import rrule

s_cn=str(input("enter client_id ex: 11 for denver"))
a_id=str(input("enter area_id ex: 11 for denver"))
a = None
try:
    dbe = pymysql.connect("****", "***", "****", "****")
    query1="select * from sample_client_details where clientid ="+s_cn+" and areaid in (160,162,171,172);"
    print("db connection sucessfull")
    with dbe.cursor() as cursor:
        cursor.execute(query1)
        a=pd.read_sql(query1,dbe)
        print(a.info())
        #print(ab.info())
        #print(df.describe(include='all'))
except:
    print("chech test server")
a.devicetype =a.devicetype.astype(str)
z=a[(a.devicetype.str.contains('paperTowel'))]
z.areaid=z.areaid.astype(int)
z=z.sort_values(['areaid'],ascending=True)
z.areaid=z.areaid.astype(str)
db = pymysql.connect("db-read.zancompute.com", "zanprduser", "Atla$19ZC", "MasterDB")
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
    R=rows.roll_width
    #sd=str(input("enter start date : (ex:2019-03-01 00:00:01)"))
    #ed=str(input("enter end date : (ex:2019-03-31 23:59:59)"))
    sd="2019-09-30 00:00:00"
    ed="2019-12-31 23:59:59"
    query="select * from(select ds.deviceId,ds.deviceName,ds.deviceMacId,ds.deviceSensorId,ds.areaId,ds.areaName,ds.floorId,ds.floorName,ds.buildingId,ds.buildingName,ds.sensorName,ds.batteryValue,ds.rssiValue,ds.Status, sv.percentage as value, CONVERT_TZ(sv.deviceTimeStamp,'+00:00','"+tf+""+ct+":00') as date from "+cn+".deviceStatus ds join "+cn+".sensor_value sv on sv.deviceSensorId=ds.deviceSensorId where sv.deviceTimeStamp>= CONVERT_TZ('"+sd+"','+00:00','"+tfd+""+ct+":00') and sv.deviceTimeStamp<= CONVERT_TZ('"+ed+"','+00:00','"+tfd+""+ct+":00') and ds.deviceName like ('PaperTo%') and ds.areaId in ("+areaId+") and sv.percentage >=0 group by ds.deviceMacId,sv.id) a order by areaName,deviceId,date;"   
    query2="select * from people_count_batch where area_id="+areaId+" and client_id="+s_cn+";# and period_type='half-hourly';"
    try:
        with dbe.cursor() as cursor:
            cursor.execute(query2)
            ab=pd.read_sql(query2,dbe)
            print(ab.info())
            #print(ab.info())
            #print(df.describe(include='all'))
    except:
        print("chech test server")
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
        ed="2019-12-31 23:59:59"
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
        print("check the date_Time range_calculation_function")
        
    
    try:
        pr1=fetch_.date_time_operation(df)
        pr2=fetch_.paper_clean(pr1)
        Final=fetch_.paper_towel_usage_calculation(pr2,R)
        Final['client_id']=np.repeat(client_id,len(Final))
        Final=Final[['client_id','area_id', 'device_id', 'date_time', 'date', 'raw_value','smoothed_value', 'usage', 'created_date']]
        j['date_time']=j['date_time'].astype(str)
        Gigar=pd.merge(j,Final,on=['date_time'],how="left")
        #Gigar['period']=fetch_.get_period_five_mins(Gigar)
        Gigar_2=Gigar[['client_id', 'area_id', 'device_id', 'date_time', 'date', 'period_type','period','raw_value', 'smoothed_value', 'usage', 'created_date']]
        Gigar_3=fetch_.optimize_smoothed_error_count_paper(Gigar_2)
        Gigar_4=fetch_.paper_towel_usage_calculation_optimize(Gigar_3,R)
        #Gigar_5=fetch_.Final_clean(Gigar_3)
    except:
        print("Error : Check the data")
    
    try:
        Gigar_6=Gigar_4[['client_id', 'area_id', 'device_id', 'date_time', 'date', 'raw_value', 'smoothed_value', 'usage', 'created_date']]
        Gigar_6.date_time=Gigar_6.date_time.astype(str)
        Gigar_6.device_id=Gigar_6.device_id.astype(str)
        ghkl=fetch_.data_loss_finder(Gigar_6,client_id,areaId,j)
        #ghkl_.reset_index(inplace=True)
        result=fetch_.usage_gap_distribution(ghkl,ab)
        result_=result[result.device_id !='nan']
        result_.smoothed_value=np.where(result_.smoothed_value > 100,0,result_.smoothed_value)
        result_['usage'] = np.where(result_['date_time'] == '2019-05-01 00:00:00',0,result_.usage)
    except:
        print("enter dedata")
    try:
        database_username = '***'
        database_password = '*****'
        database_ip       = '****'
        database_name     = '****'
        database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))
        result_.to_sql(con=database_connection, name='paper_towel_batch', if_exists='append',index=False)
    except:
        print("check the paper_towel_batch")
    #data_ass
    try:
        data_ass=fetch_.data_assesment(areaId,z,fetch_.merge_device_count_proc,result_,fetch_.duplication_finder_half_hour,df,fetch_.inproper_data_finder,pr1)
        data_ass_dev=fetch_.data_assesment_device(fetch_.inproper_data_finder,result_,df,pr1)
        data_ass.to_sql(con=database_connection, name='data_assesment', if_exists='append',index=False)
        data_ass_dev.to_sql(con=database_connection, name='data_assesment_device', if_exists='append',index=False)
    except:
        print("check data assessment part")
db.close()
dbe.close()
#tlt_ass.to_csv('C:\\Users\\saravanan\\Desktop\\client_details_cleanliness_index_paper.csv',index=False)
#tlt_ass_dev.to_csv('C:\\Users\\saravanan\\Desktop\\client_details_cleanliness_index_paper_device.csv',index=False)


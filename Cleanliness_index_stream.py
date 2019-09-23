#Feature Engineeing with Cleanliness_index - Stream
import fetch_stream
import pandas as pd
strt_time=pd.to_datetime('now').replace(microsecond=0)
import pymysql
import numpy as np
from datetime import datetime, timedelta
from time import gmtime, strftime
import sys
import requests
import sqlalchemy
import mysql
try:
    db = pymysql.connect("localhost", "root", "zancompute", "Analytics")
    dbe = pymysql.connect("db-read.zancompute.com", "zanprduser", "Atla$19ZC", "MasterDB")
except:
    print("data base connection error")
a = None
try:
    query1="select * from sample_client_details where clientid='17' and areaid='26';"
    print("db connection sucessfull")
    with db.cursor() as cursor:
        cursor.execute(query1)
        a=pd.read_sql(query1,db)
except:
    print("Client_details_table query error")
a.devicetype =a.devicetype.astype(str)
try:
    z=a[(a.devicetype.str.contains('paperTowel'))]
    R_pt=(np.array(a[a.devicetype=='paperTowel'].roll_width))[0]
except:
    print('paperTowel not available ')
try:
    R_tp=(np.array(a[a.devicetype=='toiletPaper'].roll_width))[0]
except:
    print('toiletPaper not available')
db_con=fetch_stream.DataBase_Connection()
for index,rows in z.iterrows():
    #print(rows)
    cn=rows.clinetname
    areaId= rows.areaid
    dev_type=rows.devicetype
    ct=rows.timezone
    client_id =rows.clientid
    tf=rows.timeLF
    tfd=rows.timeFD
    client_type=rows.type
    pub_time=rows.pub_time
    proc_id=rows.processing_id
    ct_=int(ct)
    dt_client = datetime.utcnow() - timedelta(hours=ct_)
    dt_client_=dt_client - timedelta(minutes=120)
    sd=dt_client_.strftime('%Y-%m-%d %H:%M:%S')
    ed=dt_client.strftime('%Y-%m-%d %H:%M:%S')
    s_date_time,s_Date,s=fetch_stream.time_formation(sd)
    e_date_time,e_Date,e_time=fetch_stream.time_formation(ed)
    query1="select * from paper_towel where date_time >= '"+s_date_time+"' and client_id = "+client_id+" and area_id= "+areaId+";"
    query2="select * from toilet_paper where date_time >= '"+s_date_time+"' and client_id = "+client_id+" and area_id= "+areaId+";"
    query3="select * from trash_bin where date_time >= '"+s_date_time+"' and client_id = "+client_id+" and area_id= "+areaId+";"
    query4="select * from uncleanliness_factor where area_id="+areaId+" and client_id="+client_id+";"
    query5="select * from lm_coefficient_table where client_id="+client_id+" and area_id="+areaId+";"
    query6="select * from traffic_data where client_id="+client_id+" and area_id="+areaId+" and date_time>='"+s_date_time+"';"
    try:
        with db.cursor() as cursor:
            cursor.execute(query1)
            Paper_1=pd.read_sql(query1,db)
            cursor.execute(query2)
            Toilet_2=pd.read_sql(query2,db)
            cursor.execute(query3)
            Trash_3=pd.read_sql(query3,db)
            cursor.execute(query4)
            loading_factor=pd.read_sql(query4,db)
            cursor.execute(query5)
            coef=pd.read_sql(query5,db)
            cursor.execute(query6)
            traffic_4=pd.read_sql(query6,db)
    except:
        print("Batch_Data query error")
    query7="select deviceId,deviceName,deviceMacId,deviceSensorId,areaId,areaName,floorId,floorName,buildingId,buildingName,sensorName,batteryValue,rssiValue,Status, percentage as value,sensorValue,lastPeopleCount,washCountValue,deviceTimeStamp,CONVERT_TZ(deviceTimeStamp,'+00:00','"+tf+""+ct+":00') as date from "+cn+".deviceStatus where areaId in ("+areaId+") and CONVERT_TZ(deviceTimeStamp,'+00:00','"+tf+""+ct+":00') between '"+sd+"' and '"+ed+"';"
    df1,df2,df3,df4,df5 = None,None,None,None,None
    Paper_output,Toilet_output,Trash_output,traffic_final,fauset_output =None,None,None,None,None
    try:        
        with dbe.cursor() as cursor:
            cursor.execute(query7)
            df10=pd.read_sql(query7,dbe)
    except:
        print("Query Error")
    try:
        df1=df10.loc[(df10.deviceName.str.contains('PaperTo'))& (df10.value >= 0)]
        df1.reset_index(inplace=True)
        df2=df10.loc[(df10.deviceName.str.contains('Toilet'))& (df10.value >= 0)]
        df2.reset_index(inplace=True)
        df3=df10.loc[(df10.deviceName.str.contains('Trash'))& (df10.value >= 0)]
        df3.reset_index(inplace=True)
        df4=df10.loc[(df10.deviceName.str.contains('People'))]
        df4.reset_index(inplace=True)
        df4['value']=df4['lastPeopleCount']
        df5=df10.loc[(df10.deviceName.str.contains('WaterFlow'))]
        df5.reset_index(inplace=True)
    except:
        print("check the data",areaId)
    if pub_time == 30:
        per_type='half_hourly'
    else:
        per_type='hourly'
    try:
        paperT=fetch_stream.date_time_operation(df1)
        paperT['date_time']=e_date_time
        paperT['Date']=e_Date
        paperT['period_type']=per_type
        paperT['Smoothed_values']=None
        paperT['usage']=None
        paperT['client_id']=np.repeat(client_id,len(paperT))
        paperT['created_date']=pd.to_datetime('now').replace(microsecond=0)
        new=e_date_time.split(" ")
        k=new[1]
        period=fetch_stream.get_period(k)
        paperT['period']=np.repeat(period,len(paperT)) 
        paper_final=paperT[['client_id','areaId', 'deviceName', 'date_time', 'Date','period_type','period', 'value','Smoothed_values', 'usage', 'created_date']]
        paper_final.columns=['client_id','area_id', 'device_id', 'date_time', 'date',  'period_type','period','raw_value','smoothed_value', 'usage', 'created_date']
        Paper_1=Paper_1[['client_id', 'area_id', 'device_id', 'date_time', 'date','period_type', 'period', 'raw_value', 'smoothed_value', 'usage','created_date']]
        Paper_output=fetch_stream.Paper_Prepare_Stream(paper_final,Paper_1,R_pt,e_date_time)
        Paper_output.to_sql(con=db_con, name='paper_towel', if_exists='append',index=False)
    except:
        print("check the paper data",areaId)
    try:
        Tpaper=fetch_stream.date_time_operation(df2)
        Tpaper['date_time']=e_date_time
        Tpaper['Date']=e_Date
        Tpaper['period_type']=per_type
        Tpaper['Smoothed_values']=None
        Tpaper['usage']=None
        Tpaper['client_id']=np.repeat(client_id,len(Tpaper))
        Tpaper['created_date']=pd.to_datetime('now').replace(microsecond=0)
        Tpaper['period']=np.repeat(period,len(Tpaper))  
        toilet_final=Tpaper[['client_id','areaId', 'deviceName', 'date_time', 'Date','period_type','period', 'value','Smoothed_values', 'usage', 'created_date']]
        toilet_final.columns=['client_id','area_id', 'device_id', 'date_time', 'date',  'period_type','period','raw_value','smoothed_value', 'usage', 'created_date']
        Toilet_2=Toilet_2[['client_id', 'area_id', 'device_id', 'date_time', 'date','period_type', 'period', 'raw_value', 'smoothed_value', 'usage','created_date']]
        Toilet_output=fetch_stream.Paper_Prepare_Stream(toilet_final,Toilet_2,R_tp,e_date_time)
        Toilet_output.to_sql(con=db_con, name='toilet_paper', if_exists='append',index=False)
    except:
        print("check the toilet data",areaId)
    try:
        trash=fetch_stream.date_time_operation(df3)
        trash['date_time']=e_date_time
        trash['Date']=e_Date
        trash['period_type']=per_type
        trash['Smoothed_values']=None
        trash['usage']=None
        trash['client_id']=np.repeat(client_id,len(trash))
        trash['created_date']=pd.to_datetime('now').replace(microsecond=0)
        trash['period']=np.repeat(period,len(trash))    
        trash_final=trash[['client_id','areaId', 'deviceName', 'date_time', 'Date','period_type','period', 'value','Smoothed_values', 'usage', 'created_date']]
        trash_final.columns=['client_id','area_id', 'device_id', 'date_time', 'date',  'period_type','period','raw_value','smoothed_value', 'usage', 'created_date']
        Trash_3=Trash_3[['client_id', 'area_id', 'device_id', 'date_time', 'date','period_type', 'period', 'raw_value', 'smoothed_value', 'usage','created_date']]
        Trash_output=fetch_stream.Trash_Prepare_Stream(trash_final,Trash_3,e_date_time)
        Trash_output.to_sql(con=db_con, name='trash_bin', if_exists='append',index=False)
    except:
        print("check the trash data",areaId)
    if len(df4) ==0:
        df4_=traffic_4.iloc[len(traffic_4)-1:,]
        traffic_Cnt=df4_.traffic_count
    elif len(df5)==0:
        df4_=traffic_4.iloc[len(traffic_4)-1:,]
        traffic_Cnt=df4_.traffic_count
    People_count_cn=np.array(['cisco','medstrar'])
    Wash_basin_cn=np.array(['fb'])
    if cn in People_count_cn:
        try:
            traffic=fetch_stream.date_time_operation(df4)
            traffic['date_time']=e_date_time
            traffic['Date']=e_Date
            traffic['period_type']=per_type
            traffic['Smoothed_values']=None
            traffic['usage']=None
            traffic['client_id']=np.repeat(client_id,len(traffic))
            traffic['created_date']=pd.to_datetime('now').replace(microsecond=0)
            traffic['period']=np.repeat(period,len(traffic))
            traffic['value']=np.round(traffic['value'])
            traffic=traffic.fillna(0)
            traffic_final=traffic[['client_id','areaId', 'deviceName', 'date_time', 'Date','period_type','period', 'value', 'created_date']]
            traffic_final.columns=['client_id','area_id', 'device_id', 'date_time', 'date',  'period_type','period','traffic_count', 'created_date']
            traffic_final.to_sql(con=db_con, name='traffic_data', if_exists='append',index=False)
            traffic_Cnt=[traffic_final.traffic_count.sum()]
        except:
            print("check the traffic data",areaId)
    elif cn in Wash_basin_cn:
        try:
            fauset=fetch_stream.date_time_operation(df5)
            fauset.columns=['areaId', 'deviceName', 'Date', 'Time', 'water_usage','traffic_count']
            fauset['water_usage']=fauset['water_usage']/1000
            fauset['client_id']=client_id
            fauset['created_date']=pd.to_datetime('now').replace(microsecond=0)
            fauset['date_']=np.repeat(ed,len(fauset))
            fauset['date_time']=fauset['date_'].astype(str)
            new=fauset['date_time'].str.split(" ",expand=True)
            fauset['date']=new[0]
            fauset['Time']=new[1]
            new=fauset['Time'].str.split(":",expand=True)
            fauset['period']=new[0]
            fauset['period_type']=per_type
            fauset=fauset.fillna(0)
            fauset_output=fauset[['client_id', 'areaId', 'deviceName', 'date_time', 'date', 'period_type','period','traffic_count', 'water_usage', 'created_date']]
            fauset_output.columns=['client_id', 'area_id', 'device_id', 'date_time', 'date', 'period_type','period','traffic_count', 'water_usage', 'created_date']
            fauset_output.to_sql(con=db_con, name='wash_basin', if_exists='append',index=False)
            fauset_output=fauset_output[['client_id', 'area_id', 'device_id', 'date_time', 'date', 'period_type','period','traffic_count','created_date']]
            fauset_output.to_sql(con=db_con, name='traffic_data', if_exists='append',index=False)
            traffic_Cnt=[fauset_output.traffic_count.sum()]
        except:
            print("check the people_count data",areaId)
    try:
        Total_Usage=pd.DataFrame([Trash_output.usage.sum()],columns=['Trash_usage'])
        Total_Usage['Paper_usage']=[Paper_output.usage.sum()]
        Total_Usage['Toilet_usage']=[Toilet_output.usage.sum()]
        if len(traffic_Cnt)==0:
            traffic_Cnt=0
        Total_Usage['Traffic_count']=traffic_Cnt
        Total_Usage['client_id'] = client_id
        Total_Usage['area_id']=areaId
        Total_Usage['period']=period
        Total_Usage['processing_id']=proc_id
        Total_Usage['period_type']='half-hourly'
        Total_Usage['date']=e_Date
        Total_Usage['date_time']=e_date_time
        Total_Usage['created_date']=pd.to_datetime('now').replace(microsecond=0)
        Total_Usage=Total_Usage[['processing_id','client_id', 'area_id','date', 'date_time','period','period_type','Trash_usage', 'Paper_usage', 'Toilet_usage', 'Traffic_count','created_date']]
        Total_Usage.to_sql(con=db_con, name='Aggregation_table_stream', if_exists='append',index=False)
        Cleanliness_Index=pd.DataFrame(fetch_stream.PCA_loading_factor_half_hour(Total_Usage,loading_factor))
        Cleanliness_Index.to_sql(con=db_con, name='Uncleanliness_index', if_exists='append',index=False)
    except:
        print("check the total_usage data",areaId)
    try:
        coef=coef.fillna(0.0)
        current_date=dt_client.timetuple()
        if client_type =='Office':
            if current_date.tm_wday <=4:
                is_holiday=0.0
            else:
                is_holiday=1.0
        else:
            is_holiday=0.0
    except:
        print("holiday variable error")
    try:
        period_type=fetch_stream.period_assign(period)
        weekdays=fetch_stream.weekday_assign(dt_client)
    except:
        print("assign function error")
    
    nk=np.array(coef.feature)
    pred=[]
    feature=[]
    for i in range(0,len(nk)):
        coef_paper=None
        if nk[i]=='papertowel':
            try:
                traffic_lag_,usage_lag=fetch_stream.traffic_usage_lag(traffic_4,Paper_1)
                coef_paper=coef[coef.feature==""+nk[i]+""]
                coef_paper.reset_index(inplace=True)
                predicted=(coef_paper.Intercept+(coef_paper.Monday*weekdays.dow_0)+(coef_paper.Tuesday*weekdays.dow_1)+(coef_paper.Wednesday*weekdays.dow_2)+(coef_paper.Thursday*weekdays.dow_3)+(coef_paper.Friday*weekdays.dow_4)+(coef_paper.Saturday*weekdays.dow_5)+(coef_paper.Sunday*weekdays.dow_6)+(coef_paper.period_0*period_type.period_0)+(coef_paper.period_1*period_type.period_1)+(coef_paper.period_2*period_type.period_2)+(coef_paper.period_3*period_type.period_3)+(coef_paper.period_4*period_type.period_4)+(coef_paper.period_5*period_type.period_5)+(coef_paper.period_6*period_type.period_6)+(coef_paper.period_7*period_type.period_7)+(coef_paper.period_8*period_type.period_8)+(coef_paper.period_9*period_type.period_9)+(coef_paper.period_10*period_type.period_10)+(coef_paper.period_11*period_type.period_11)+(coef_paper.period_12*period_type.period_12)+(coef_paper.period_13*period_type.period_13)+(coef_paper.period_14*period_type.period_14)+(coef_paper.period_15*period_type.period_15)+(coef_paper.period_16*period_type.period_16)+(coef_paper.period_17*period_type.period_17)+(coef_paper.period_18*period_type.period_18)+(coef_paper.period_19*period_type.period_19)+(coef_paper.period_20*period_type.period_20)+(coef_paper.period_21*period_type.period_21)+(coef_paper.period_22*period_type.period_22)+(coef_paper.period_23*period_type.period_23)+(coef_paper.period_24*period_type.period_24)+(coef_paper.period_25*period_type.period_25)+(coef_paper.period_26*period_type.period_26)+(coef_paper.period_27*period_type.period_27)+(coef_paper.period_28*period_type.period_28)+(coef_paper.period_29*period_type.period_29)+(coef_paper.period_30*period_type.period_30)+(coef_paper.period_31*period_type.period_31)+(coef_paper.period_32*period_type.period_32)+(coef_paper.period_33*period_type.period_33)+(coef_paper.period_34*period_type.period_34)+(coef_paper.period_35*period_type.period_35)+(coef_paper.period_36*period_type.period_36)+(coef_paper.period_37*period_type.period_37)+(coef_paper.period_38*period_type.period_38)+(coef_paper.period_39*period_type.period_39)+(coef_paper.period_40*period_type.period_40)+(coef_paper.period_41*period_type.period_41)+(coef_paper.period_42*period_type.period_42)+(coef_paper.period_43*period_type.period_43)+(coef_paper.period_44*period_type.period_44)+(coef_paper.period_45*period_type.period_45)+(coef_paper.period_46*period_type.period_46)+(coef_paper.period_47*period_type.period_47)+(coef_paper.lag1*usage_lag.lag1)+(coef_paper.lag2*usage_lag.lag2)+(coef_paper.lag3*usage_lag.lag3)+(coef_paper.lag4*usage_lag.lag4)+(coef_paper.lag5*usage_lag.lag5)+(coef_paper.lag1_other*traffic_lag_.lag1)+(coef_paper.lag4_other*traffic_lag_.lag4)+(coef_paper.is_holiday*is_holiday))
                predicted=np.round(predicted)
                predicted=predicted
                print("f_paper",predicted)
            except:
                print('forecast_paper_calculation_error')
        elif nk[i]=='toilet_paper':
            try:
                traffic_lag_,usage_lag=fetch_stream.traffic_usage_lag(traffic_4,Toilet_2)
                coef_paper=coef[coef.feature==""+nk[i]+""]
                coef_paper.reset_index(inplace=True)
                predicted=(coef_paper.Intercept+(coef_paper.Monday*weekdays.dow_0)+(coef_paper.Tuesday*weekdays.dow_1)+(coef_paper.Wednesday*weekdays.dow_2)+(coef_paper.Thursday*weekdays.dow_3)+(coef_paper.Friday*weekdays.dow_4)+(coef_paper.Saturday*weekdays.dow_5)+(coef_paper.Sunday*weekdays.dow_6)+(coef_paper.period_0*period_type.period_0)+(coef_paper.period_1*period_type.period_1)+(coef_paper.period_2*period_type.period_2)+(coef_paper.period_3*period_type.period_3)+(coef_paper.period_4*period_type.period_4)+(coef_paper.period_5*period_type.period_5)+(coef_paper.period_6*period_type.period_6)+(coef_paper.period_7*period_type.period_7)+(coef_paper.period_8*period_type.period_8)+(coef_paper.period_9*period_type.period_9)+(coef_paper.period_10*period_type.period_10)+(coef_paper.period_11*period_type.period_11)+(coef_paper.period_12*period_type.period_12)+(coef_paper.period_13*period_type.period_13)+(coef_paper.period_14*period_type.period_14)+(coef_paper.period_15*period_type.period_15)+(coef_paper.period_16*period_type.period_16)+(coef_paper.period_17*period_type.period_17)+(coef_paper.period_18*period_type.period_18)+(coef_paper.period_19*period_type.period_19)+(coef_paper.period_20*period_type.period_20)+(coef_paper.period_21*period_type.period_21)+(coef_paper.period_22*period_type.period_22)+(coef_paper.period_23*period_type.period_23)+(coef_paper.period_24*period_type.period_24)+(coef_paper.period_25*period_type.period_25)+(coef_paper.period_26*period_type.period_26)+(coef_paper.period_27*period_type.period_27)+(coef_paper.period_28*period_type.period_28)+(coef_paper.period_29*period_type.period_29)+(coef_paper.period_30*period_type.period_30)+(coef_paper.period_31*period_type.period_31)+(coef_paper.period_32*period_type.period_32)+(coef_paper.period_33*period_type.period_33)+(coef_paper.period_34*period_type.period_34)+(coef_paper.period_35*period_type.period_35)+(coef_paper.period_36*period_type.period_36)+(coef_paper.period_37*period_type.period_37)+(coef_paper.period_38*period_type.period_38)+(coef_paper.period_39*period_type.period_39)+(coef_paper.period_40*period_type.period_40)+(coef_paper.period_41*period_type.period_41)+(coef_paper.period_42*period_type.period_42)+(coef_paper.period_43*period_type.period_43)+(coef_paper.period_44*period_type.period_44)+(coef_paper.period_45*period_type.period_45)+(coef_paper.period_46*period_type.period_46)+(coef_paper.period_47*period_type.period_47)+(coef_paper.lag1*usage_lag.lag1)+(coef_paper.lag2*usage_lag.lag2)+(coef_paper.lag3*usage_lag.lag3)+(coef_paper.lag4*usage_lag.lag4)+(coef_paper.lag5*usage_lag.lag5)+(coef_paper.lag1_other*traffic_lag_.lag1)+(coef_paper.lag4_other*traffic_lag_.lag4)+(coef_paper.is_holiday*is_holiday))
                predicted=np.round(predicted)
                predicted=predicted+(predicted**2.5)
                print("f_tpaper",predicted)
            except:
                print('forecast_tlt_paper_error')
        elif nk[i]=='trash':
            try:
                traffic_lag_,usage_lag=fetch_stream.traffic_usage_lag(traffic_4,Trash_3)
                coef_paper=coef[coef.feature==""+nk[i]+""]
                coef_paper.reset_index(inplace=True)
                predicted=(coef_paper.Intercept+(coef_paper.Monday*weekdays.dow_0)+(coef_paper.Tuesday*weekdays.dow_1)+(coef_paper.Wednesday*weekdays.dow_2)+(coef_paper.Thursday*weekdays.dow_3)+(coef_paper.Friday*weekdays.dow_4)+(coef_paper.Saturday*weekdays.dow_5)+(coef_paper.Sunday*weekdays.dow_6)+(coef_paper.period_0*period_type.period_0)+(coef_paper.period_1*period_type.period_1)+(coef_paper.period_2*period_type.period_2)+(coef_paper.period_3*period_type.period_3)+(coef_paper.period_4*period_type.period_4)+(coef_paper.period_5*period_type.period_5)+(coef_paper.period_6*period_type.period_6)+(coef_paper.period_7*period_type.period_7)+(coef_paper.period_8*period_type.period_8)+(coef_paper.period_9*period_type.period_9)+(coef_paper.period_10*period_type.period_10)+(coef_paper.period_11*period_type.period_11)+(coef_paper.period_12*period_type.period_12)+(coef_paper.period_13*period_type.period_13)+(coef_paper.period_14*period_type.period_14)+(coef_paper.period_15*period_type.period_15)+(coef_paper.period_16*period_type.period_16)+(coef_paper.period_17*period_type.period_17)+(coef_paper.period_18*period_type.period_18)+(coef_paper.period_19*period_type.period_19)+(coef_paper.period_20*period_type.period_20)+(coef_paper.period_21*period_type.period_21)+(coef_paper.period_22*period_type.period_22)+(coef_paper.period_23*period_type.period_23)+(coef_paper.period_24*period_type.period_24)+(coef_paper.period_25*period_type.period_25)+(coef_paper.period_26*period_type.period_26)+(coef_paper.period_27*period_type.period_27)+(coef_paper.period_28*period_type.period_28)+(coef_paper.period_29*period_type.period_29)+(coef_paper.period_30*period_type.period_30)+(coef_paper.period_31*period_type.period_31)+(coef_paper.period_32*period_type.period_32)+(coef_paper.period_33*period_type.period_33)+(coef_paper.period_34*period_type.period_34)+(coef_paper.period_35*period_type.period_35)+(coef_paper.period_36*period_type.period_36)+(coef_paper.period_37*period_type.period_37)+(coef_paper.period_38*period_type.period_38)+(coef_paper.period_39*period_type.period_39)+(coef_paper.period_40*period_type.period_40)+(coef_paper.period_41*period_type.period_41)+(coef_paper.period_42*period_type.period_42)+(coef_paper.period_43*period_type.period_43)+(coef_paper.period_44*period_type.period_44)+(coef_paper.period_45*period_type.period_45)+(coef_paper.period_46*period_type.period_46)+(coef_paper.period_47*period_type.period_47)+(coef_paper.lag1*usage_lag.lag1)+(coef_paper.lag2*usage_lag.lag2)+(coef_paper.lag3*usage_lag.lag3)+(coef_paper.lag4*usage_lag.lag4)+(coef_paper.lag5*usage_lag.lag5)+(coef_paper.lag1_other*traffic_lag_.lag1)+(coef_paper.lag4_other*traffic_lag_.lag4)+(coef_paper.is_holiday*is_holiday))
                predicted=np.round(predicted)
                predicted=predicted+(predicted**4)
                print("f_trash",predicted)
            except:
                print('forecast trash_error')
        try:
            pred.append(np.array(predicted))
            feature.append(nk[i])
        except:
            print("check the preddicted append variables")
    try:
        forecast=pd.DataFrame(pred,columns=['forecast_value'])
        forecast['device_type']=feature
        forecast=forecast.fillna(0)
    except:
        print('forecast values data frame error')
    
    try:
        Forecast_time=dt_client + timedelta(minutes=30)
        p_dt=Forecast_time.strftime('%Y-%m-%d %H:%M:%S')
        p_date_time,p_Date,p_time=fetch_stream.time_formation(p_dt)
        new=p_date_time.split(" ")
        k=new[1]
        p_period=fetch_stream.get_period(k)
        forecast.set_index(['device_type'],inplace=True)
        forecasted=forecast.T
        forecasted.reset_index(inplace=True)
        forecasted.columns=['index','Toilet_usage','Trash_usage','Traffic_count','Paper_usage']
        forecasted[['client_id','area_id','processing_id','period_type','created_date']]=Total_Usage[['client_id','area_id','processing_id','period_type','created_date']]
        forecasted['period']=p_period
        forecasted['date']=p_Date
        forecasted['date_time']=p_date_time
        forecasted=forecasted.fillna(0.0)
        forecasted=forecasted[['processing_id', 'client_id', 'area_id', 'date', 'date_time','period_type', 'period','Paper_usage','Toilet_usage','Trash_usage','Traffic_count','created_date']]
        forecasted.to_sql(con=db_con, name='forecasted_table', if_exists='append',index=False)
    except:
        print("forecasted table error")
    try:
        Forecast_cleanliness_index=fetch_stream.PCA_loading_factor_half_hour(forecasted,loading_factor)
        Forecast_cleanliness_index.to_sql(con=db_con, name='forecast_uncleanliness_index_test', if_exists='append',index=False)
    except:
        print("forecast cleaning error",areaId)
dbe.close()
db.close()
end_time=pd.to_datetime('now').replace(microsecond=0)
print("total execution time",end_time-strt_time,"of area_id=",areaId,"and client_id=",client_id)
import pandas as pd
import pymysql
import numpy as np
from datetime import datetime, timedelta
from time import gmtime, strftime
import sys
import requests
import sqlalchemy
import mysql
try:
    db = pymysql.connect("****", "***", "****", "****")
    dbe = pymysql.connect("****", "****", "****", "****")
except:
    print("data base connection error")
a = None
try:
    query1="SELECT * FROM Analytics.UncleaningIndex_thresholds where client_id=17 and area_id not in (21,22,30,31,38,39,46,47,54,55,68,72,74,108,112,129);"
    print("db connection sucessfull")
    with db.cursor() as cursor:
        cursor.execute(query1)
        threshold=pd.read_sql(query1,db)
except:
    print("UncleaningIndex_thresholds query error")
for index,rows in threshold.iterrows():
    #print(rows)
    areaId=int(rows.area_id)
    thresh=rows.Cleaning_Index_Threshold
    try:
        query2="select * from  cisco.areaStatus where areaId="+str(areaId)+" order by notificationTime desc limit 1;"
        with dbe.cursor() as cursor:
            cursor.execute(query2)
            areaStatus=pd.read_sql(query2,dbe)
        cleaned_time=str(((areaStatus.notificationTime)-timedelta(hours=7))[0])
        new=cleaned_time.split(" ")
        time=new[1].split(":")
        if int(time[1])>=30:
            time[1]='30'
            time[2]='00'
        if int(time[1])<30:
            time[1]='00'
            time[2]='00'
        cleaned_time=new[0]+' '+time[0]+':'+time[1]+':'+time[2]
    except:
        print("areaStatus query error",areaId)
    try:
        query3="select * from Analytics.Uncleanliness_index where client_id=17 and area_id="+str(areaId)+" and date_time >='"+cleaned_time+"';"
        with db.cursor() as cursor:
            cursor.execute(query3)
            clean_index=pd.read_sql(query3,db)
        clean_index=clean_index.fillna(0) 
        clean_index_areaStatus=sum(clean_index.unclealiness_index)
        cur_clean=clean_index.tail(1)
        cur_clean.reset_index(inplace=True)
        cur_clean_index=cur_clean.unclealiness_index[0]
        query5="select * from Analytics.Unclean_index_validation_one where client_id=17 and area_id="+str(areaId)+" order by date_time desc limit 1;"
        with db.cursor() as cursor:
            cursor.execute(query5)
            cleanliness_index=pd.read_sql(query5,db)
        cleanliness_index=cleanliness_index.fillna(0)
        thresh_clean_past=cleanliness_index.cum_clean_index_Threshold[0]
        #thresh_clean_past_one=cleanliness_index.cum_clean_index_Threshold_one[0]
        if thresh_clean_past >= thresh:
            #print("area_id=",areaId,"for cleanliness_index alert reached")
            thresh_clean_past=0
        thresh_clean_new=thresh_clean_past+cur_clean_index
        if thresh_clean_new >=thresh:
            thresh_clean_new_one=1
            print("area_id=",areaId,"for cleanliness_index alert reached")
        else:
            thresh_clean_new_one=0
    except:
        print("Uncleanliness_index query error",areaId)
    try:
        query4="select status,deviceName,convert_tz(deviceTimestamp,'+00:00','-07:00') as dates from cisco.deviceStatus where areaId="+str(areaId)+" and deviceTimestamp>=convert_tz('"+cur_clean.date_time[0]+"','+00:00','-07:00');"
        with dbe.cursor() as cursor:
            cursor.execute(query4)
            device_status=pd.read_sql(query4,dbe)
        red=device_status[(device_status.status =="High")]
        yellow=device_status[(device_status.status =="Medium")]
        red_device=len(red)
        yellow_device=len(yellow)
        red_deviceN=str(red.deviceName.unique())
        yellow_deviceN=str(yellow.deviceName.unique())
    except:
        print("deviceStatus query error",areaId)
    try:
        #thresh_clean_new=0
        #thresh_clean_new_one=0
        cur_clean['cum_clean_index_areaStatus']=clean_index_areaStatus
        cur_clean['cum_clean_index_Threshold']=thresh_clean_new
        cur_clean['CI_Alert_Status']=thresh_clean_new_one
        cur_clean['red_devices_count']=red_device
        cur_clean['red_devices_name']=red_deviceN
        cur_clean['yellow_devices_count']=yellow_device
        cur_clean['yellow_devices_name']=yellow_deviceN
        cur_clean_=cur_clean[['processing_id', 'client_id', 'area_id', 'date', 'date_time','period_type', 'period', 'scoring', 'unclealiness_index','cum_clean_index_areaStatus','cum_clean_index_Threshold','CI_Alert_Status', 'red_devices_count','red_devices_name','yellow_devices_count','yellow_devices_name', 'created_date']]
    except:
        print("check the final dataframe",areaId)
    try:
        database_username = '***'
        database_password = '***'
        database_ip       = '***'
        database_name     = '***'
        database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))
        cur_clean_.to_sql(con=database_connection, name='Unclean_index_validation_one', if_exists='append',index=False)
        print("completed",areaId)
    except:
        print("data posting error",areaId)
    #break
db.close()
dbe.close()

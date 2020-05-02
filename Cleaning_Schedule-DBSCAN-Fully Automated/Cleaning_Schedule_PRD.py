import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import calendar
from datetime import time
from sklearn.cluster import DBSCAN 
from sklearn.preprocessing import StandardScaler
import pandas as pd
#strt_time=pd.to_datetime('now').replace(microsecond=0)
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
    query1="select * from sample_client_details where clientid='17' and areaid not in (21,22,35,118,57,30,31,38,39,46,47,54,55,68,72,74,108,112,129);"
    print("db connection sucessfull")
    with db.cursor() as cursor:
        cursor.execute(query1)
        samle=pd.read_sql(query1,db)
except:
    print("Client_details_table query error")
samle_=samle[samle.devicetype=='paperTowel']
ae,ad=None,None
threshold=1
query2="SELECT * FROM Analytics.Uncleanliness_index;"# not in ('21',22,30,31,38,39,46,47,54,55,68,72,74,108,112,129);"
print("db connection sucessfull")
with db.cursor() as cursor:
    cursor.execute(query2)
    a_data=pd.read_sql(query2,db)
    print(a_data.shape)
query3="select * from Analytics.UncleaningIndex_thresholds;"# not in ('21',22,30,31,38,39,46,47,54,55,68,72,74,108,112,129);"
print("db connection sucessfull")
with db.cursor() as cursor:
    cursor.execute(query3)
    area_threshold=pd.read_sql(query3,db)
    print(area_threshold.shape)
not_in=[]
not_index=[]
for index,rows in samle_.iterrows():  
    thrsd=threshold
    try:
        try:
            thr=area_threshold[(area_threshold['area_id']==int(rows['areaid']))]
            thr.reset_index(inplace=True)
            threshold=thr['Cleaning_Index_Threshold'].ix[0]
            #print(thr.tail())
        except:
            print('threshold')
            threshold=thrsd
            not_in.append(rows['areaid'])
        try:
            a=a_data[a_data.area_id==int(rows['areaid'])]
            a.reset_index(inplace=True)
            #print(a.tail())
        except:
            not_index.append(rows['areaid'])
            
        #print(rows['areaid'],threshold) 
        period=np.arange(0,48)
        a['period']=pd.Series(np.tile(period,len(a)))
        a['date']=a['date_time'].str.split(" ",expand=True)[0]
        dat=a[['area_id','period','date','unclealiness_index']]
        dat = dat.rename(columns = {"unclealiness_index":"uncln_idx"})
        """ Create moving average of cleaning index"""

        dat['ma_idx'] = dat['uncln_idx'].rolling(3).mean()
        dat['ld_ma'] = dat['ma_idx'].shift(periods = -1)
        
        """ Metric to identify the first cleaning hour of a day"""

        dat['metric'] = dat['ma_idx']*(dat['ld_ma']-dat['ma_idx'])


        """ Remove off hours data"""
        dat = dat[(dat.period<40)&(dat.period>14)]

        """ Find first cleaning hours of a day
            Read Threshold and add a column for cleaning hours based on threshold
            trasform the data in wide format id var as date and columns as cleaning on hours"""

        sum_cln_idx = 0
        fst_cln_hr = 0
        dat['indx'] = range(len(dat))
        dat = dat.set_index('indx')
        dat['cln_hr'] = 0
        dat['cln_seq'] = 0
        cln_no = 0
        for i in range(len(dat)):
            if i == 0:
                if dat['metric'][i]>0:
                    fst_cln_hr = 1
                    dat['cln_hr'][i] = 1
                    cln_no =  1
                    sum_cln_idx = dat['uncln_idx'][i]
                    dat['cln_seq'][i] = cln_no
            else:
                if(dat['date'][i] == dat['date'][i-1]):
                    if(fst_cln_hr == 1):
                        sum_cln_idx = sum_cln_idx + dat['uncln_idx'][i] 
                        if sum_cln_idx >= threshold:
                            dat['cln_hr'][i] = 1
                            cln_no = cln_no + 1
                            sum_cln_idx = 0
                            dat['cln_seq'][i] = cln_no
                    else:
                        if dat['metric'][i]>0:
                            fst_cln_hr = 1
                            dat['cln_hr'][i] = 1
                            cln_no = 1
                            sum_cln_idx = dat['uncln_idx'][i]
                            dat['cln_seq'][i] = cln_no                    
                else:
                    fst_cln_hr = 0
                    sum_cln_idx = 0
                    cln_no = 0
                    if dat['metric'][i]>0:
                        fst_cln_hr = 1
                        dat['cln_hr'][i] = 1
                        cln_no = 1
                        sum_cln_idx = dat['uncln_idx'][i]
                        dat['cln_seq'][i] = cln_no
        dat_clust = dat[['date','period','cln_seq']][dat.cln_hr ==1]
        dow = lambda x:calendar.day_name[x.weekday()]
        dat_clust['date']=pd.to_datetime(dat_clust['date'])
        dat_clust['dow'] = dat_clust['date'].apply(dow)
        """ Run Clustering with DB SCAN"""                          
        wkwnd = (lambda x: 1 if x not in ["Saturday","Sunday"] else 0 )
        dat_clust['isweekday'] = dat_clust['dow'].apply(wkwnd) 
        #print(dat_clust)

        def datForDbSCAN(dat_clust,scdle_type):# scdle_type 'Mon', 'Tue'...., 'wkday','wkend'
            if(scdle_type == 'wkday'):
                dat = np.array(dat_clust[['period','cln_seq']][dat_clust.isweekday==1])
            elif(scdle_type == 'wkend'):
                dat = np.array(dat_clust[['period','cln_seq']][dat_clust.isweekday==0])
            elif(scdle_type == 'Mon'):
                dat = np.array(dat_clust[['period','cln_seq']][dat_clust.dow=='Monday'])
            elif(scdle_type == 'Tue'):
                dat = np.array(dat_clust[['period','cln_seq']][dat_clust.dow=='Tuesday'])
            elif(scdle_type == 'Wed'):
                dat = np.array(dat_clust[['period','cln_seq']][dat_clust.dow=='Wednesday'])
            elif(scdle_type == 'Thu'):
                dat = np.array(dat_clust[['period','cln_seq']][dat_clust.dow=='Thursday'])
            elif(scdle_type == 'Fri'):
                dat = np.array(dat_clust[['period','cln_seq']][dat_clust.dow=='Friday'])
            elif(scdle_type == 'Sat'):
                dat = np.array(dat_clust[['period','cln_seq']][dat_clust.dow=='Satuday'])
            elif(scdle_type == 'Sun'):
                dat = np.array(dat_clust[['period','cln_seq']][dat_clust.dow=='Sunday'])
            return(dat)

        types_=['wkday','Mon','Tue','Wed','Thu','Fri']
        xe,xd=None,None
        for i in range(0,len(types_)):
            try:
                dat= datForDbSCAN(dat_clust,types_[i])#np.array(dat_clust[['period','cln_seq']][dat_clust.isweekday==1])

                dat_Scaled = StandardScaler().fit_transform(dat)

                #plt.scatter(dat[:,0],dat[:,1])
                #plt.show()


                # cluster the data into five clusters
                if  types_[i]=='wkday':
                    dbscan = DBSCAN(eps=.5, min_samples = 15)# parameter needs to be modified as per the data.
                    clusters = dbscan.fit_predict(dat_Scaled)
                else:
                    dbscan = DBSCAN(eps=.7, min_samples = 5)# parameter needs to be modified as per the data.
                    clusters = dbscan.fit_predict(dat_Scaled)
                #clusters
                # plot the cluster assignments
                #plt.scatter(dat[:, 0], dat[:, 1], c=clusters, cmap="plasma")
                #plt.xlabel("Feature 0")
                #plt.ylabel("Feature 1")      

                #len(clusters)
                #len(dat)

                #df_Mon_scored = np.append(df_Mon, clusters, axis = 1)

                dat_scored = {'cln_hr': dat[:,0],
                                         'clstr':clusters}
                dat_scored = pd.DataFrame(data = dat_scored)

                final_scdle = dat_scored.groupby(['clstr']).median()

                final_scdle = final_scdle[final_scdle.index!=-1]

                def tme(x):
                    hr = int(x/2)
                    mnt = int((x%2)*30)
                    sec =0
                    return(time(hr,mnt,sec))

                final_scdle['scdle_time'] = final_scdle['cln_hr'].apply(tme)
                final_scdle=final_scdle.sort_values(by=['cln_hr'])
                final_scdle['fine_tuning']=final_scdle['cln_hr']-final_scdle['cln_hr'].shift(1)
                final_scdle=final_scdle.fillna(5)
                #print(final_scdle)
                Final_scdle=final_scdle[final_scdle['fine_tuning']>3]
                Final_scdle=Final_scdle[Final_scdle['cln_hr']>=16]
                Final_scdle.reset_index(inplace=True)
                Final_scdle['type']=types_[i]
                Final_scdle['area_id']=rows['areaid']
                Final_scdle['client_id']=17
                Final_scdle['created_date']=pd.to_datetime('now').replace(microsecond=0)
                Final_scdle=Final_scdle[['client_id','area_id','scdle_time','type','created_date']]                
                if i==0:
                    xe=Final_scdle
                else:
                    xd=Final_scdle
                    xe=pd.concat([xe,xd],axis=0)
            except:
                print("low data in area")
    except:
        print("data losss")
        xe=None
    try:
        if rows['areaid']=='100':
            ae=xe
        else:
            ad=xe
            ae=pd.concat([ae,ad],axis=0)
    except:
        print("data loss in area")
    #break
    database_username = '****'
    database_password = '****'
    database_ip       = '****'
    database_name     = '****'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))
    #ae.to_sql(con=database_connection, name='Cleanling_Schedule', if_exists='append',index=False)
        
    #break
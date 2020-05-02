

import pymysql
import pandas as pd
import numpy as np
from functools import reduce
from datetime import datetime, timedelta


def date_time_operation(df):
    df['date']=df['date'].astype(str)
    new=df['date'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    new=df['Time'].str.split(":",expand=True)
    df['hour']=new[0]
    df['minute']=new[1]
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where(df['minute']<=29,'00','30')
    df['date_Hhr']=df['Date']+" "+df['hour']+":"+df['minute']+":"+'00'
    df=pd.DataFrame(df.groupby(['areaId','areaName','deviceName','date_Hhr'])['value'].first())
    df.reset_index(inplace=True)
    df['date_Hhr']=df['date_Hhr'].astype(str)
    new=df['date_Hhr'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    gh=df[['areaId','deviceName','Date','Time','value']]
    #gh.reset_index(inplace=True)    
    return gh

def date_time_operation_one_hour(df):
    df['date']=df['date'].astype(str)
    new=df['date'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    new=df['Time'].str.split(":",expand=True)
    df['hour']=new[0]
    df['minute']=new[1]
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where(df['minute']<=59,'00','00')
    df['date_Hhr']=df['Date']+" "+df['hour']+":"+df['minute']+":"+'00'
    df=pd.DataFrame(df.groupby(['areaId','areaName','deviceName','date_Hhr'])['value'].first())
    df.reset_index(inplace=True)
    df['date_Hhr']=df['date_Hhr'].astype(str)
    new=df['date_Hhr'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    gh=df[['areaId','deviceName','Date','Time','value']]
    #gh.reset_index(inplace=True)    
    return gh

def date_time_operation_one_hour_mm_wave(df):
    df['date']=df['date'].astype(str)
    new=df['date'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    new=df['Time'].str.split(":",expand=True)
    df['hour']=new[0]
    df['minute']=new[1]
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where(df['minute']<=59,'00','00')
    df['date_Hhr']=df['Date']+" "+df['hour']+":"+df['minute']+":"+'00'
    df=pd.DataFrame(df.groupby(['areaId','areaName','deviceName','date_Hhr'])['value'].sum())
    df.reset_index(inplace=True)
    df['date_Hhr']=df['date_Hhr'].astype(str)
    new=df['date_Hhr'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    gh=df[['areaId','deviceName','Date','Time','value']]
    #gh.reset_index(inplace=True)    
    return gh

def date_time_operation_five_min(df):
    df['date']=df['date'].astype(str)
    new=df['date'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    new=df['Time'].str.split(":",expand=True)
    df['hour']=new[0]
    df['minute']=new[1]
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=10)&(df['minute']<=14),'10',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=15)&(df['minute']<=19),'15',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=20)&(df['minute']<=24),'20',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=25)&(df['minute']<=29),'25',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=30)&(df['minute']<=34),'30',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=35)&(df['minute']<=39),'35',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=40)&(df['minute']<=44),'40',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=45)&(df['minute']<=49),'45',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=50)&(df['minute']<=54),'50',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=55)&(df['minute']<=59),'55',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=5)&(df['minute']<=9),'05',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where(df['minute']<=4,'00',df['minute'])
    df['minute']=np.where(df['minute']=='5','05',df['minute'])
    df['date_Hhr']=df['Date']+" "+df['hour']+":"+df['minute']+":"+'00'
    df=pd.DataFrame(df.groupby(['areaId','areaName','deviceName','date_Hhr'])['value'].first())
    df.reset_index(inplace=True)
    df['date_Hhr']=df['date_Hhr'].astype(str)
    new=df['date_Hhr'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    gh=df[['areaId','deviceName','Date','Time','value']]
    #gh.reset_index(inplace=True)    
    return gh

def water_flow_data_prepare(df_):
    df_.columns=['areaId','deviceName','Date','Time','Water_Usage']
    df_['Water_Usage']= df_['Water_Usage']/1000
    new=df_['Time'].str.split(":",expand=True)
    df_['hour']=new[0]
    df_['date_']=df_['Date']+" "+df_['hour']+":00:00"
    df_=df_[['areaId','deviceName','date_','Water_Usage']]
    return df_

def traffic_data_prepare(dff):
    new=dff['Time'].str.split(":",expand=True)
    dff['hour']=new[0]
    dff['date_']=dff['Date']+" "+dff['hour']+":00:00"
    df_wc=dff[['areaId','date_','deviceName','value']]
    df_wc.columns=['areaId','date_','deviceName','Wash_Count']
    return df_wc

def data_loss_finder(Final,client_id,areaId,j):
    j.date_time =j.date_time.astype(str)
    Final.date_time=Final.date_time.astype(str)
    ght=np.array(Final.device_id.unique())
    try:
        for i in range(len(ght)):
            print(ght[i])
            j['device_id']=ght[i]
            print(ght[i])
            #jj=j[['date_time','device_id']]
            ghr=pd.merge(j,Final,on=['date_time','device_id'],how="left")
            print(i)
            if i==0:
                rjk=ghr
                #rjk['device_id']=ght[i]
                rjk['client_id']=client_id
                rjk['area_id']=areaId
                d_t=rjk.date_time.str.split(" ",expand=True)
                rjk['date']=d_t[0]
                rjk['created_date']=pd.to_datetime('now').replace(microsecond=0)
            if i>0:
                #ghr['device_id']=ght[i]
                rjk=pd.concat([rjk,ghr],axis=0)
                rjk['client_id']=client_id
                rjk['area_id']=areaId
                d_t=rjk.date_time.str.split(" ",expand=True)
                rjk['date']=d_t[0]
                rjk['created_date']=pd.to_datetime('now').replace(microsecond=0)
        rjk.reset_index(inplace=True)
    except:
        print("fhbf")
    return rjk


def usage_gap_distribution(ghkl,ab):
    paer_tra=pd.merge(ghkl,ab[['date_time','traffic_count']],on=['date_time'],how='left')
    paer_tra['usage']=np.where(paer_tra.traffic_count == 0,0,paer_tra.usage)
    paper_may=paer_tra[paer_tra.date_time >= '2019-05-01 00:00:00']
    paper_may.reset_index(inplace=True)
    jkm=paper_may[paper_may.raw_value.isna()]
    jkm.date_time=pd.to_datetime(jkm.date_time)
    dd=np.array(jkm.index+1)
    dk=np.array(jkm.index)
    z=[]
    z.extend([dd,dk])
    z=np.unique(z)
    n=[]
    for index,row in paper_may.iterrows():
        if index in z:
            n.append(row)
    nl=pd.DataFrame(n)
    nl['repl']=np.where(nl['raw_value'] >= 0,1,np.nan)
    nl['count_1'] = nl.groupby((nl['repl'] == 1))['repl'].cumsum()
    nl['count_1'] = nl['count_1'].fillna(method='bfill')
    nl['count_1'] = nl['count_1'].fillna(0)
    yuygg = nl.groupby(['count_1'])['traffic_count'].sum()
    yuygg = pd.DataFrame(yuygg)
    yuygg.reset_index(inplace=True)
    usage_sum = yuygg#[['usage','sum']]
    usage_sum.columns=['count_1','sum']
    frt=pd.merge(nl,usage_sum,on=['count_1'],how='left')
    frt.usage=frt.usage.fillna(method='bfill')
    frt['ratio']=frt['sum']/frt['traffic_count']
    frt['new_usage']=frt['usage']/frt['ratio']
    frt.new_usage=frt.new_usage.fillna(0)
    frtt=frt[['date_time','device_id','new_usage']]
    llmj=pd.merge(frtt,paper_may,on=['date_time','device_id'],how='right')
    llmj['usage_new']=np.where(llmj.new_usage >=0,llmj.new_usage,llmj.usage)
    llmj=llmj.sort_values(['device_id','date_time'],ascending=True)
    ljgh=llmj[['client_id', 'area_id','device_id','date_time', 'date', 'period_type','period', 'raw_value', 'smoothed_value','usage_new','created_date']]
    ljgh.columns=['client_id', 'area_id','device_id','date_time', 'date', 'period_type','period', 'raw_value', 'smoothed_value','usage','created_date']
    return ljgh
def inproper_data_finder(pr1):
    df_pl=pr1[(pr1.deviceName.str.contains("PaperTowel")) |(pr1.deviceName.str.contains("ToiletPaper")) ]
    a=df_pl
    a['in_prop'] = a.groupby('deviceName')['value'].diff(-1) * (1)
    a['in_prop']=np.where((a.in_prop > -20) & (a.in_prop < 0) ,1111,a.in_prop)
    bn=a[a.in_prop == 1111]        
    kr=bn.groupby('deviceName')['in_prop'].count()
    if len(bn) ==0:
        kr=np.matrix(0)
    k=pd.DataFrame(bn.groupby('deviceName')['in_prop'].count())
    k.reset_index(inplace=True)
    in_pro=pd.DataFrame(np.matrix(k['in_prop']))
    if len(a) == 0:
        #print("jbdbd")
        df_pl=pr1[(pr1.deviceName.str.contains("TrashLevel"))]
        a=df_pl
        a['in_prop'] = a.groupby('deviceName')['value'].diff(-1) * (1)
        a['in_prop']=np.where((a.in_prop < 20) & (a.in_prop > 0) ,1111,a.in_prop)
        bn=a[a.in_prop == 1111]
        kr=bn.groupby('deviceName')['in_prop'].count()
        if len(bn) ==0:
            kr=np.matrix(0)
        k=pd.DataFrame(bn.groupby('deviceName')['in_prop'].count())
        k.reset_index(inplace=True)
        in_pro=pd.DataFrame(np.matrix(k['in_prop']))
    return in_pro,k,kr


def duplication_finder_half_hour(df):
    df['date']=df['date'].astype(str)
    new=df['date'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    new=df['Time'].str.split(":",expand=True)
    df['hour']=new[0]
    df['minute']=new[1]
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where(df['minute']<=29,'00','30')
    df['date_Hhr']=df['Date']+" "+df['hour']+":"+df['minute']+":"+'00'
    kli=pd.DataFrame(df.groupby(['deviceName','date_Hhr'])['deviceId'].count())
    klr=kli[kli.deviceId>1]
    klr.reset_index(inplace=True)
    klr_sum=klr.deviceId.sum()-klr.shape[0]
    klr['deviceId']=klr.deviceId - 1
    klr_=pd.DataFrame(klr.groupby('deviceName')['deviceId'].sum())
    klr_.deviceId=klr_.deviceId.astype(str)
    klr_.reset_index(inplace=True)
    klr_t=pd.DataFrame(np.matrix(klr_['deviceId']))
    return klr_t,klr_sum,klr_

def data_assesment_device(inproper_data_finder,result_,df,pr1):
    k=np.unique(result_.device_id)
    for i in range(0,len(k)):
        ki=result_[result_.device_id == k[i]]
        kf=df[df.deviceName == k[i]]
        kp1=pr1[pr1.deviceName == k[i]]
        data_ass_Dev=None
        data_ass_Dev=pd.DataFrame(ki['client_id'].unique(),columns=['client_id'])
        data_ass_Dev['area_id']=ki['area_id'].unique()
        data_ass_Dev['device_id']=ki['device_id'].unique()
        if len(data_ass_Dev[(data_ass_Dev['device_id'].str.contains('PaperTo'))]) >0:
            data_ass_Dev['device_type']="PaperTowel"
        elif len(data_ass_Dev[(data_ass_Dev['device_id'].str.contains('Toilet'))]) >0:
            data_ass_Dev['device_type']="ToiletPaper"
        elif len(data_ass_Dev[(data_ass_Dev['device_id'].str.contains('Trash'))]) >0:
            data_ass_Dev['device_type']="Trashbin"
        data_ass_Dev['start_date']=ki['date'].min()
        data_ass_Dev['end_date']=ki['date'].max()
        data_ass_Dev['gap_count']=ki.raw_value.isnull().sum()
        dc=kf.groupby(['deviceName','date_Hhr'])['deviceId'].count()-1
        data_ass_Dev['dup_count']=dc.sum()
        data_ass_Dev['length_rawData']=len(kf)
        data_ass_Dev['total_period']=ki.shape[0]/ki.device_id.nunique()
        data_ass_Dev['perfect_record']=kf.shape[0]-dc.sum()
        in_prop,inprop,inp=inproper_data_finder(kp1)
        print(in_prop,inprop)
        data_ass_Dev['in_prop_cnt']=inp[0]
        data_ass_Dev['raw_value_>100']=len(ki[ki.raw_value > 100])
        data_ass_Dev['raw_value_mm']=str(np.min(ki.raw_value))+"-"+str(np.max(ki.raw_value))
        data_ass_Dev['smoothed_value_mm']=str(np.min(ki.smoothed_value))+"-"+str(np.max(ki.smoothed_value))
        data_ass_Dev['usage_mm']=str(np.min(ki.usage))+"-"+str(int(np.max(ki.usage)))
        data_ass_Dev['created_date']=pd.to_datetime('now').replace(microsecond=0)
        if i==0:
            tlt_ass_dev=data_ass_Dev
        else:
            tlt_ass_dev=pd.concat([data_ass_Dev,tlt_ass_dev],axis=0)
    return tlt_ass_dev

def merge_device_count_proc(result_):
    kj=result_[(result_.raw_value.isna())]
    xb=pd.DataFrame(kj.groupby('device_id')['device_id'].count())
    #xb.reset_index(inplace=True)
    xb.device_id=xb.device_id.astype(str)
    q=pd.DataFrame(np.matrix(xb.index))
    r=pd.DataFrame(np.matrix(xb['device_id']))
    return q,r,xb

def data_assesment(areaId,z,merge_device_count_proc,result_,duplication_finder_half_hour,df,inproper_data_finder,pr1):
    try:
        zk=z[z.areaid == ""+areaId+""]
        data_ass=zk[['clientid','clinetname','areaid','devicetype']]
        data_ass['start_date']=np.min(pr1['Date'])
        data_ass['end_date'] = np.max(pr1['Date'])
        data_ass['device_count']=df.deviceName.nunique()
        dev_id,gap_cnt,gap=merge_device_count_proc(result_)
        gap.rename(columns={'device_id':'gap_count'},inplace=True)
        gap.reset_index(inplace=True)
        gap.rename(columns={'device_id':'deviceName'},inplace=True)        
        dev_id['device_id']=np.matrix(dev_id[dev_id.columns[0:]].apply(lambda x: ', '.join(x.dropna().astype(str)),axis=1))
        gap_cnt['gap_cnt']=np.matrix(gap_cnt[gap_cnt.columns[0:]].apply(lambda x: ', '.join(x.dropna().astype(str)),axis=1))
        data_ass['device_id']=dev_id['device_id'][0]#merge_device_count_proc_2(dev_id)[0]
        data_ass['gap_count_dev']=gap_cnt['gap_cnt'][0]#merge_device_count_proc_2(gap_cnt)[0]
        data_ass['total_gap_cnt']=result_.raw_value.isnull().sum()
        dup,sum_dup,duplication=duplication_finder_half_hour(df)
        duplication.rename(columns={'deviceId':'dup_count'},inplace=True)
        dup['dup_cnt']=np.matrix(dup[dup.columns[0:]].apply(lambda x: ', '.join(x.dropna().astype(str)),axis=1))
        data_ass['dup_count_dev']=dup['dup_cnt'][0]#merge_device_count_proc_2(dup)[0]
        data_ass['total_dup_cnt']=sum_dup
        data_ass['lengt h_raw_data']=df.shape[0]
        data_ass['total_periods']=result_.shape[0]/result_.device_id.nunique()
        data_ass['perfect_records_raw']=df.shape[0]-sum_dup
        in_prop,inprop,inp=inproper_data_finder(pr1)
        print(inprop,inp)
        data_ass['inproper_data_cnt']=np.matrix(in_prop[in_prop.columns[0:]].apply(lambda x: ', '.join(x.dropna().astype(str)),axis=1))
        data_ass['raw_data_greater_than_100']=len(df[df.value > 100])
        data_ass['raw_data_min_max']=str(np.min(result_.raw_value))+"-"+str(np.max(result_.raw_value))
        data_ass['smoothed_value_min_max']=str(np.min(result_.smoothed_value))+"-"+str(np.max(result_.smoothed_value))
        data_ass['usage_min_max']=str(np.min(result_.usage))+"-"+str(int(np.max(result_.usage)))
        data_ass['usage_gap_distribution']="Traffic_Count"
        data_ass['Dev_Pub_Time']="30 mins"
        data_ass['other']=len(result_[result_.usage>0])
        data_ass['inproper_data_cnt']=data_ass['inproper_data_cnt'].fillna(0)
        #print(data_ass)
    except:
        print("check data ass part")
    return data_ass
    

def date_time_operation_ten_min(df):
    df['date']=df['date'].astype(str)
    new=df['date'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    new=df['Time'].str.split(":",expand=True)
    df['hour']=new[0]
    df['minute']=new[1]
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=10)&(df['minute']<=19),'10',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=20)&(df['minute']<=29),'20',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=30)&(df['minute']<=39),'30',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=40)&(df['minute']<=49),'40',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=50)&(df['minute']<=59),'50',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where(df['minute']<=9,'00',df['minute'])
    df['date_Hhr']=df['Date']+" "+df['hour']+":"+df['minute']+":"+'00'
    df=pd.DataFrame(df.groupby(['areaId','areaName','deviceName','date_Hhr'])['value'].first())
    df.reset_index(inplace=True)
    df['date_Hhr']=df['date_Hhr'].astype(str)
    new=df['date_Hhr'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    gh=df[['areaId','deviceName','Date','Time','value']]
    #gh.reset_index(inplace=True)    
    return gh


def washbasin_final_tune(Final):
    Final.date_=Final.date_.astype(str)
    new=Final.date_.str.split(" ",expand=True)
    Final['date']=new[0]
    Final['time']=new[1]
    new=Final.time.str.split(":",expand=True)
    Final['hour']=new[0]
    Final['created_date']=pd.to_datetime('now').replace(microsecond=0)
    Final=Final[['areaId',  'deviceName', 'date_','date','hour','Wash_Count', 'Water_Usage',  'created_date']]
    Final.columns=['area_id','device_id','date_time','date','hour','traffic_count','water_usage','created_date']
    return Final



def trash_clean(gh):
    df_tl=gh[(gh.deviceName.str.contains("TrashLevel"))]
    a=df_tl
        
    a['dif_5'] = a.groupby('deviceName')['value'].diff(1) * (-1)
    a.loc[a['dif_5'] < 0, 'dif_5'] = 0
    a['dif_5']=a['dif_5'].abs()
    a.loc[a['dif_5'] >= 15, 'dif_5'] = 0
    a['value_5']=a['value']+a['dif_5']
    
    
    a['dif_6'] = a.groupby('deviceName')['value_5'].diff(1) * (-1)
    a.loc[a['dif_6'] < 0, 'dif_6'] = 0
    a['dif_6']=a['dif_6'].abs()
    a.loc[a['dif_6'] >= 15, 'dif_6'] = 0
    a['value_6']=a['value_5']+a['dif_6']

    a['dif_7'] = a.groupby('deviceName')['value_6'].diff(1) * (-1)
    a.loc[a['dif_7'] < 0, 'dif_7'] = 0
    a['dif_7']=a['dif_7'].abs()
    a.loc[a['dif_7'] >= 15, 'dif_7'] = 0
    a['Smoothed_values']=a['value_6']+a['dif_7']
    
    
    a[['Smoothed_values']]=a.groupby('deviceName')['Smoothed_values'].fillna(a['value'])
    a=a[['areaId', 'deviceName', 'Date','Time', 'value','Smoothed_values']]
    return a

def optimize_smoothed_error_count_trash(df):
    paper_srt = df.sort_values(['client_id', 'area_id', 'device_id','date_time','date', 'period_type','period'])
    #paper_10= paper_srt[(paper_srt.date_time >= '2019-04-10 00:00:00')]
    a=paper_srt
    a['diffrnce'] = a.groupby('device_id')['smoothed_value'].diff(1) * (1)
    a['0_neg'] = np.where(a.diffrnce < -15 , 1 , a.diffrnce)
    a['pos'] = np.where(a['0_neg'] >=0 , 1 , a['0_neg'])
    a['final'] = np.where(a['0_neg'] < 0 , 0 , a['pos'])
    k=len(a[a['final']==0])
    h=30
    while(k>h):
        a['dif_7'] = a.groupby('device_id')['smoothed_value'].diff(1) * (-1)
        a.loc[a['dif_7'] < 0, 'dif_7'] = 0
        a['dif_7']=a['dif_7'].abs()
        a.loc[a['dif_7'] >= 15, 'dif_7'] = 0
        a['Smoothed_values']=a['smoothed_value']+a['dif_7']
        a['smoothed_value']=a.groupby('device_id')['Smoothed_values'].fillna(a['smoothed_value'])

        a['diffrnce'] = a.groupby('device_id')['smoothed_value'].diff(1) * (1)
        a['0_neg'] = np.where(a.diffrnce < -15 , 1 , a.diffrnce)
        a['pos'] = np.where(a['0_neg'] >=0 , 1 , a['0_neg'])
        a['final'] = np.where(a['0_neg'] < 0 , 0 , a['pos'])
        k=len(a[a['final']==0])
        print(k)
        a=a[['client_id', 'area_id', 'device_id', 'date_time', 'date', 'period_type','period', 'raw_value', 'smoothed_value', 'usage', 'created_date']]
    #a.usage=np.where(a.smoothed_value > 150,0,a.usage)
    return a


def Final_clean(Gigar_4):
    paper_srt = Gigar_4.sort_values(['date_time','period'])
    paper_srt[['client_id','area_id','device_id','created_date']]=paper_srt[['client_id','area_id','device_id','created_date']].fillna(method='bfill')
    #paper_srt[['raw_value','smoothed_value']]=paper_srt.groupby('device_id')['raw_value','smoothed_value'].fillna(method='ffill')
    #paper_srt['usage']=paper_srt['usage'].fillna(0)
    paper_srt['date_time']=pd.to_datetime(paper_srt['date_time'])
    paper_srt['date']=paper_srt['date'].fillna(paper_srt['date_time'].dt.date)
    paper_srt['usage']=np.where(paper_srt.smoothed_value==255,0,paper_srt.usage)
    paper_srt['smoothed_value']=np.where(paper_srt.smoothed_value==255,0,paper_srt.smoothed_value)
    return paper_srt

def optimize_smoothed_error_count_paper(df):
    #paper
    paper_srt = df.sort_values(['client_id', 'area_id', 'device_id','date_time','date', 'period_type','period'])
    #paper_10= paper_srt[(paper_srt.date_time >= '2019-04-10 00:00:00')]
    a=paper_srt
    import numpy as np
    a['diffrnce'] = a.groupby('device_id')['smoothed_value'].diff(-1) * (1)
    a['0_neg'] = np.where(a.diffrnce < -20 , 1 , a.diffrnce)
    a['pos'] = np.where(a['0_neg'] >=0 , 1 , a['0_neg'])
    a['final'] = np.where(a['0_neg'] < 0 , 0 , a['pos'])
    k=len(a[a['final']==0])
    h=0
    while(k>h):
        a['dif_15'] = a.groupby('device_id')['smoothed_value'].diff(1) * (-1)
        a.loc[a['dif_15'] > 0, 'dif_15'] = 0
        a.loc[a['dif_15'] < -20, 'dif_15'] = 0
        a['dif_15']=a['dif_15'].abs()
        a['smoothed_value_']=a['smoothed_value']-a['dif_15']          
        a['smoothed_value']=a.groupby('device_id')['smoothed_value_'].fillna(a['smoothed_value'])

        a['diffrnce'] = a.groupby('device_id')['smoothed_value'].diff(-1) * (1)
        a['0_neg'] = np.where(a.diffrnce < -20 , 1 , a.diffrnce)
        a['pos'] = np.where(a['0_neg'] >=0 , 1 , a['0_neg'])
        a['final'] = np.where(a['0_neg'] < 0 , 0 , a['pos'])
        k=len(a[a['final']==0])
        print(k)
        a=a[['client_id', 'area_id', 'device_id', 'date_time', 'date', 'period_type','period', 'raw_value', 'smoothed_value', 'usage', 'created_date']]
    #a.usage=np.where(a.smoothed_value > 100,0,a.usage)
    return a


def trash_data_prepare(a):
    a['Trash Replacement'] = a.groupby('deviceName')['Smoothed_values'].diff(1) * (-1)
    a.loc[a['Trash Replacement'] <= 20, 'Trash Replacement'] = 0
    a.loc[a['Trash Replacement'] >= 20, 'Trash Replacement'] = 1

    a['Trash Replacement']=a.groupby('deviceName')['Trash Replacement'].fillna(method='bfill')
    #a.reset_index(inplace=True)
    a=a[['areaId', 'deviceName', 'Date','Time', 'value','Smoothed_values','Trash Replacement']]
    new=a['Time'].str.split(":",expand=True)
    a['hour']=new[0]
    a['date_']=a['Date']+" "+a['hour']+":00:00"
    a['hu']=a.Smoothed_values.diff(1)*(1)
    a['hrly_Use']=np.where(a['Trash Replacement'] == 1,a['Smoothed_values'],a['hu'])
    #a.tail(150)
    a['hrly_Use']=np.where(a['hrly_Use'] <= 0,0,a['hrly_Use'])
    #a.tail(150)
    #a=a.fillna(method='bfill')
    db=pd.DataFrame(a.groupby(['areaId','deviceName','date_'])['value'].first())
    db.reset_index(inplace=True)
    
    abcd=pd.DataFrame(a.groupby(['areaId','deviceName','date_'])['Smoothed_values'].first())
    abcd.reset_index(inplace=True)
    abcm=pd.DataFrame(a.groupby(['areaId','deviceName','date_'])['Trash Replacement','hrly_Use'].sum())
    abcm.reset_index(inplace=True)
    abcm['Trash Replacement']=np.where(abcm['Trash Replacement'] >= 1,1,0)
    #abcm['Trash Replacement'].value_counts()
    abcg=pd.merge(abcm,abcd, on=['areaId','deviceName','date_'], how='outer')
    

    new=abcg['date_'].str.split(" ",expand=True)
    abcg['Date']=new[0]
    abcg['time']=new[1]
    new=abcg['time'].str.split(":",expand=True)
    abcg['hour']=new[0]
    abcg['raw_value']=db['value']
    
    #clientid=int(input("enter the client id , Ex : 2 for bmw (refer: ClientTable)"))
    #abcg['ClientId']=np.repeat(clientid,len(abcg))
    abcg=abcg[['areaId','deviceName','date_', 'Date', 'hour','raw_value','Smoothed_values','hrly_Use']]
    abcg.columns=['area_id','device_id','date_time','date','hour','raw_value','smoothed_value','usage']
    abcg['created_date']=pd.to_datetime('now').replace(microsecond=0)
    jk=abcg.groupby("device_id", as_index=False).apply(lambda x: x.iloc[:-1])   
    return jk




def paper_clean(gh):
    df_pl=gh[(gh.deviceName.str.contains("PaperTowel")) |(gh.deviceName.str.contains("ToiletPaper")) ]
    a=df_pl
    a['dif_5'] = a.groupby('deviceName')['value'].diff(-1) * (1)
    a['dif_5']=a['dif_5'].fillna(0)
    a['dif_5']=np.where(a.dif_5 < -20 ,1,a.dif_5)
    a['value1']=np.where(a.dif_5 >= 0 ,a.value,0)
    a['value2']=np.where(a.dif_5 < 0,a.value.shift(1),0)
    a['value3'] = np.where(a.value1 == 0 , a.value2 , a.value1)

    a['dif_6'] = a.groupby('deviceName')['value3'].diff(-1) * (1)
    a['dif_6']=a['dif_6'].fillna(0)
    a['dif_6']=np.where(a.dif_6 < -20 ,1,a.dif_6)
    a['value4']=np.where(a.dif_6 >= 0 ,a.value3,0)
    a['value5']=np.where(a.dif_6 < 0,a.value3.shift(1),0)
    a['value6'] = np.where(a.value4 == 0 , a.value5 , a.value4)

    a['dif_7'] = a.groupby('deviceName')['value6'].diff(-1) * (1)
    a['dif_7']=a['dif_7'].fillna(0)
    a['dif_7']=np.where(a.dif_7 < -20 ,1,a.dif_7)
    a['value7']=np.where(a.dif_7 >= 0 ,a.value6,0)
    a['value8']=np.where(a.dif_7 < 0,a.value6.shift(1),0)
    a['value9'] = np.where(a.value7 == 0 , a.value8 , a.value7)

    a['dif_9'] = a.groupby('deviceName')['value9'].diff(1) * (-1)
    a.loc[a['dif_9'] > 0, 'dif_9'] = 0
    a['dif_9']=a['dif_9'].abs()
    a.loc[a['dif_9'] >= 20, 'dif_9'] = 0
    a['Smoothed_values_2']=a['value9']-a['dif_9']

    a['dif_10'] = a.groupby('deviceName')['Smoothed_values_2'].diff(1) * (-1)
    a.loc[a['dif_10'] > 0, 'dif_10'] = 0
    a['dif_10']=a['dif_10'].abs()
    a.loc[a['dif_10'] >= 20, 'dif_10'] = 0
    a['Smoothed_values_3']=a['Smoothed_values_2']-a['dif_10']

    a['dif_11'] = a.groupby('deviceName')['Smoothed_values_3'].diff(1) * (-1)
    a.loc[a['dif_11'] > 0, 'dif_11'] = 0
    a['dif_11']=a['dif_11'].abs()
    a.loc[a['dif_11'] >= 20, 'dif_11'] = 0
    a['Smoothed_values_4']=a['Smoothed_values_3']-a['dif_11']

    a['dif_12'] = a.groupby('deviceName')['Smoothed_values_4'].diff(1) * (-1)
    a.loc[a['dif_12'] > 0, 'dif_12'] = 0
    a['dif_12']=a['dif_12'].abs()
    a.loc[a['dif_12'] >= 20, 'dif_12'] = 0
    a['Smoothed_values_5']=a['Smoothed_values_4']-a['dif_12']

    a['dif_13'] = a.groupby('deviceName')['Smoothed_values_5'].diff(1) * (-1)
    a.loc[a['dif_13'] > 0, 'dif_13'] = 0
    a['dif_13']=a['dif_13'].abs()
    a.loc[a['dif_13'] >= 20, 'dif_13'] = 0
    a['Smoothed_values_6']=a['Smoothed_values_5']-a['dif_13']

    a['dif_14'] = a.groupby('deviceName')['Smoothed_values_6'].diff(1) * (-1)
    a.loc[a['dif_14'] > 0, 'dif_14'] = 0
    a['dif_14']=a['dif_14'].abs()
    a.loc[a['dif_14'] >= 20, 'dif_14'] = 0
    a['Smoothed_values_7']=a['Smoothed_values_6']-a['dif_14']

    a['dif_15'] = a.groupby('deviceName')['Smoothed_values_7'].diff(1) * (-1)
    a.loc[a['dif_15'] > 0, 'dif_15'] = 0
    a['dif_15']=a['dif_15'].abs()
    a.loc[a['dif_15'] >= 20, 'dif_15'] = 0
    a['Smoothed_values']=a['Smoothed_values_7']-a['dif_15']
        
    a['dif_15'] = a.groupby('deviceName')['Smoothed_values'].diff(1) * (-1)
    a.loc[a['dif_15'] > 0, 'dif_15'] = 0
    a.loc[a['dif_15'] < -20, 'dif_15'] = 0
    a['dif_15']=a['dif_15'].abs()
    a['Smoothed_values']=a['Smoothed_values']-a['dif_15']
        
    a['dif_15'] = a.groupby('deviceName')['Smoothed_values'].diff(1) * (-1)
    a.loc[a['dif_15'] > 0, 'dif_15'] = 0
    a.loc[a['dif_15'] < -20, 'dif_15'] = 0
    a['dif_15']=a['dif_15'].abs()
    a['Smoothed_values']=a['Smoothed_values']-a['dif_15']
         
    a['dif_15'] = a.groupby('deviceName')['Smoothed_values'].diff(1) * (-1)
    a.loc[a['dif_15'] > 0, 'dif_15'] = 0
    a.loc[a['dif_15'] < -20, 'dif_15'] = 0
    a['dif_15']=a['dif_15'].abs()
    a['Smoothed_values']=a['Smoothed_values']-a['dif_15']
          
    a['dif_15'] = a.groupby('deviceName')['Smoothed_values'].diff(1) * (-1)
    a.loc[a['dif_15'] > 0, 'dif_15'] = 0
    a.loc[a['dif_15'] < -20, 'dif_15'] = 0
    a['dif_15']=a['dif_15'].abs()
    a['Smoothed_values']=a['Smoothed_values']-a['dif_15']
        
    a['dif_15'] = a.groupby('deviceName')['Smoothed_values'].diff(1) * (-1)
    a.loc[a['dif_15'] > 0, 'dif_15'] = 0
    a.loc[a['dif_15'] < -20, 'dif_15'] = 0
    a['dif_15']=a['dif_15'].abs()
    a['Smoothed_values']=a['Smoothed_values']-a['dif_15']
    
    a['dif_15'] = a.groupby('deviceName')['Smoothed_values'].diff(1) * (-1)
    a.loc[a['dif_15'] > 0, 'dif_15'] = 0
    a.loc[a['dif_15'] < -20, 'dif_15'] = 0
    a['dif_15']=a['dif_15'].abs()
    a['Smoothed_values']=a['Smoothed_values']-a['dif_15']
    
    
    a['dif_15'] = a.groupby('deviceName')['Smoothed_values'].diff(1) * (-1)
    a.loc[a['dif_15'] > 0, 'dif_15'] = 0
    a.loc[a['dif_15'] < -20, 'dif_15'] = 0
    a['dif_15']=a['dif_15'].abs()
    a['Smoothed_values']=a['Smoothed_values']-a['dif_15']
    
    
    a['dif_15'] = a.groupby('deviceName')['Smoothed_values'].diff(1) * (-1)
    a.loc[a['dif_15'] > 0, 'dif_15'] = 0
    a.loc[a['dif_15'] < -20, 'dif_15'] = 0
    a['dif_15']=a['dif_15'].abs()
    a['Smoothed_values']=a['Smoothed_values']-a['dif_15']
          
    a['Smoothed_values']=a.groupby('deviceName')['Smoothed_values'].fillna(a['value'])
    a=a[['areaId', 'deviceName', 'Date', 'Time', 'value','Smoothed_values']]
    return a



def date_time_operation_five_min_mm_wave(df):
    df['date']=df['date'].astype(str)
    new=df['date'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    new=df['Time'].str.split(":",expand=True)
    df['hour']=new[0]
    df['minute']=new[1]
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=10)&(df['minute']<=14),'10',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=15)&(df['minute']<=19),'15',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=20)&(df['minute']<=24),'20',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=25)&(df['minute']<=29),'25',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=30)&(df['minute']<=34),'30',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=35)&(df['minute']<=39),'35',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=40)&(df['minute']<=44),'40',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=45)&(df['minute']<=49),'45',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=50)&(df['minute']<=54),'50',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=55)&(df['minute']<=59),'55',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=5)&(df['minute']<=9),'05',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where(df['minute']<=4,'00',df['minute'])
    df['minute']=np.where(df['minute']=='5','05',df['minute'])
    df['date_Hhr']=df['Date']+" "+df['hour']+":"+df['minute']+":"+'00'
    df=pd.DataFrame(df.groupby(['areaId','areaName','deviceName','date_Hhr'])['value'].sum())
    df.reset_index(inplace=True)
    df['date_Hhr']=df['date_Hhr'].astype(str)
    new=df['date_Hhr'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    gh=df[['areaId','deviceName','Date','Time','value']]
    #gh.reset_index(inplace=True)    
    return gh

def date_time_operation_mm_wave(df):
    df['date']=df['date'].astype(str)
    new=df['date'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    new=df['Time'].str.split(":",expand=True)
    df['hour']=new[0]
    df['minute']=new[1]
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where(df['minute']<=29,'00','30')
    df['date_Hhr']=df['Date']+" "+df['hour']+":"+df['minute']+":"+'00'
    df=pd.DataFrame(df.groupby(['areaId','areaName','deviceName','date_Hhr'])['value'].sum())
    df.reset_index(inplace=True)
    df['date_Hhr']=df['date_Hhr'].astype(str)
    new=df['date_Hhr'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    gh=df[['areaId','deviceName','Date','Time','value']]
    #gh.reset_index(inplace=True)    
    return gh
def date_time_operation_fifteen_min_mm_wave(df):
    df['date']=df['date'].astype(str)
    new=df['date'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    new=df['Time'].str.split(":",expand=True)
    df['hour']=new[0]
    df['minute']=new[1]
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=15)&(df['minute']<=29),'15',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=30)&(df['minute']<=44),'30',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=45)&(df['minute']<=59),'45',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=0)&(df['minute']<=14),'00',df['minute'])
    df['date_Hhr']=df['Date']+" "+df['hour']+":"+df['minute']+":"+'00'
    df=pd.DataFrame(df.groupby(['areaId','areaName','deviceName','date_Hhr'])['value'].sum())
    df.reset_index(inplace=True)
    df['date_Hhr']=df['date_Hhr'].astype(str)
    new=df['date_Hhr'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    gh=df[['areaId','deviceName','Date','Time','value']]
    #gh.reset_index(inplace=True)    
    return gh



def date_time_operation_fifteen_min(df):
    df['date']=df['date'].astype(str)
    new=df['date'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    new=df['Time'].str.split(":",expand=True)
    df['hour']=new[0]
    df['minute']=new[1]
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=15)&(df['minute']<=29),'15',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=30)&(df['minute']<=44),'30',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=45)&(df['minute']<=59),'45',df['minute'])
    df['minute']=df['minute'].astype(int)
    df['minute']=np.where((df['minute']>=0)&(df['minute']<=14),'00',df['minute'])
    df['date_Hhr']=df['Date']+" "+df['hour']+":"+df['minute']+":"+'00'
    df=pd.DataFrame(df.groupby(['areaId','areaName','deviceName','date_Hhr'])['value'].first())
    df.reset_index(inplace=True)
    df['date_Hhr']=df['date_Hhr'].astype(str)
    new=df['date_Hhr'].str.split(" ",n=1,expand=True)
    df['Date']=new[0]
    df['Time']=new[1]
    gh=df[['areaId','deviceName','Date','Time','value']]
    #gh.reset_index(inplace=True)    
    return gh


def paper_data_prepare(b):
    #b['Time']=b['Time'].astype(str)
    new=b['Time'].str.split(":",expand=True)
    b['hour']=new[0]
    b['Time']=b['hour']+":00:00"
    db=pd.DataFrame(b.groupby(['areaId','deviceName','Date','Time'])['value'].first())
    db.reset_index(inplace=True)

    b=pd.DataFrame(b.groupby(['areaId','deviceName','Date','Time'])['Smoothed_values'].first())
    b.reset_index(inplace=True)
    #Replacement
    b['Roll Replacement'] = b.groupby('deviceName')['Smoothed_values'].diff(1) * (1)
    b.loc[b['Roll Replacement'] <= 19, 'Roll Replacement'] = 0
    b.loc[b['Roll Replacement'] >= 20, 'Roll Replacement'] = 1

    b['Lag1_Replacement'] =b['a']=b['Roll Replacement'].shift(-1)
    b['diff_value'] = b.groupby('deviceName')['Smoothed_values'].diff(-1) * (1)
    #b['diff_value']=np.where(b['diff_value'] <= -19,0,b.diff_value)
    b['diff_value']=np.where((b['diff_value'] >= -19) & (b['diff_value'] <= 0),0,b.diff_value) 
    b['diff_value']=b['diff_value'].abs()
    b['hrly_Use'] = np.where(b['Lag1_Replacement'] == 1,0,b.diff_value)
    b[['Roll Replacement','hrly_Use']]=b.groupby('deviceName')[['Roll Replacement','hrly_Use']].fillna(method='bfill')
    b.reset_index(inplace=True)
    
    #b.keys()
    b=b[['areaId', 'deviceName', 'Date', 'Time', 'Smoothed_values','Roll Replacement','hrly_Use']]
    b.columns=['areaId', 'deviceName', 'Date', 'Time', 'Smoothed_values','Roll Replacement','hrly_Use']
    b['date_']=b['Date']+" "+b['Time']
    b=b[['areaId','deviceName','date_','hrly_Use','Smoothed_values']]
    b['raw_Value']=db['value']   
    new=b['date_'].str.split(" ",expand=True)
    b['Date']=new[0]
    b['time']=new[1]
    new=b['time'].str.split(":",expand=True)
    b['hour']=new[0]
    bb=b[['areaId','deviceName','date_','Date','hour','hrly_Use','raw_Value','Smoothed_values']]
    #clientid=int(input("enter the client id , Ex : 2 for bmw (refer: ClientTable)"))
    #bb['ClientId']=np.repeat(clientid,len(bb))
    bb=bb[['areaId','deviceName','date_', 'Date', 'hour','raw_Value','Smoothed_values','hrly_Use']]
    bb.columns=['area_id','device_id','date_time','date','hour','raw_value','smoothed_value','usage']
    bb['created_date']=pd.to_datetime('now').replace(microsecond=0)
    #jk=bb.groupby("device_id", as_index=False).apply(lambda x: x.iloc[:-1])
    return bb


def paper_towel_usage_calculation(pr2,R):
    pr2['Lag1_Replacement'] =pr2['Smoothed_values'].shift(-1)
    a1 = pr2['Smoothed_values']/100
    a2 = pr2['Lag1_Replacement']/100
    r = 6.5
    #RR = 80
    RR=R
    pr2['formula_usage'] = (a1 - a2)*((2*r+(a1+a2)*RR))*100/(2*r+RR)
    pr2['formula_usage'] = np.where(pr2['formula_usage']<=0, 0, pr2.formula_usage)
    #pr2=pr2.groupby("deviceName", as_index=False).apply(lambda x: x.iloc[:-2])
    pr2['date_time']=pr2['Date']+" "+pr2['Time']
    pr2=pr2[['areaId','deviceName','date_time','Date','value','Smoothed_values','formula_usage']]
    pr2.columns=['area_id','device_id','date_time','date','raw_value','smoothed_value','usage']
    pr2['created_date']=pd.to_datetime('now').replace(microsecond=0)
    return pr2


def toilet_paper_usage_calculation(pr2,R):
    pr2['Lag1_Replacement'] =pr2['Smoothed_values'].shift(-1)
    a1 = pr2['Smoothed_values']/100
    a2 = pr2['Lag1_Replacement']/100
    r = 6.5
    #RR = 55
    RR=R
    pr2['formula_usage'] = (a1 - a2)*((2*r+(a1+a2)*RR))*100/(2*r+RR)
    pr2['formula_usage'] = np.where(pr2['formula_usage']<=0, 0, pr2.formula_usage)
    #pr2=pr2.groupby("deviceName", as_index=False).apply(lambda x: x.iloc[:-2])
    pr2['date_time']=pr2['Date']+" "+pr2['Time']
    pr2=pr2[['areaId','deviceName','date_time','Date','value','Smoothed_values','formula_usage']]
    pr2.columns=['area_id','device_id','date_time','date','raw_value','smoothed_value','usage']
    pr2['created_date']=pd.to_datetime('now').replace(microsecond=0)
    return pr2


def toiletpaper_data_prepare(b):    
    #b['Time']=b['Time'].astype(str)
    new=b['Time'].str.split(":",expand=True)
    b['hour']=new[0]
    b['Time']=b['hour']+":00:00"
    db=pd.DataFrame(b.groupby(['areaId','deviceName','Date','Time'])['value'].first())
    db.reset_index(inplace=True)
    b=pd.DataFrame(b.groupby(['areaId','deviceName','Date','Time'])['Smoothed_values'].first())
    b.reset_index(inplace=True)
    #Replacement
    b['Roll Replacement'] = b.groupby('deviceName')['Smoothed_values'].diff(1) * (1)
    b.loc[b['Roll Replacement'] <= 19, 'Roll Replacement'] = 0
    b.loc[b['Roll Replacement'] >= 20, 'Roll Replacement'] = 1
    b['Lag1_Replacement'] =b['a']=b['Roll Replacement'].shift(-1)
    b['diff_value'] = b.groupby('deviceName')['Smoothed_values'].diff(-1) * (1)
    #b['diff_value']=np.where(b['diff_value'] >= -19,0,b.diff_value)
    b['diff_value']=np.where((b['diff_value'] >= -19) & (b['diff_value'] <= 0),0,b.diff_value) 
    b['diff_value']=b['diff_value'].abs()
    b['hrly_Use'] = np.where(b['Lag1_Replacement'] == 1,0,b.diff_value)
    b[['Roll Replacement','hrly_Use']]=b.groupby('deviceName')[['Roll Replacement','hrly_Use']].fillna(method='bfill')
    b.reset_index(inplace=True)
    
    #b.keys()
    b=b[['areaId', 'deviceName', 'Date', 'Time', 'Smoothed_values','Roll Replacement','hrly_Use']]
    b.columns=['areaId', 'deviceName', 'Date', 'Time', 'Smoothed_values','Roll Replacement','hrly_Use']
    b['date_']=b['Date']+" "+b['Time']
    b=b[['areaId','deviceName','date_','hrly_Use','Smoothed_values']]
    b['raw_Value']=db['value']   
    new=b['date_'].str.split(" ",expand=True)
    b['Date']=new[0]
    b['time']=new[1]
    new=b['time'].str.split(":",expand=True)
    b['hour']=new[0]
    bb=b[['areaId','deviceName','date_','Date','hour','hrly_Use','raw_Value','Smoothed_values']]
    #clientid=int(input("enter the client id , Ex : 2 for bmw (refer: ClientTable)"))
    #bb['ClientId']=np.repeat(clientid,len(bb))
    bb=bb[['areaId','deviceName','date_', 'Date', 'hour','raw_Value','Smoothed_values','hrly_Use']]
    bb.columns=['area_id','device_id','date_time','date','hour','raw_value','smoothed_value','usage']
    bb['created_date']=pd.to_datetime('now').replace(microsecond=0)
    jk=bb.groupby("device_id", as_index=False).apply(lambda x: x.iloc[:-1])
    return jk


def Trash_Usage_Calculation(a):
    a['Trash Replacement'] = a.groupby('deviceName')['Smoothed_values'].diff(1) * (-1)
    a.loc[a['Trash Replacement'] <= 15, 'Trash Replacement'] = 0
    a.loc[a['Trash Replacement'] >= 15, 'Trash Replacement'] = 1

    a['Trash Replacement']=a.groupby('deviceName')['Trash Replacement'].fillna(method='bfill')
    #a.reset_index(inplace=True)
    a=a[['areaId', 'deviceName', 'Date','Time', 'value','Smoothed_values','Trash Replacement']]
    a['date_time']=a['Date']+" "+a['Time']
    a['hu']=a.Smoothed_values.diff(1)*(1)
    a['hrly_Use']=np.where(a['Trash Replacement'] == 1,a['Smoothed_values'],a['hu'])
    a['hrly_Use']=np.where(a['hrly_Use'] <= 0,0,a['hrly_Use'])
    abcg=a[['areaId','deviceName','date_time', 'Date','value','Smoothed_values','hrly_Use']]
    abcg.columns=['area_id','device_id','date_time','date','raw_value','smoothed_value','usage']
    abcg['created_date']=pd.to_datetime('now').replace(microsecond=0)
    return abcg

def get_period_five_mins(paper_na):
    paper_na['date_time'] = paper_na['date_time'].astype(str)
    date_time_split = paper_na['date_time'].str.split(" ",expand=True)
    paper_na['time'] =date_time_split[1]
    paper_na['time']= paper_na['time'].astype(str)
    time_hour_split = paper_na['time'].str.split(":",expand=True)
    paper_na['hour']=time_hour_split[0]
    paper_na['minutes']=time_hour_split[1]
    paper_na[['hour','minutes']] = paper_na[['hour','minutes']].astype(int)
    paper_na['period'] = (paper_na['hour']*12 + (paper_na['minutes']/5))
    paper_na = paper_na.drop(['time','hour','minutes'],axis=1)
    return paper_na


def datetime_range_calc(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta
        
def get_period(dt):
    hh = int(dt.split(":")[0])
    jj= int(dt.split(":")[1])
    dl=[]
    if hh == 0 :
        ad = 0
        #ls.append(ad)
        if jj==30:
            ad = ad+1
            dl.append(ad)
        else:
            dl.append(ad)
    elif hh > 0 :
        added=hh*2
        #dl.append(added)
        if jj==30:
            added=added+1
            dl.append(added)
        else:
            dl.append(added)
    return dl


def paper_towel_usage_calculation_optimize(pr2,R):
    pr2['Lag1_Replacement'] =pr2['smoothed_value'].shift(-1)
    a1 = pr2['smoothed_value']/100
    a2 = pr2['Lag1_Replacement']/100
    r = 6.5
    #RR = 80
    RR=R
    pr2['usage'] = (a1 - a2)*((2*r+(a1+a2)*RR))*100/(2*r+RR)
    pr2['usage'] = np.where(pr2['usage']<=0, 0, pr2.usage)
    pr2.usage=np.where(pr2.smoothed_value > 100,0,pr2.usage)
    pr2['usage']=pr2['usage'].shift(1)
    pr2=pr2[['client_id', 'area_id', 'device_id', 'date_time', 'date', 'period_type','period', 'raw_value', 'smoothed_value', 'usage', 'created_date']]
    return pr2

def toilet_paper_usage_calculation_optimize(pr2,R):
    pr2['Lag1_Replacement'] =pr2['smoothed_value'].shift(-1)
    a1 = pr2['smoothed_value']/100
    a2 = pr2['Lag1_Replacement']/100
    r = 6.5
    #RR = 55
    RR=R
    pr2['usage'] = (a1 - a2)*((2*r+(a1+a2)*RR))*100/(2*r+RR)
    pr2['usage'] = np.where(pr2['usage']<=0, 0, pr2.usage)
    pr2.usage=np.where(pr2.smoothed_value > 100,0,pr2.usage)
    pr2['usage']=pr2['usage'].shift(1)

    pr2=pr2[['client_id', 'area_id', 'device_id', 'date_time', 'date', 'period_type','period', 'raw_value', 'smoothed_value', 'usage', 'created_date']]
    return pr2

def Trash_Usage_Calculation_optimize(a):
    a['Trash Replacement'] = a.groupby('device_id')['smoothed_value'].diff(1) * (-1)
    a.loc[a['Trash Replacement'] <= 15, 'Trash Replacement'] = 0
    a.loc[a['Trash Replacement'] >= 15, 'Trash Replacement'] = 1
    a['Trash Replacement']=a.groupby('device_id')['Trash Replacement'].fillna(method='bfill')
    a['hu']=a.smoothed_value.diff(1)*(1)
    a['usage']=np.where(a['Trash Replacement'] == 1,a['smoothed_value'],a['hu'])
    a['usage']=np.where(a['usage'] <= 0,0,a['usage'])
    a.usage=np.where(a.smoothed_value > 150,0,a.usage)
    abcg=a[['client_id', 'area_id', 'device_id', 'date_time', 'date', 'period_type','period', 'raw_value', 'smoothed_value', 'usage', 'created_date']]
    return abcg


def Final_clean_People_count(Gigar_4):
    paper_srt = Gigar_4.sort_values(['date_time','period'])
    paper_srt[['client_id','area_id','device_id','created_date']]=paper_srt[['client_id','area_id','device_id','created_date']].fillna(method='bfill')
    #paper_srt[['raw_value','smoothed_value']]=paper_srt.groupby('device_id')['raw_value','smoothed_value'].fillna(method='ffill')
    #paper_srt['usage']=paper_srt['usage'].fillna(0)
    paper_srt['traffic_count']=paper_srt.groupby('period')['traffic_count'].transform(lambda x: x.fillna(int(x.mean())))
    paper_srt['date_time']=pd.to_datetime(paper_srt['date_time'])
    paper_srt['date']=paper_srt['date'].fillna(paper_srt['date_time'].dt.date)
    return paper_srt

def merge_device_count_proc_1(result_):
    kj=result_[(result_.raw_value.isna())]
    np.array(kj.device_id.value_counts())
    xb=pd.DataFrame(kj.device_id.value_counts())
    xb.reset_index(inplace=True)
    xb.device_id=xb.device_id.astype(str)
    q=pd.DataFrame(np.matrix(xb['index']))
    r=pd.DataFrame(np.matrix(xb['device_id']))
    return q,r
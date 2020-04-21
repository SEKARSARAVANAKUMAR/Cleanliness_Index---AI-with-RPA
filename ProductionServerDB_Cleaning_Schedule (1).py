#python C:\Users\RAGHU_ZANCT\Desktop\CleaningScheduleAlgorithm_Duplicatefilter2.py fb 2018-12-05 2018-12-01 true 70 6,7 8 18
import pymysql
import math
import pandas as pd
import scipy.stats
import datetime
from collections import Counter
from dateutil import parser
import sys
from dateutil.relativedelta import relativedelta
import numpy as np

def main():
    global cursor,areaId,notActionToBeTaken,hourList,totalHour,actionToBeTakenPaperLevel,actionToBeTakenPeopleCount,tempFinalHours,finalHourList,actionToBeTaken,highCountTrashLevel,highCountPaperLevel,resultList,paperLevelCountEntropy,peopleCountEntropy,threshold,hourBetweenStart,hourBetweenEnd,hours,areasPassToQuery,client,start,end,strStartData,strEndData,weekend,listAreas,strStart,strEnd,cursor,db

    today_date = parser.parse(datetime.datetime.today().strftime('%Y-%m-%d'))
    sub_months = parser.parse(str(datetime.datetime.today() + relativedelta(months=-1)))
    end = datetime.date( year = 2019, month = 12, day = 10 ) # To 
    #end = datetime.date( year = 2018, month = 12, day = 1 ) # To 
    start = datetime.date( year = 2019, month = 12, day = 20 ) # From 
    strStartData = str(end)
    strEndData = str(start)
    dateDifference = start - end
    dateDifference = dateDifference.days
    db = pymysql.connect("****", "****", "****", "****")
    cursor = db.cursor()
    print("DB connected")
    QueryTogetDbNamesFromMasterDB ="select distinct dbName from client_details where dbName in('cisco');"
    cursor.execute(QueryTogetDbNamesFromMasterDB)
    cursor.close()
    print("cursor closed")
    db.close()
    print("DB closed")
    ListOfDbs = [dbNames[0] for dbNames in cursor.fetchall()]
    for client in ListOfDbs:
        print("entered loop")
        hourBetweenStart = 0 
        hourBetweenEnd = 23
        db = pymysql.connect("db-write.zancompute.com", "zanprduser", "Atla$19ZC", client)
        cursor = db.cursor()
        print(client)
        QueryToCheckWeekEndsEnabled = "select analyticsWeekEndRestrictionFlag from MasterDB.client_details where  dbName='"+client+"';"
        cursor.execute(QueryToCheckWeekEndsEnabled)
        weekend = cursor.fetchone()[0]
        if weekend=='true':
            weekend ='"Saturday", "Sunday"'
            hourBetweenStart = 8
            hourBetweenEnd = 20
            dateDifference = np.busday_count( end, start )
        sqlToGetPeopleCountAreas ="select distinct(areaId) from deviceStatus where deviceName rlike 'WaterFlow|ToiletPaper|PaperTowel';"
        cursor.execute(sqlToGetPeopleCountAreas)
        ListOfAreas = [areas[0] for areas in cursor.fetchall()]
        areasPassToQuery = ",".join(str(x) for x in ListOfAreas)
        threshold = getThresholdValue(areasPassToQuery)
        #threshold = 90
        print(threshold)
        arealist = areasPassToQuery
        listAreas = arealist.split(",")
        finalOP2 =[]
        mapAreaIdandName ={}
        sqlToGetPeopleCountAreas ="select areaId,concat(floorName,'_',areaName) from deviceStatus where areaId in("+areasPassToQuery+") group by areaId;"
        cursor.execute(sqlToGetPeopleCountAreas)
        for areas in cursor.fetchall():
            mapAreaIdandName[areas[0]]=areas[1] 
        strStart = str(hourBetweenStart)
        strEnd = str(hourBetweenEnd)
        hours = [datetime.time(num).strftime("%I:00 %p") for num in range(hourBetweenStart,hourBetweenEnd +1)]
        query = "INSERT INTO cleaningschedule_algorithm(areaId,cleaningHours,cleaningColors,extra) VALUES(%s,%s,%s,%s)"
        deleletquery ="delete from cleaningschedule_algorithm;"
        cursor.execute(deleletquery)
        db.commit()
        for area in listAreas:
            notActionToBeTaken = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
            hourList = notActionToBeTaken
            totalHour = notActionToBeTaken.__len__()
            actionToBeTakenPaperLevel = []
            actionToBeTakenPeopleCount = []
            tempFinalHours = []
            finalHourList = []
            actionToBeTaken = []
            highCountTrashLevel = {}
            highCountPaperLevel = {}
            resultList = []
            temp = 0
            sorted_afterHumulativeSum ={}

            finalOP ={}
            colors = ["#62cd83" for num in range(hourBetweenStart,hourBetweenEnd +1)]
            for d in daterange(start,end):
                if d.strftime("%A") not in weekend:
                    actionToBeTakenPaperLevel = []
                    actionToBeTakenPeopleCount = []
                    tempFinalHours = []
                    finalHourList = []
                    actionToBeTaken = []
                    highCountTrashLevel = {}
                    highCountPaperLevel = {}
                    paperData(d.strftime('%Y-%m-%d'),area)
                    msg = " threshold:{}"
                    #print(msg.format(threshold));
                    peopleCountData(threshold,d.strftime('%Y-%m-%d'),area)
                    setFinalValuesForEntropyCalculation()
                    impurityMeasure = entropyCalculation(actionToBeTaken.__len__(), notActionToBeTaken.__len__())
                    peopleCountEntropy = impurityMeasure - highLowCalculation(actionToBeTakenPeopleCount.__len__(), totalHour - actionToBeTakenPeopleCount.__len__(), actionToBeTaken.__len__() - actionToBeTakenPeopleCount.__len__())
                    paperLevelCountEntropy = impurityMeasure - highLowCalculation(actionToBeTakenPaperLevel.__len__(), totalHour - actionToBeTakenPaperLevel.__len__(), actionToBeTaken.__len__() - actionToBeTakenPaperLevel.__len__())
                    msg = " PaperLevel:{}\n PeopleCount :{}\n actionToBeTaken:{}\n notActionToBeTaken:{}\n impurityMeasure:{}\n peopleCountEntropy:{}\n paperLevelCountEntropy:{}\n actionToBeTaken for:{}"
                    #print(msg.format(actionToBeTakenPaperLevel, actionToBeTakenPeopleCount, actionToBeTaken, notActionToBeTaken, impurityMeasure, peopleCountEntropy, paperLevelCountEntropy, actionToBeTakenPeopleCount))
                    getPaperHighCount(d.strftime('%Y-%m-%d'),area)
                    finalHourList = cleanDecisionList(area)
                    msg = " Final Hours for {} is {}"
                    #print(msg.format(d.strftime('%Y-%m-%d'), finalHourList));
                    for value in finalHourList:
                        resultList.append(value)

            #print();
            msg = " Final Hours for this area {}"
            ##print(msg.format(resultList));
            ##print("RESULT:")
            msg = " Hour {} is occurred {} times"
            resultList.sort()
            ##print("Shorted:")
            #print(resultList)
            addToMap={}
            sorted_by_value={}
            sumRepeatCountValue=0
            for value in Counter(resultList):
                print(msg.format(value, Counter(resultList)[value]))
                addToMap[value]=Counter(resultList)[value]
                sumRepeatCountValue=sumRepeatCountValue+(Counter(resultList)[value])
            print(sumRepeatCountValue)
            print(addToMap)
            lengthOfDict = int(len(addToMap))
            print(lengthOfDict)
            percentageToCheck = round((sumRepeatCountValue/dateDifference*lengthOfDict))
            print(percentageToCheck)
            for k,v in addToMap.items():
                #print(k,v)
                temp = temp+v
                if(round((temp/dateDifference*100) ) >=70):
                    print(k,v)
                    sorted_afterHumulativeSum[k] = v
                    temp = 0
            print(sorted_afterHumulativeSum)
            finalOP["type"]=mapAreaIdandName.get(int(area))
            convertedNumberToTime = [str(datetime.time(k).strftime("%I:00 %p")) for k,v in sorted_afterHumulativeSum.items()]
            for redHour in convertedNumberToTime:
                colors[hours.index(redHour)]='#f3c12b'
            finalOP["xAxis"] = hours
            finalOP["yAxis"] = colors
            finalOP["zAxis"] = convertedNumberToTime
            finalOP2.append(finalOP)
            args = (int(area),str(finalOP["xAxis"]), str(finalOP["yAxis"]),str(finalOP))
            cursor.execute(query, args)
            db.commit()
            print("inserted")
        print(finalOP2)
    cursor.close()
    db.close()
def getThresholdValue(areasPassToQuery):
        print(areasPassToQuery)
        print(strStartData)
        print(strEndData)
        sql = "select peopleCount,areaName,hourOfDay,dayname(clientTime) from peoplecountanalytics where areaId in("+areasPassToQuery+") and date(clientTime) between '"+strStartData+"' and '"+strEndData+"' and dayname(clientTime) not in("+weekend+") and hourOfDay between 0 and 23  order by areaId,dayofweek(clientTime),hourOfDay;"
        #print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        thresholdList = [30, 40, 50, 60, 70, 80, 90, 100,110,120,130,140,150]
        multipleValuesResult = []
        for threshold in thresholdList:
            peopleCount = 0
            redCount = 0
            previousDayName = ""
            areaName = ""
            currentDayName = ""
            cleaningHoursMap = {}
            actionToBeTakenHours = {}
            cleandDataList = {}
            notCleandDataList = {}
            for row in data:
                currentDayName = row[3]
                if(areaName != row[1]):
                    peopleCount = 0
                peopleCount = peopleCount + row[0]
                if(currentDayName != previousDayName or areaName != row[1]):
                    if(previousDayName != ""):
                        cleaningHoursList = []
                        if previousDayName in cleaningHoursMap:
                            cleaningHoursList = cleaningHoursMap[previousDayName]
                        cleaningHoursList.append(redCount)
                        cleaningHoursMap[previousDayName] = cleaningHoursList
                        redCount = 0
                    previousDayName = currentDayName
                    areaName = row[1]
                if(peopleCount >= threshold):
                    actionToBeTakenHoursList = []
                    if previousDayName in actionToBeTakenHours:
                        actionToBeTakenHoursList = actionToBeTakenHours[previousDayName]
                    actionToBeTakenHoursList.append(row[2])
                    actionToBeTakenHours[previousDayName] = actionToBeTakenHoursList
                    redCount = redCount + 1
                    peopleCount = 0   
            cleaningHoursList = []
            if previousDayName in cleaningHoursMap:
                cleaningHoursList = cleaningHoursMap[previousDayName]
            cleaningHoursList.append(redCount)
            cleaningHoursMap[previousDayName] = cleaningHoursList
            for key in cleaningHoursMap:
                cleaningTotal = 0
                notCleanList = []
                cleanList = []
                for value in cleaningHoursMap[key]:
                    cleaningTotal = cleaningTotal + value
                duplicateCleaningTotal = 0
                actionToBeTakenHoursList = []
                if key in actionToBeTakenHours:
                    for value in actionToBeTakenHours[key]:
                        if(actionToBeTakenHours[key].count(value) > 1 and value not in actionToBeTakenHoursList):
                            actionToBeTakenHoursList.append(value)
                            duplicateCleaningTotal = duplicateCleaningTotal + (actionToBeTakenHours[key].count(value) - 1)
                for value in cleaningHoursMap[key]:
                    if(value == 0):
                        value = 1
                    notCleanValue = (cleaningTotal - duplicateCleaningTotal) - value
                    if(notCleanValue < 1):
                        notCleanValue = 0
                    notCleanList.append(notCleanValue)
                    cleanList.append(value)
                cleandDataList[key] = cleanList
                notCleandDataList[key] = notCleanList
            msg = "Key : {}\n ActionToBeTakenHours : {}"
            #print(msg.format(threshold, actionToBeTakenHours));
            getresulthere = getChiSquareData(cleandDataList, notCleandDataList, threshold)
            multipleValuesResult.append(getresulthere)
        df = pd.concat([pd.DataFrame(x) for x in multipleValuesResult], axis=1)
        #print(df)
        #df.to_excel('E:\\ThresholdConvergence.xlsx')
        return int((df.max() - df.min()).idxmin())

def  getChiSquareData(cleandDataList, notCleandDataList, value):
#     notCleandDataList['Wednesday'] = [x+1 if x<=0 else x for x in notCleandDataList['Wednesday']]
    cleandDataList = { x:[1 if i < 1 else i for i in y] for x, y in cleandDataList.items() }
    notCleandDataList = { x:[1 if i < 1 else i for i in y] for x, y in notCleandDataList.items() }
    lst = []
    data = {}
    for day in cleandDataList:
        cleaning = [ cleandDataList[day], notCleandDataList[day] ]
        chi2, p, ddof, expected = scipy.stats.chi2_contingency(cleaning)
        data[day] = round(math.sqrt(chi2), 2)
        lst.append(data)
    finalList = {}
    finalList[str(value)] = lst[0]
    msg = " CleandDataList : {}\n NotCleandDataList : {}\n ChiSquareData : {}"
    #print(msg.format(cleandDataList, notCleandDataList, finalList));
    return finalList


def entropyCalculation(x, y):
    if(x <= 0 or y <= 0):
        return 0
    return -((x / totalHour) * math.log2(x / totalHour)) - ((y / totalHour) * math.log2(y / totalHour))


def highLowCalculation(high, low, temp):
    x = 0
    y = 0
    if(low != 0):
        x = (low / totalHour) * entropyCalculation(temp, low - temp)
    if(high != 0):
        y = (high / totalHour) * entropyCalculation(high, 0)
    return x + y


def paperData(date,area):
    cursor.execute("select hour  from (select min(sv.percentage) percentage, hour(sv.deviceTimeStamp) hour, da.high from deviceStatus ds join sensor_value sv on sv.deviceSensorId=ds.deviceSensorId join device d on d.deviceId = ds.deviceMacId join device_alerts da on da.id= d.deviceAlertId where ds.areaId=" + area + " and ds.sensorName like '%Paper%' and date(sv.deviceTimeStamp)='" + date + "' and sv.percentage>=0 and hour(sv.deviceTimeStamp) between "+strStart+" and "+strEnd+" group by hour(sv.deviceTimeStamp)) a where percentage <= high ")
#     for row in cursor.fetchall():
#         actionToBeTakenPaperLevel.append(row[0]);
#     #print(actionToBeTakenPaperLevel)
    count = 0
    tempValue = 0
    for row in cursor.fetchall():
        if(count == 0 or row[0] - tempValue >1):
            actionToBeTakenPaperLevel.append(row[0])
        tempValue= row[0]
        count = count + 1

def peopleCountData(threshold, date,area):
    cursor.execute("select peopleCount,hourOfDay from peoplecountanalytics where date(clientTime)='" + date + "' and areaId=" + area + " and hourOfDay between "+strStart+" and "+strEnd+" order by hourOfDay;")
    count = 0
    for row in cursor.fetchall():
        count = count + row[0]
        if(count >= threshold):
            count = 0
            actionToBeTakenPeopleCount.append(row[1])


def setFinalValuesForEntropyCalculation():
    for hour in notActionToBeTaken:
        if(hour in actionToBeTakenPaperLevel or hour in actionToBeTakenPeopleCount):
            actionToBeTaken.append(hour)
    for hour in actionToBeTaken:
        notActionToBeTaken.remove(hour)


def getPaperHighCount(date,area):
    cursor.execute("select hour,count(*) from (select sv.deviceSensorId,max(sv.percentage) percentage, hour(sv.deviceTimeStamp) hour, da.high from deviceStatus ds join sensor_value sv on sv.deviceSensorId=ds.deviceSensorId join device d on d.deviceId = ds.deviceMacId join device_alerts da on da.id= d.deviceAlertId where ds.areaId=" + area + " and ds.sensorName like '%Paper%' and date(sv.deviceTimeStamp)='" + date + "' and sv.percentage>=0 and hour(sv.deviceTimeStamp) between "+strStart+" and "+strEnd+" group by hour(sv.deviceTimeStamp),sv.deviceSensorId) a where percentage <= high group by hour;")
    for row in cursor.fetchall():
        highCountPaperLevel[row[0]] = row[1]
    msg = " highCountPaperLevel:{}"
    #print(msg.format(highCountPaperLevel));


def cleanDecisionList(area):
    tempFinalHours = actionToBeTakenPeopleCount
    msg = " tempFinalHours:{}"
    #print(msg.format(tempFinalHours));
    if(paperLevelCountEntropy > peopleCountEntropy):
        #print(" PaperLevel is greater.....");
        for hour in highCountPaperLevel:
            if(hour not in tempFinalHours):
                tempFinalHours.append(hour)
    finalHourList = tempFinalHours
    if(paperLevelCountEntropy == peopleCountEntropy or paperLevelCountEntropy < peopleCountEntropy):
        cursor.execute("select count(*) from deviceStatus where sensorName like '%Paper%' and areaId=" + area + ";")
        totalDeviceCount = cursor.fetchone()[0]
        msg = " totalDeviceCount:{}"
        #print(msg.format(totalDeviceCount));
        for hour in hourList:
            if((hour not in tempFinalHours) and (hour in highCountPaperLevel) and (((highCountPaperLevel[hour] / totalDeviceCount) * 100) > 30)):
                finalHourList.append(hour)
    return finalHourList


def daterange( start_date, end_date ):
    if start_date == end_date:
        for n in range( ( end_date - start_date ).days + 1 ):
            yield start_date + datetime.timedelta( n )
    else:
        for n in range( ( start_date - end_date ).days + 1 ):
            yield start_date - datetime.timedelta( n )

if __name__ == "__main__":
	main()
    

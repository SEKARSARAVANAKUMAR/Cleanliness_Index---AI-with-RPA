import pymysql
import math
import pandas as pd
import scipy.stats

def main():
    global cursor,areaId,notActionToBeTaken,hourList,totalHour,actionToBeTakenPaperLevel,actionToBeTakenPeopleCount,tempFinalHours,finalHourList,actionToBeTaken,highCountTrashLevel,highCountPaperLevel,resultList,paperLevelCountEntropy,peopleCountEntropy,client,start,end,weekend
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
    # areaId = input("Enter areaID: ");
    # date = input("Enter Date(yyyy-MM-dd): "); 
    client = 'cisco'
    start = '2019-12-10'
    end =  '2019-12-20'
    weekend = '""'
    weekendenabled='true'
    if weekendenabled=='false':
        weekend ='"Saturday", "Sunday"'
    #print(weekend)
    db = pymysql.connect("*****", "***", "****", "****")
    cursor = db.cursor()
    threshold = getThresholdValue()
    print(threshold)
    cursor.close()
    db.close()

def getThresholdValue():
        sqlToGetPeopleCountAreas ="select distinct(areaId) from peoplecountanalytics ;"
        cursor.execute(sqlToGetPeopleCountAreas)
        ListOfAreas = [areas[0] for areas in cursor.fetchall()]
        areasPassToQuery = ",".join(str(x) for x in ListOfAreas)
        print(areasPassToQuery)
        sql = "select peopleCount,areaName,hourOfDay,dayname(clientTime) from peoplecountanalytics where areaId in("+areasPassToQuery+")  and date(clientTime) between '"+start+"' and '"+end+"' and dayname(clientTime) not in("+weekend+") and hourOfDay between 0 and 23 order by areaId,dayofweek(clientTime),hourOfDay;"
        #print(sql)
        cursor.execute(sql)
        data = cursor.fetchall()
        thresholdList = [20,30,40,50,60,70,80,90,100,110,120,130,140,150]
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
            print(msg.format(threshold, actionToBeTakenHours))
            getresulthere = getChiSquareData(cleandDataList, notCleandDataList, threshold)
            multipleValuesResult.append(getresulthere)
        df = pd.concat([pd.DataFrame(x) for x in multipleValuesResult], axis=1)
        #print(df)
        df.to_excel('E:\\ThresholdConvergence.xlsx')
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
        print(p,ddof,expected)
        data[day] = round(math.sqrt(chi2), 2)
        lst.append(data)
    finalList = {}
    finalList[str(value)] = lst[0]
    msg = " CleandDataList : {}\n NotCleandDataList : {}\n ChiSquareData : {}"
    print(msg.format(cleandDataList, notCleandDataList, finalList))
    return finalList

if __name__ == "__main__":
    main()
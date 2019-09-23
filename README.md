fetch_stream.py is a module file which has define function for generate cleanliness_index 

Cleanliness_index_stream file has complete code for generate cleanliness_index for smart washrooms. 
This file will genarating cleanliness_index at each half hour period in streamline. It's a automatic process which will run within 2 seconds using RPA architecture.



Processing Steps:
PROCESS 1:
Fetching Sensor percentage or value ,AreaName , AreaId , DeviceId, DeviceName, SensorId, SensorName from DEVICE STATUS Table in Production Database for Paper Towel, Toilet Paper, Trash, People Count with respect to every half an hour period . In case of Washbasin, the data is collected for every one hour period. And also fetching loading factors for calculating cleanliness index from UNCLEANLINESS FACTOR table in Test Server Database. 
PROCESS 2:
	The collected data is processed using various operations like date time operations, Smoothing algorithm, Usage calculation, Data loss finder and Usage gap distribution for data formation. There are separate Python script will run for each device type. The formatted data will get posted in individual tables for each device type in the Test Database. 
PROCESS 3:
	The formatted data will generate Cleanliness Index using loading factors. The calculated Cleanliness index is posted in the Test Server Database. 
PROCESS 4:
	The posted last five period formatted data will be collected on residing tables with coefficient data from LM COEFFICIENT table from the Test server’s which is further used for forecasting usages and traffic count using ML layer. The forecasted usages and traffic is posted in FORECASTED table in the Test Server Database.
Process 5:
          The forecasted usages and traffic count will generate forecast Cleanliness Index with help of linear model coefficient values. The forecasted Cleanliness Index is posted in FORECASTED CLEANLINESS INDEX table in Test Server Database.






For more details about data processign architecture and complete architecture 












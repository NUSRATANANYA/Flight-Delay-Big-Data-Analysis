@echo off
echo ================================================
echo FLIGHT DELAY BIG DATA PROJECT - HDFS STATUS
echo ================================================
echo.

echo [1] Checking Hadoop processes...
jps
echo.

echo [2] Checking project directories in HDFS...
call hdfs dfs -ls /flight_delay_project
echo.

echo [3] Checking input dataset...
call hdfs dfs -ls -h /flight_delay_project/input
echo.

echo [4] Checking MapReduce outputs...
call hdfs dfs -ls /flight_delay_project/mapreduce_output
echo.

echo [5] Checking Pig output directory...
call hdfs dfs -ls /flight_delay_project/pig_output
echo.

echo ================================================
echo STATUS CHECK COMPLETED
echo ================================================
pause
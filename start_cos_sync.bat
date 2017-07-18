@echo off
set cur_dir=%CD%
cd %cur_dir%
set my_java_cp=.;%cur_dir%\dep\*;%cur_dir%\src\main\resources\*
java -cp "%my_java_cp%" com.qcloud.cos.cos_sync.main.CosSyncMain
pause>nul

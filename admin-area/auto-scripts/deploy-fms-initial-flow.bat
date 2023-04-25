cd ../..
@echo on
cls
python deployment.py INIT
@echo =============== FMS-INIT Flow Deployed Successfully. ===============

TIMEOUT /T 15
@echo =============== Init Flow Run Started ==============================
prefect deployment run FMS-INIT/FMS-INIT-JOB
pause
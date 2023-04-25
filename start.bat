cd admin-area
cd auto-scripts

start start-prefect-server.bat
start start-agent.bat
TIMEOUT /T 10
start deploy-fms-initial-flow.bat

pause
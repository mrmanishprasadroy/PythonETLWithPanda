# PythonETLWithPanda

This script simply copy the data from oracle to sql server considering the fact that both the database have same table structure. 
if running on windows use the env in power shell to start celery and celery beat 

$env:FORKED_BY_MULTIPROCESSING = 1; celery -A task worker --loglevel=info
$env:FORKED_BY_MULTIPROCESSING = 1; celery -A task beat --loglevel=info

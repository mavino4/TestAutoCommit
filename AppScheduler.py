import socket
import time
import logging
import os
import sys
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from Configuration.ConfigurationManager import ConfigurationManager
from Loggin.Logger import *
from Infraestructure.Services.NewRecord import * 
from apscheduler.triggers.cron import CronTrigger
log = Logger.CreateLogger(__name__)
sys.path.append(os.path.dirname(__name__))


## Definiendo variables globales
TABLA_MADS          = None
TABLA_STD           = None
NEW_TRAINING        = False 
FINISHED_TRAINING   = False

def NacionalDepartamental():
    log.info('Invoke to Madcalculator')
    A = NewRecord()
    d = GenerateReports()
    # Definiendo controladores
    global TABLA_MADS, TABLA_STD, NEW_TRAINING, FINISHED_TRAINING
    fecha_act = A.Consulta()

    if fecha_act: 
	    A.Diferencial(fecha_act)
	    d.ReportDaily(fecha_act)
	    A.AutoCommit()
	    log.info("Se registro nuevo día")
    else :
    	log.info("No se tienen nuevos días a registrar")

def NuevaConsulta():
    global TABLA_MADS,TABLA_STD, NEW_TRAINING, FINISHED_TRAINING
    log.info('Invoke to Madcalculator')
    A = ConsultaMunicipios()
    B = NewRecord()

    A.Bolivia()
    A.Municipios()
    B.AutoCommit()
    print('SDFG SDH')
    
## Definiendo el trigger bajo el cual se ejecutar� el entrenamiento
## Definiendo el trigger bajo el cual se ejecutar� el entrenamiento
tg_train = CronTrigger( **ConfigurationManager.GetValue("SchedulerConfig"))
tg_copy = CronTrigger( **ConfigurationManager.GetValue("SchedulerCopy"))


if __name__ == '__main__':
    scheduler = BlockingScheduler(job_defaults={'misfire_grace_time': 3600})
    scheduler.add_job(NacionalDepartamental, trigger = tg_train )
    #scheduler.add_job(NuevaConsulta, trigger = tg_copy )
    scheduler.start()

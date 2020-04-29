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
    log.info('Consulta general y departamental')
    A = NewRecord()
    d = GenerateReports()
    fecha_act = A.Consulta()

    if fecha_act: 
        A.Diferencial(fecha_act)
        d.ReportDaily(fecha_act)
        d.TotalDepto()
        d.TotalNacional()
        A.AutoCommit()
        log.info("Se registro nuevo día NACIONAL y DEPARTAMENTAL" )
    else :
        log.info("No se tienen nuevos días a registrar NACIONAL y DEPARTAMENTAL")

def MunicipiosConsulta():
    log.info('Ejecutando la nueva consulta municipal')
    A = ConsultaMunicipios()
    B = NewRecord()

    mapa_id, nro_mun , dia_i , hora_i , ruta_i = A.ExtraerIDmapa()
    # Si el mapa es nuevo o cambiaron la cantidad de municipios
    A.Municipios(mapa_id)
    B.AutoCommit()
    log.info('Se agrego exitosamente la consulta MUNICIPAL')
    
## Definiendo el trigger bajo el cual se ejecutar� el entrenamiento
## Definiendo el trigger bajo el cual se ejecutar� el entrenamiento
tg_NacDep = CronTrigger( **ConfigurationManager.GetValue("SchedulerNacDep"))
tg_Municipal = CronTrigger( **ConfigurationManager.GetValue("SchedulerMunicipal"))


if __name__ == '__main__':
    scheduler = BlockingScheduler(job_defaults={'misfire_grace_time': 3600})
    scheduler.add_job(NacionalDepartamental, trigger = tg_NacDep )
    scheduler.add_job(MunicipiosConsulta, trigger = tg_Municipal )
    scheduler.start()

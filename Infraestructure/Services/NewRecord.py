import requests
import json
import os
import time
import sqlite3
import pandas as pd
from Loggin.Logger import *




class NewRecord(object):
    _logger = None

    def __init__(self):
        self._logger = Logger.CreateLogger(__name__)
    
    def Consulta(self):
        registro_i = requests.get("https://boliviasegura.agetic.gob.bo/wp-content/json/api.php")
        registro_i_json = registro_i.json()
        registro_i_json["fecha"]
        self._logger.info('Se realizo la consulta : {}'.format(registro_i_json["fecha"]))


        # Verificando si es un nuevo día
        conn = sqlite3.connect('BD_COVID19_BOL.sqlite')

        df_t = pd.read_sql_query("""select * 
        from daily_covid19_BO
        where daily_fecha = '{fecha_consulta}'
        order by daily_depto """.format(fecha_consulta = pd.to_datetime(registro_i_json["fecha"]).strftime("%Y-%m-%d")), conn)
        conn.close()

        if not df_t.shape[0] > 0: 

	        # Por departamentos 
	        day_i = {}
	        for key, value in registro_i_json["departamento"].items()  :
	            val_j =  value["contador"]
	            val_j["total"]  = value["total"] 
	            day_i[key] = val_j

	        df_day_i = pd.DataFrame(day_i).T.rename_axis(["depto"]).reset_index()
	        df_day_i["fecha"] = pd.to_datetime(registro_i_json["fecha"]).strftime("%Y-%m-%d")
	        df_day_i["depto"] = df_day_i["depto"].str.upper()
	        df_day_i["activos"] = df_day_i.confirmados - df_day_i.decesos - df_day_i.recuperados
	        self._logger.info(df_day_i)

	        # A nivel nacional
	        bol_i = registro_i_json["contador"]
	        bol_i["total"] = registro_i_json["total"]

	        sr_bol_i = pd.Series(bol_i)
	        sr_bol_i["fecha"] = pd.to_datetime(registro_i_json["fecha"]).strftime("%Y-%m-%d")
	        sr_bol_i["depto"] = "BOL"
	        sr_bol_i["activos"] = sr_bol_i.confirmados - sr_bol_i.decesos - sr_bol_i.recuperados
	        self._logger.info(sr_bol_i)

	        conn = sqlite3.connect('BD_COVID19_BOL.sqlite')
	        cur = conn.cursor()

	        # Insertando los valores
	        valores = [j.to_list() for i,j  in df_day_i.iterrows()]
	        cur.executemany("""INSERT INTO daily_covid19_BO(daily_depto
	        ,daily_total_confirmados
	        ,daily_total_decesos
	        ,daily_total_recuperados
	        ,daily_total_sospechosos
	        ,daily_total_descartados
	        ,daily_total_total
	        ,daily_fecha
	        ,daily_total_activos) VALUES (?,?,?,?,?,?,?,?,?);""", valores)
	        conn.commit()
	        conn.close()

	        return pd.to_datetime(registro_i_json["fecha"]).strftime("%Y-%m-%d")
        else:
        	return False

        
    def Diferencial(self, fecha_actualizacion):
        # Calculando la diferencial con los valores nuevos
        conn = sqlite3.connect('BD_COVID19_BOL.sqlite')

        df_t_1 = pd.read_sql_query("""select * 
        from daily_covid19_BO
        where daily_fecha = strftime('%Y-%m-%d',date('{fecha_consulta}', '-1 day'))
        order by daily_depto """.format(fecha_consulta = fecha_actualizacion), conn)

        df_t = pd.read_sql_query("""select * 
        from daily_covid19_BO
        where daily_fecha = '{fecha_consulta}'
        order by daily_depto """.format(fecha_consulta = fecha_actualizacion), conn)


        conn.close()


        delta = df_t[[  'daily_total_confirmados', 'daily_total_activos',
               'daily_total_decesos', 'daily_total_recuperados',
               'daily_total_sospechosos', 'daily_total_descartados',
               'daily_total_total']].values -   df_t_1[[  'daily_total_confirmados', 'daily_total_activos',
               'daily_total_decesos', 'daily_total_recuperados',
               'daily_total_sospechosos', 'daily_total_descartados',
               'daily_total_total']].values

        delta_df = pd.DataFrame(delta , columns=['daily_delta_confirmados',
        'daily_delta_activos', 'daily_delta_decesos', 'daily_delta_recuperados',
        'daily_delta_sospechosos', 'daily_delta_descartados',
        'daily_delta_total'])

        delta_df[['daily_id', 'daily_fecha', 'daily_depto']] = df_t[['daily_id', 'daily_fecha', 'daily_depto']]

        valores_delta = [j.to_list() for i,j  in delta_df[['daily_delta_confirmados',
        'daily_delta_activos', 'daily_delta_decesos', 'daily_delta_recuperados',
        'daily_delta_sospechosos', 'daily_delta_descartados',
        'daily_delta_total','daily_id', 'daily_fecha', 'daily_depto']].iterrows()]

        conn = sqlite3.connect('BD_COVID19_BOL.sqlite')
        cur = conn.cursor()

        for val_i in valores_delta : 
            cur.execute("""UPDATE  daily_covid19_BO
            SET   
              daily_delta_confirmados = {0}
            , daily_delta_activos     =     {1} 
            , daily_delta_decesos     =     {2}
            , daily_delta_recuperados =  {3}
            , daily_delta_sospechosos =   {4}
            , daily_delta_descartados =   {5}
            , daily_delta_total       =   {6}                   
            where daily_id = {7}
            and daily_fecha = '{8}'
            and daily_depto = '{9}'  """.format(*val_i))
        conn.commit()
        conn.close()
    
    def AutoCommit(self):
        os.system("git add .")
        os.system('git commit -m "Autocommit {}"'.format(time.strftime("%Y%m%d %H%M")))
        os.system("git push -u origin master")
        self._logger.info("Se ejecuto el autocommit correctamente")




class GenerateReports(object):
	_logger = None
	def __init__(self):
		self._logger = Logger.CreateLogger(__name__)

	def ReportDaily(self, report_date):
		conn = sqlite3.connect('BD_COVID19_BOL.sqlite')
		df_t = pd.read_sql_query("""select * 
		from daily_covid19_BO
		where daily_fecha = '{fecha_consulta}'
		order by daily_depto """.format(fecha_consulta = report_date), conn)


		conn.close()

		df_t[['daily_fecha', 'daily_depto', 'daily_total_confirmados', 'daily_total_activos',
		   'daily_total_decesos', 'daily_total_recuperados',
		   'daily_total_sospechosos', 'daily_total_descartados',
		   'daily_total_total']].to_csv("{}.csv".format(report_date),index=False)

		# Confirmed 	Deaths 	Recovered 	Active

		conn = sqlite3.connect('BD_COVID19_BOL.sqlite')

		time_series = pd.read_sql_query("""select daily_fecha, daily_depto,
		daily_total_confirmados, daily_total_activos,
		daily_total_decesos, daily_total_recuperados,
		daily_total_sospechosos, daily_total_descartados,
		daily_total_total
		from daily_covid19_BO
		order by daily_depto """.format(fecha_consulta = report_date), conn)


		conn.close()

		time_series
		confirmados_ts = pd.pivot(time_series,"daily_depto","daily_fecha","daily_total_confirmados").reset_index()
		activos_ts     = pd.pivot(time_series,"daily_depto","daily_fecha","daily_total_activos").reset_index()
		decesos_ts     = pd.pivot(time_series,"daily_depto","daily_fecha","daily_total_decesos").reset_index()
		recuperados_ts = pd.pivot(time_series,"daily_depto","daily_fecha","daily_total_recuperados").reset_index()

		confirmados_ts.to_csv("time_series_covid19_confirmados_BO.csv",index=False)
		activos_ts.to_csv("time_series_covid19_activos_BO.csv",index=False)
		decesos_ts.to_csv("time_series_covid19_decesos_BO.csv",index=False)
		recuperados_ts.to_csv("time_series_covid19_recuperados_BO.csv",index=False)


class ConsultaMunicipios(object):
	_logger = None
	def __init__(self):
		self._logger = Logger.CreateLogger(__name__)

	def Bolivia(self):
		# Decesos
		decesos = requests.get("https://cartocdn-gusc-d.global.ssl.fastly.net/juliael/api/v1/map/juliael@5266c6ad@7bcf5e7830ffcbbf71979fba83fbeacc:1587909430470/dataview/fa6d9d45-c1fb-4a32-a494-b59cb08dd33d")
		time.sleep(1)
		recuperados = requests.get("https://cartocdn-gusc-a.global.ssl.fastly.net/juliael/api/v1/map/juliael@5266c6ad@7bcf5e7830ffcbbf71979fba83fbeacc:1587909430470/dataview/52149491-d719-44f3-b278-0890a1e514f6")
		time.sleep(1)
		positivos = requests.get("https://cartocdn-gusc-a.global.ssl.fastly.net/juliael/api/v1/map/juliael@5266c6ad@7bcf5e7830ffcbbf71979fba83fbeacc:1587909430470/dataview/f1b7b77e-4465-4cca-8b2f-d4ebbdcb21f5")

		BOL = {"date_get" : positivos.headers["date"]
		,"positivos" :positivos.json()["result"] 
		,"decesos" : decesos.json()["result"]
		,"recuperados" : recuperados.json()["result"]
		,"activos" : positivos.json()["result"] -  decesos.json()["result"] - recuperados.json()["result"]
		}

		with open('Bolivia/{}.json'.format(BOL["date_get"]), 'w') as outfile:
			json.dump(BOL, outfile)

	def Municipios(self):
		responses = []
		for i in range(400):
			registro_i = requests.get("https://cartocdn-gusc-c.global.ssl.fastly.net/juliael/api/v1/map/juliael@5266c6ad@7bcf5e7830ffcbbf71979fba83fbeacc:1587909430470/4/attributes/{}".format(i))
			responses.append([i, registro_i.headers["Date"] , registro_i])
			time.sleep(0.5)

		VALUES = {}
		for response_i in responses: 
			if response_i[2].ok:
				dict_i = {}
				dict_i["date_get"] =  response_i[1]
				dict_i.update(response_i[2].json())
				VALUES[str(response_i[0])] = dict_i

		CONSULTA_i = pd.DataFrame(VALUES).T.reset_index()
		CONSULTA_i["date_get"] = pd.to_datetime(CONSULTA_i.date_get)
		CONSULTA_i.fillna(value=0, inplace=True)
		CONSULTA_i["activos"] = CONSULTA_i.positivos - CONSULTA_i.decesos - CONSULTA_i.recuperados

		CONSULTA_i.to_csv("Municipios/{} Municipios.csv".format(CONSULTA_i.date_get.max().strftime("%Y-%m-%d %H:%M")),index=False)
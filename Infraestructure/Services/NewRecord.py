import requests
import json
import os
import time
import sqlite3
import pandas as pd
from Loggin.Logger import *
from selenium import webdriver
import re
from bs4 import BeautifulSoup
import numpy as np
from tqdm import tqdm
from datetime import datetime



class NewRecord(object):
    _logger = None

    def __init__(self):
        self._logger = Logger.CreateLogger(__name__)
    
    def Consulta(self):
        registro_i = requests.get("https://boliviasegura.agetic.gob.bo/wp-content/json/api.php")
        registro_i_json = registro_i.json()
        self._logger.info('Se realizo la consulta : {}'.format(registro_i_json["fecha"]))
        fecha_json = pd.to_datetime(registro_i_json["fecha"], format="%d/%m/%y %H:%M").strftime("%Y-%m-%d")



        # Verificando si es un nuevo día
        conn = sqlite3.connect('BD_COVID19_BOL.sqlite')

        df_t = pd.read_sql_query("""select * 
        from daily_covid19_BO_depto
        where daily_fecha = '{fecha_consulta}'
        order by daily_depto """.format(fecha_consulta = fecha_json), conn)
        conn.close()

        if not df_t.shape[0] > 0: 
            with open('JSON_BoliviaSegura/{}.json'.format(fecha_json), 'w') as outfile :
                json.dump(registro_i_json, outfile)
            self._logger.info("Se guardo la consulta en formato JSON")
	        # Por departamentos 
            day_i = {}
            for key, value in registro_i_json["departamento"].items()  :
	            val_j =  value["contador"]
	            val_j["total"]  = value["total"] 
	            day_i[key] = val_j

            df_day_i = pd.DataFrame(day_i).T.rename_axis(["depto"]).reset_index()
            df_day_i["fecha"] = fecha_json
            df_day_i["depto"] = df_day_i["depto"].str.upper()
            df_day_i["activos"] = df_day_i.confirmados - df_day_i.decesos - df_day_i.recuperados
            self._logger.info(df_day_i)

            # A nivel nacional
            bol_i = registro_i_json["contador"]
            bol_i["total"] = registro_i_json["total"]
            sr_bol_i = pd.Series(bol_i)
            sr_bol_i["fecha"] = fecha_json
            sr_bol_i["depto"] = "BOL"
            sr_bol_i["activos"] = sr_bol_i.confirmados - sr_bol_i.decesos - sr_bol_i.recuperados
            self._logger.info(sr_bol_i)

            # Insertar Valores NACIONAL
            conn = sqlite3.connect('BD_COVID19_BOL.sqlite')
            cur = conn.cursor()
            cur.execute("""INSERT INTO daily_covid19_BO(
            daily_total_confirmados
            ,daily_total_decesos
            ,daily_total_recuperados
            ,daily_total_sospechosos
            ,daily_total_descartados
            ,daily_total_total
            ,daily_fecha
            ,daily_total_activos) VALUES (?,?,?,?,?,?,?,?);""", 
            list(sr_bol_i[['confirmados', 'decesos', 'recuperados',  'sospechosos','descartados',
            'total', 'fecha', 'activos']]))
            conn.commit()
            conn.close()
            # Insertando los valores departamental
            conn = sqlite3.connect('BD_COVID19_BOL.sqlite')
            cur = conn.cursor()
            valores = [list(j) for i,j  in df_day_i[['depto', 'confirmados', 'decesos', 'recuperados', 'sospechosos',
            'descartados', 'total', 'fecha', 'activos']].iterrows()]
            cur.executemany("""INSERT INTO daily_covid19_BO_depto(daily_depto
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
            return fecha_json
        else:
            return False

        
    def Diferencial(self, fecha_actualizacion):
        # Calculando la diferencial con los valores nuevos DEPARTAMENTAL
        conn = sqlite3.connect('BD_COVID19_BOL.sqlite')

        df_t_1 = pd.read_sql_query("""select * 
        from daily_covid19_BO_depto
        where daily_fecha = strftime('%Y-%m-%d',date('{fecha_consulta}', '-1 day'))
        order by daily_depto """.format(fecha_consulta = fecha_actualizacion), conn)

        df_t = pd.read_sql_query("""select * 
        from daily_covid19_BO_depto
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

        valores_delta = [list(j) for i,j  in delta_df[['daily_delta_confirmados',
        'daily_delta_activos', 'daily_delta_decesos', 'daily_delta_recuperados',
        'daily_delta_sospechosos', 'daily_delta_descartados',
        'daily_delta_total','daily_id', 'daily_fecha', 'daily_depto']].iterrows()]

        conn = sqlite3.connect('BD_COVID19_BOL.sqlite')
        cur = conn.cursor()

        for val_i in valores_delta : 
            cur.execute("""UPDATE  daily_covid19_BO_depto
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
        # Añadiendo la diferencial NACIONAL
        conn = sqlite3.connect('BD_COVID19_BOL.sqlite')
        df_t_1 = pd.read_sql_query("""select * 
        from daily_covid19_BO
        where daily_fecha = strftime('%Y-%m-%d',date('{fecha_consulta}', '-1 day'))""".format(fecha_consulta = fecha_actualizacion), conn)
        df_t = pd.read_sql_query("""select * 
        from daily_covid19_BO
        where daily_fecha = '{fecha_consulta}'""".format(fecha_consulta = fecha_actualizacion), conn)
        conn.close()
        a_t = df_t[[  'daily_total_confirmados', 'daily_total_activos',
        	'daily_total_decesos', 'daily_total_recuperados',
        	'daily_total_sospechosos', 'daily_total_descartados',
        	'daily_total_total']].values
        a_t_1 = df_t_1[[  'daily_total_confirmados', 'daily_total_activos',
        	'daily_total_decesos', 'daily_total_recuperados',
        	'daily_total_sospechosos', 'daily_total_descartados',
        	'daily_total_total']].values
        delta =  np.where(a_t== None , 0, a_t)  - np.where(a_t_1== None , 0, a_t_1) 
        val = list(delta[0]) + list(df_t[['daily_id', 'daily_fecha']].values[0])
        conn = sqlite3.connect('BD_COVID19_BOL.sqlite')
        cur = conn.cursor()
        cur.execute("""UPDATE  daily_covid19_BO
        SET   daily_delta_confirmados = {0}
        , daily_delta_activos     =     {1} 
        , daily_delta_decesos     =     {2}
        , daily_delta_recuperados =  {3}
        , daily_delta_sospechosos =   {4}
        , daily_delta_descartados =   {5}
        , daily_delta_total       =   {6}                   
        where daily_id = {7}
        and daily_fecha = '{8}'  """.format(*val))
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

	def ActFec_README(self, file_path, fecha_act):
		A = open(file_path, "r")
		lines_A = A.readlines()
		A.close()

		with  open(file_path, "w+") as file_out :  
			for line in lines_A: 
				if "> Información actualizada al : " in line: 
					file_out.write("> Información actualizada al : {}\n".format(fecha_act))
				else : 
					file_out.write(line)

	def ReportDaily(self, report_date):
		conn = sqlite3.connect('BD_COVID19_BOL.sqlite')

		df_t = pd.read_sql_query("""select * 
		from daily_covid19_BO_depto
		where daily_fecha = '{fecha_consulta}'
		order by daily_depto """.format(fecha_consulta = report_date), conn)


		conn.close()

		df_t[['daily_fecha', 'daily_depto', 'daily_total_confirmados', 'daily_total_activos',
			'daily_total_decesos', 'daily_total_recuperados']].to_csv("daily_report/{}.csv".format(report_date),index=False)

		df_t[['daily_fecha', 'daily_depto', 'daily_delta_confirmados', 'daily_delta_activos',
			'daily_delta_decesos', 'daily_delta_recuperados']].to_csv("daily_report/{}_delta.csv".format(report_date),index=False)
		self._logger.info("Se generaron los reportes diarios")

		# Confirmed 	Deaths 	Recovered 	Active

		conn = sqlite3.connect('BD_COVID19_BOL.sqlite')

		time_series = pd.read_sql_query("""select daily_fecha, daily_depto,
		daily_total_confirmados, daily_total_activos,
		daily_total_decesos, daily_total_recuperados,
		daily_total_sospechosos, daily_total_descartados,
		daily_total_total,
		daily_delta_confirmados, daily_delta_activos,
		daily_delta_decesos, daily_delta_recuperados
		from daily_covid19_BO_depto
		order by daily_depto """.format(fecha_consulta = report_date), conn)

		conn.close()

		time_series
		confirmados_ts = time_series.pivot("daily_depto","daily_fecha","daily_total_confirmados").reset_index()
		activos_ts     = time_series.pivot("daily_depto","daily_fecha","daily_total_activos").reset_index()
		decesos_ts     = time_series.pivot("daily_depto","daily_fecha","daily_total_decesos").reset_index()
		recuperados_ts = time_series.pivot("daily_depto","daily_fecha","daily_total_recuperados").reset_index()

		confirmados_ts = time_series.pivot("daily_depto","daily_fecha","daily_delta_confirmados").reset_index()
		activos_ts     = time_series.pivot("daily_depto","daily_fecha","daily_delta_activos").reset_index()
		decesos_ts     = time_series.pivot("daily_depto","daily_fecha","daily_delta_decesos").reset_index()
		recuperados_ts = time_series.pivot("daily_depto","daily_fecha","daily_delta_recuperados").reset_index()


		confirmados_ts.to_csv("time_series/time_series_covid19_confirmados_BO.csv",index=False)
		activos_ts.to_csv("time_series/time_series_covid19_activos_BO.csv",index=False)
		decesos_ts.to_csv("time_series/time_series_covid19_decesos_BO.csv",index=False)
		recuperados_ts.to_csv("time_series/time_series_covid19_recuperados_BO.csv",index=False)

		confirmados_ts.to_csv("time_series/time_series__delta_covid19_confirmados_BO.csv",index=False)
		activos_ts.to_csv("time_series/time_series__delta_covid19_activos_BO.csv",index=False)
		decesos_ts.to_csv("time_series/time_series__delta_covid19_decesos_BO.csv",index=False)
		recuperados_ts.to_csv("time_series/time_series__delta_covid19_recuperados_BO.csv",index=False)
		self._logger.info("Se actualizaron las serie de tiempo")
	
	def TotalDepto(self):
		conn = sqlite3.connect('BD_COVID19_BOL.sqlite')

		time_series = pd.read_sql_query("""select daily_fecha, daily_depto,
		daily_total_confirmados, daily_total_activos,
		daily_total_decesos, daily_total_recuperados,
		daily_delta_confirmados, daily_delta_activos,
		daily_delta_decesos, daily_delta_recuperados
		from daily_covid19_BO_depto
		order by daily_depto """, conn)
		conn.close()
		time_series.to_csv("COVID19_BOL_depto.csv", index=False )
		self._logger.info("Se actualizo el reporte COVID19_BOL_depto")

	def TotalNacional(self):
		conn = sqlite3.connect('BD_COVID19_BOL.sqlite')

		time_series = pd.read_sql_query("""select daily_fecha, 
		daily_total_confirmados, daily_total_activos,
		daily_total_decesos, daily_total_recuperados,
		daily_delta_confirmados, daily_delta_activos,
		daily_delta_decesos, daily_delta_recuperados
		from daily_covid19_BO""", conn)
		conn.close()
		time_series.to_csv("COVID19_BOL.csv", index=False )
		self._logger.info("Se actualizo el reporte COVID19_BOL")


class ConsultaMunicipios(object):
	_logger = None
	def __init__(self):
		self._logger = Logger.CreateLogger(__name__)

	def ExtraerIDmapa(self):
		A = requests.get("https://snis.minsalud.gob.bo/")
		page = BeautifulSoup(A.text, 'html.parser')
		iframe = page.find_all("iframe")[0]
		self._logger.info(iframe.attrs["src"])
		
		dia_i  = pd.to_datetime(A.headers["Date"]).strftime("%Y-%m-%d")
		hora_i = pd.to_datetime(A.headers["Date"]).strftime("%H:%M")
		ruta_i = iframe.attrs["src"]

		browser = webdriver.Chrome()
		time.sleep(5)
		browser.get(iframe.attrs["src"])
		time.sleep(2)
		lista_dir = []
		for img in browser.find_elements_by_tag_name("img"):
			lista_dir = lista_dir + re.findall(".*datosagt2020@(.*)/\d,", img.get_attribute("src"))

		mapa_id = np.unique(np.array(lista_dir))[0] # Todos las rutas deberìan referenciar al mismo mapa 
		self._logger.info("Se consultara el mapa: " + mapa_id)
		browser.close()

		nro_mun = requests.get("https://cartocdn-gusc-d.global.ssl.fastly.net/datosagt2020/api/v1/map/datosagt2020@{mapa_id}/dataview/eac18df0-661c-4c8f-a7c6-532c7ed3b5bf".format(mapa_id=mapa_id))
		nro_mun.json()
		self._logger.info("Se tienen {} municipios".format(nro_mun.json()))
		
		return  mapa_id , nro_mun.json() , dia_i , hora_i , ruta_i

	def Municipios(self, mapa_id):
		responses = []
		for i in tqdm(range(400)):
			registro_i = requests.get("https://cartocdn-gusc-c.global.ssl.fastly.net/juliael/api/v1/map/juliael@{mapa_id}/3/attributes/{mun_id}".format(mapa_id = mapa_id, mun_id= i))
			responses.append([i, registro_i.headers["Date"] , registro_i])
			time.sleep(0.5)
			if (i+1) % 40 == 0 :
				self._logger.info("{} \% de Consultas".format(((i+1) / 400)*100))

		VALUES = {}
		for response_i in responses: 
			if response_i[2].ok:
				dict_i = {}
				dict_i["date_get"] =  response_i[1]
				dict_i.update(response_i[2].json())
				VALUES[str(response_i[0])] = dict_i

		CONSULTA_i = pd.DataFrame(VALUES).T.reset_index()
		CONSULTA_i["date_get"] = pd.to_datetime(CONSULTA_i.date_get).dt.strftime("%Y-%m-%d %H:%M")
		CONSULTA_i.fillna(value=0, inplace=True)
		CONSULTA_i["activos"] = CONSULTA_i.positivos - CONSULTA_i.decesos - CONSULTA_i.recuperados

		params = CONSULTA_i.sum()[["decesos","positivos","recuperados","activos"]].to_dict()
		params
		# ¿ A que día corresponde ? 
		conn = sqlite3.connect('BD_COVID19_BOL.sqlite')
		df_t = pd.read_sql_query("""select daily_fecha
		from daily_covid19_BO
		where daily_total_decesos ={decesos}
		and daily_total_confirmados = {positivos}
		and daily_total_recuperados = {recuperados}
		and daily_total_activos = {activos}""".format(**params)
								, conn)
		conn.close()


		if df_t.shape[0]>0: # ¿existen fechas válidas ?
			self._logger.info("Los municipios corresponden a una fecha válida")
			fec_coincidencia = df_t.daily_fecha.values[0]

			CONSULTA_i["fecha"] = fec_coincidencia
			valores = [list(j) for i,j  in CONSULTA_i[['fecha', 'index', 'date_get', 'municipio', 'positivos', 'activos','decesos', 'recuperados']]
			.iterrows()]
			
			# Consultando si existe la variable
			conn = sqlite3.connect('BD_COVID19_BOL.sqlite')
			registros_previos = pd.read_sql_query("""select daily_fecha
			from daily_covid19_BO_mun
			where daily_fecha = '{}'""".format(fec_coincidencia) , conn)
			conn.close()
			
			if registros_previos.shape[0]>0: # ¿Tenemos registrado elementos?
				self._logger.info("Ya se tienen registros de estos municipios para la fecha asiganada")
				pass
			else : 
				conn = sqlite3.connect('BD_COVID19_BOL.sqlite')
				cur = conn.cursor()
				cur.executemany("""INSERT INTO daily_covid19_BO_mun (
				daily_fecha
				,mun_id
				,mun_fec_consulta
				,daily_municipio
				,daily_total_confirmados
				,daily_total_activos
				,daily_total_decesos
				,daily_total_recuperados) VALUES (?,?,?,?,?,?,?,?);""", valores)
				conn.commit()
				conn.close() 
				self._logger.info("Se inserto una nueva fecha")
		
		CONSULTA_i.to_csv("Municipios/{} Municipios.csv".format(CONSULTA_i.date_get.max()),index=False)



class ReporteSNIS(object):
	_logger = None
	def __init__(self):
		self._logger = Logger.CreateLogger(__name__)

	def GetReporte(self):
		now_time = datetime.now()
		pdf_i = requests.get("https://snis.minsalud.gob.bo/images/vigilancia_2018/REPORTE_EPIDEMIOLOGICO.pdf")
		with open("ReportesSNIS/{}.pdf".format(now_time.strftime("%Y-%m-%d_%H_%M")) , "wb") as a: 
			a.write(pdf_i.content)
		self._logger.info("Se guardo el reporte del SNIS")
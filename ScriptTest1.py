
# coding: utf-8

# In[116]:


import requests
import json
import time
import pandas as pd
import matplotlib.pyplot as plt
#get_ipython().run_line_magic('matplotlib', 'inline')


# # 0. Consulta

# In[117]:


registro_i = requests.get("https://www.boliviasegura.gob.bo/wp-content/json/api.php")


# In[118]:


registro_i_json = registro_i.json()
# Elementos en fecha de entrada
# ['fecha', 'departamento', 'contador', 'total', 'porcentaje']


# In[119]:


registro_i_json["fecha"] # retroceder un día


# In[120]:


#with open('20200421.json', 'w') as outfile:
#json.dump(registro_i_json, outfile)


# ## 1. Parseo del imput para cifras acumuladas

# ### Por departamentos

# In[121]:


day_i = {}
for key, value in registro_i_json["departamento"].items()  :
    val_j =  value["contador"]
    val_j["total"]  = value["total"] 
    day_i[key] = val_j

df_day_i = pd.DataFrame(day_i).T.rename_axis(["depto"]).reset_index()
df_day_i["fecha"] = pd.to_datetime(registro_i_json["fecha"]).strftime("%Y%m%d")
df_day_i["depto"] = df_day_i["depto"].str.upper()
df_day_i["activos"] = df_day_i.confirmados - df_day_i.decesos - df_day_i.recuperados
print(df_day_i)
time.sleep(5)


# ### A nivel nacional

# In[122]:


bol_i = registro_i_json["contador"]
bol_i["total"] = registro_i_json["total"]

sr_bol_i = pd.Series(bol_i)
sr_bol_i["fecha"] = pd.to_datetime(registro_i_json["fecha"]).strftime("%Y%m%d")
sr_bol_i["depto"] = "BOL"
sr_bol_i["activos"] = sr_bol_i.confirmados - sr_bol_i.decesos - sr_bol_i.recuperados
print(sr_bol_i)


# # 2. Calculando variaciones

# ### Extrayendo el acumulado del día anterior

# In[123]:


# Extraer de base de datos los elememtos del día anterior para obtener las variaciones


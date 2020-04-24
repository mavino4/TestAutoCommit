select t.daily_id, t.daily_fecha, t.daily_depto
,t.daily_total_confirmados - t_1.daily_total_confirmados as daily_delta_confirmados 
,t.daily_total_activos     - t_1.daily_total_activos     as daily_delta_activos      
,t.daily_total_decesos     - t_1.daily_total_decesos     as daily_delta_decesos    
,t.daily_total_recuperados - t_1.daily_total_recuperados as daily_delta_recuperados 
,t.daily_total_sospechosos - t_1.daily_total_sospechosos as daily_delta_sospechosos  
,t.daily_total_descartados - t_1.daily_total_descartados as daily_delta_descartados  
,t.daily_total_total       - t_1.daily_total_total       as daily_delta_total                      
from daily_covid19_BO t 
inner join daily_covid19_BO t_1
on t.daily_fecha = strftime('%Y-%m-%d',date(t_1.daily_fecha, '+1 day')) 
and t.daily_depto = t_1.daily_depto
where t.daily_fecha = '2020-04-23'



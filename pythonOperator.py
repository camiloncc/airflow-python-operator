#HERE DECLARE LIBS AND OPERATORS
import airflow
from airflow import DAG
from airflow.operators.sensors import TimeDeltaSensor
from airflow.models import Variable
from datetime import datetime, timedelta, date, time
from airflow.contrib.kubernetes.pod import Resources
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
import logging
import os
import sys


#HERE DECLARE DAG DEFINITION
start = datetime.today() - timedelta(days=1)
default_args = {
    'owner': 'Udacity-DAG',
    'start_date': start,
    'email': 'carrascocamilo@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG('00_DAG_UDACITY_PYTHON_OPERATOR', default_args=default_args, schedule_interval="0 0 * * 1-5",tags=['GIT','PYTHON OPERATOR'])

#HERE CREATE FILE 
def create_file(**kwargs):
    ds_dt = datetime.strptime(kwargs['ds'], '%Y-%m-%d')
    if ds_dt.weekday() == 4:
        fecha_ref_dia = (ds_dt + timedelta(days=3))
    else:
        fecha_ref_dia = (ds_dt + timedelta(days=1))
    fecha = fecha_ref_dia.strftime('%Y%m%d')
    textList = [fecha]

    ruta = "/files/tmp/" #define local path  
    filename = 'Udacity_%s' % fecha
    ruta_archivo = ruta + filename
    outF = open(ruta_archivo, "w")
    for line in textList:
        # write line to output file
        outF.write(line)
        outF.write("\n")
    outF.close()

    archivo_return = ruta_archivo
    return archivo_return

#HERE SEND FILE TO FTP
def to_ftp(ftp_conn_id, **kwargs):

    ds_dt = datetime.strptime(kwargs['ds'], '%Y-%m-%d')
    if ds_dt.weekday() == 4:
        fecha_ref_dia = (ds_dt + timedelta(days=3))
    else:
        fecha_ref_dia = (ds_dt + timedelta(days=1)) # Only week days
    fecha = fecha_ref_dia.strftime('%Y%m%d')

    ruta_remota = '/ftp_remote/Udacity_%s' % fecha  #remote ftp path
    ruta_local = '/files/tmp/Udacity_%s' % fecha #local ftp

    ftp_hook = FTPHook(ftp_conn_id=ftp_conn_id)
    ftp_hook.store_file(ruta_remota, ruta_local)

    return True
    
    
 Create_File = PythonOperator(
    task_id='01_A_Genera_Archivo',
    provide_context=True,
    python_callable=create_file,
    dag=dag)

Move_File_FTP = PythonOperator(
    task_id='01_B_FTP_To_Aprimo',
    provide_context=True,
    op_kwargs={
        'ftp_conn_id': 'sftp_conexion_id', #define conection FTP type  
    },
    python_callable=to_ftp,
    dag=dag)
    
    Create_File >> Move_File_FTP
    


import pandas as pd
import glob
from datetime import datetime
import numpy as np
import shutil
import os,sys
from os import remove
from os import path

cruise_name= input(("Input cruise name:"))
cruise_id = input(("Input cruise id (e.g. 29SG20181004):"))
cruise_name= str(cruise_name)
cruise_id = str(cruise_id)

options = ["Sarmiento de Gamboa", "Hespérides", "García del Cid"]
vessel_input = ''
input_message = "Pick an option:\n"
for index, item in enumerate(options):
    input_message += f'{index+1}) {item}\n'
while vessel_input not in map(str, range(1, len(options) + 1)):
    vessel_input = input(input_message)
print('You picked: ' + options[int(vessel_input) - 1])
if vessel_input == "1":
  vessel_mode = "Sarmiento"
  vessel = "Sarmiento de Gamboa"
elif vessel_input == "2":
    vessel = "Hespérides"
    vessel_mode ="Hesperides"
elif vessel_input == "3":
  vessel_mode = "GarciaCid"
  vessel = "García del Cid"

folder= "/newcdi/"

try:
    url= "http://www.utm.csic.es/metadata/"+ vessel_mode + "/generated/"+ cruise_id +"/cdi/"+ cruise_id + "_samples_and_stations_with_pos.csv"
    header_list=['First_lat', 'First_long', 'End_lat', 'End_long', 'First_time', 'End_time','Instrument', 'Coments']
    samples_and_stations = pd.read_csv(url, names = header_list)
    print("Information obtained from the following link:\n")
    print(url)

    samples = pd.DataFrame(samples_and_stations)
    #list of cdi's that have been made in the campaign
    instrument_list = samples["Instrument"].unique().tolist()
    intrument_input = ''
    input_message_inst = "Pick an option:\n"
    for index, item in enumerate(instrument_list):
        input_message_inst += f'{index+1}) {item}\n'  
    while intrument_input not in map(str, range(1, len(instrument_list) + 1)):
        intrument_input = input(input_message_inst)
    print('You picked: ' + instrument_list[int(intrument_input) - 1])
 
except: 
    url= "/newcdi/"+ cruise_id + "_samples_and_stations_with_pos.csv"
    header_list=['First_lat', 'First_long', 'End_lat', 'End_long', 'First_time', 'End_time','Instrument', 'Coments']
    samples_and_stations = pd.read_csv(url, names = header_list)
    samples = pd.DataFrame(samples_and_stations)
    
    #list of cdi's that have been made in the campaign
    instrument_list = samples["Instrument"].unique().tolist()
    intrument_input = ''
    input_message_inst = "Pick an option:\n"
    for index, item in enumerate(instrument_list):
        input_message_inst += f'{index+1}) {item}\n'
    while intrument_input not in map(str, range(1, len(instrument_list) + 1)):
        intrument_input = input(input_message_inst)
    print('You picked: ' + instrument_list[int(intrument_input) - 1])

cdi_model_list = ["CTD", "CTD_ROS", "CTD_ROS_LADCP","CTD_UND","DRE","MOC", "ROV","SVP","TRA","XBT","XSV"]

"""cdi models: the order is the one in the list cdi_model_list
1) CTD
2) CTD_ROS
3) CTD_ROS_LADCP
4) CTD_UND
5) DRE
6) MOC
7) ROV
8) SVP
9) TRA
10) XBT
11) XSV

if any is added ALERT!!! with the number change"""

cdi_input = ''
input_message_cdi = "Pick a model option:\n"
for index, item in enumerate(cdi_model_list):
    input_message_cdi += f'{index+1}) {item}\n'

while cdi_input not in map(str, range(1, len(cdi_model_list) + 1)):
    cdi_input = input(input_message_cdi)

print('You picked: ' + cdi_model_list[int(cdi_input) - 1])
print("Correctly selected CDIs")

dia= cruise_id[10:12]
mes=cruise_id[8:10]
any=cruise_id[4:8]
short_date = any +"-"+ mes +"-"+ dia
fila=0

def delete_columns(csv_name):
  arxiu = pd.read_csv(csv_name)
  arxiu = arxiu.reindex(columns=["longitude", "latitude", "time", "date", "id", "codi", "cruise_id"])
  arxiu = arxiu.to_csv(csv_name,header=True, index=False)
   
def update_columns(samples,fila):
    lista_name=[] 
    for i in range(0,total_lines):
        name=str(samples.loc [i,"index"])
        name = name.zfill(2) #fem que el nom sigui de 2 digits i ho ompli amb 0 a la esquerre
        
        name2= cruise_name + " " + cdi_text + " " + name + " data"
        fila=fila+1
        lista_name.append(name2)
    samples['name'] = lista_name
    lista_codi=[]
    for i in range(0,total_lines):
        id=str(samples.loc [i,"index"])
        id = id.zfill(2) #fem que el id sigui de 2 digits i ho ompli amb 0 a la esquerre
        afegir=cdi_model
        id2= afegir + "_" + id
        id2= id2[1:]
        fila=fila+1
        lista_codi.append(id2)
    samples['codi'] = lista_codi
    lista_cruise_id=[]
    for i in range(0,total_lines):
        id=str(samples.loc [i,"index"])
        id2= cruise_id
        fila=fila+1
        lista_cruise_id.append(id2)
    samples['cruise_id'] = lista_cruise_id
    samples["csr_name"] = cruise_name
    lista_id=[]
    
    for i in range(0,total_lines):
        id=str(samples.loc [i,"index"])
        id = id.zfill(2) #fem que el id sigui de 2 digits i ho ompli amb 0 a la esquerre
        afegir=cdi_model
        id2= cruise_id+afegir + "_" + id
        fila=fila+1
        lista_id.append(id2)
    samples['id'] = lista_id
    lista_fecha=[]
    lista_hora =[]
    lista_hora_1=[]
    lista_min =[]
    lista_time=[]
    
    lista_dia=[]
    lista_mes=[]
    lista_any=[]
    lista_date=[]
    lista_time=[]
    lista_date_final=[]
    
    for i in range(0,total_lines):
        fecha=str(samples.loc [i,"First_time"])
        fecha_corta = fecha.split(" ")[0] 
        hora= fecha.split(" ")[1] 
        fila=fila+1
        hora_1= hora.split(":")[0]
        min= hora.split(":")[1]
        lista_fecha.append(fecha_corta)
        lista_hora.append (hora)
        lista_hora_1.append (hora_1)
        lista_min.append (min)

    samples['fecha'] = lista_fecha
    samples['hora'] = lista_hora
    samples['hora_1'] = lista_hora_1
    samples['min'] = lista_min

    for i in range(0,total_lines):
        fecha_hora=str(samples.loc [i,"hora_1"])
        fecha_min=str(samples.loc [i,"min"])
        time = fecha_hora +":" + fecha_min + ":00"
        lista_time.append(time)    
    samples["time"] = lista_time

    for i in range(0,total_lines):
        fecha=str(samples.loc [i,"fecha"])
        dia = fecha.split("-")[0] 
        mes= fecha.split("-")[1] 
        any= fecha.split("-")[2]   
        fila=fila+1
        lista_dia.append(dia)
        lista_mes.append(mes)
        lista_any.append (any)

    samples['dia'] = lista_dia
    samples['mes'] = lista_mes
    samples['any'] = lista_any

    for i in range(0,total_lines):
        fecha_dia=str(samples.loc [i,"dia"])
        fecha_mes=str(samples.loc [i,"mes"])
        fecha_any=str(samples.loc [i,"any"])
        date = fecha_any + "-" + fecha_mes + "-"+ fecha_dia
        fila=fila+1
        lista_date.append(date)
    samples["date"] = lista_date

    for i in range(0,total_lines):
        date=str(samples.loc [i,"date"])
        time=str(samples.loc [i,"time"])
        fila=fila+1
        fecha_final = date + "T"+ time
        
        lista_date_final.append(fecha_final)
    samples["date_time"] = lista_date_final
 
def final_time_generation (samples, fila):
  lista_fecha_end=[]
  lista_hora_end =[]
  lista_hora_1_end=[]
  lista_min_end =[]
  lista_time_end=[]
  lista_fecha_final_end=[]
  lista_dia_end=[]
  lista_mes_end=[]
  lista_any_end=[]
  lista_date_end=[]
  lista_time_end=[]
  lista_date_final_end=[]
  for i in range(0,total_lines):
    fecha_end=str(samples.loc [i,"End_time"])
    fecha_corta_end = fecha_end.split(" ")[0] 
    hora_end= fecha_end.split(" ")[1] 
    fila=fila+1
    hora_1_end= hora_end.split(":")[0]
    min_end= hora_end.split(":")[1]
    lista_fecha_end.append(fecha_corta_end)
    lista_hora_end.append (hora_end)
    lista_hora_1_end.append (hora_1_end)
    lista_min_end.append (min_end)
    
  samples['fecha_end'] = lista_fecha_end
  samples['hora_end'] = lista_hora_end
  samples['hora_1_end'] = lista_hora_1_end
  samples['min_end'] = lista_min_end

  for i in range(0,total_lines):
    fecha_hora_end=str(samples.loc [i,"hora_1_end"])
    fecha_min_end=str(samples.loc [i,"min_end"])
    time_end = fecha_hora_end +":" + fecha_min_end + ":00"
    lista_time_end.append(time_end)    
  samples["time_end"] = lista_time_end

  for i in range(0,total_lines):
    fecha_end=str(samples.loc [i,"fecha_end"])
    dia_end = fecha_end.split("-")[0] 
    mes_end= fecha_end.split("-")[1] 
    any_end= fecha_end.split("-")[2]   
    fila=fila+1
    lista_dia_end.append(dia_end)
    lista_mes_end.append(mes_end)
    lista_any_end.append (any_end)
    
  samples['dia_end'] = lista_dia_end
  samples['mes_end'] = lista_mes_end
  samples['any_end'] = lista_any_end

  for i in range(0,total_lines):
    fecha_dia_end=str(samples.loc [i,"dia_end"])
    fecha_mes_end=str(samples.loc [i,"mes_end"])
    fecha_any_end=str(samples.loc [i,"any_end"])
    date_end = fecha_any_end + "-" + fecha_mes_end + "-"+ fecha_dia_end
    fila=fila+1
    lista_date_end.append(date_end)
  samples["date_end"] = lista_date_end

  for i in range(0,total_lines):
      date_end=str(samples.loc [i,"date_end"])
      time_end=str(samples.loc [i,"time_end"])
      fila=fila+1
      fecha_final_end = date_end + "T"+ time_end
      
      lista_date_final_end.append(fecha_final_end)
  samples["date_time_end"] = lista_date_final_end
   
if cdi_input == "1": #CTD  
    select_instrument = instrument_list[int(intrument_input) - 1]
    
    header_list=['longitude', 'latitude', 'End_lat', 'End_long', 'First_time', 'End_time','Instrument', 'Coments']
    samples_and_stations = pd.read_csv(url, names = header_list)
    samples = pd.DataFrame(samples_and_stations)
    
    if path.exists("cdi_model_ctd.txt"):
        remove('cdi_model_ctd.txt')

    shutil.copy("cdi_model_ctd.xml", "cdi_model_ctd_1.xml")
    filename = "cdi_model_ctd_1.xml"

    infilename = os.path.join(filename)
    newname = infilename.replace('cdi_model_ctd_1.xml', 'cdi_model_ctd.txt')
    output = os.rename(infilename, newname)
    cdi_model = "_ctd"
    cdi_text = "CTD"
    
    instrument = samples.loc[samples['Instrument'] == select_instrument]
    instrument.to_csv("samples.csv", index=False)

    samples = pd.read_csv("samples.csv")
    samples.index = np.arange(1, len(samples) + 1) # que els index comencin per 1 no per 0
    samples = samples.rename_axis('index').reset_index()
    samples.set_index('index')
    total_lines=len(samples.axes[0])
    
    update_columns(samples,fila)
    
    lista_abstract=[]
    for i in range(0,total_lines):
        name=str(samples.loc [i,"index"])
        name = name.zfill(2) 
        ctd_text = "CTD"  
        name3= "Water column data acquired on board the R/V " + vessel 
        + " with a SeaBird SBE911 plus " + ctd_text + " during the " + cruise_name + " cruise."
        fila=fila+1
        lista_abstract.append(name3)
    samples['abstract'] = lista_abstract
    
    csv_name= '.\\ctd\\' + "samples"+ cdi_model  +  ".csv"
    samples.to_csv(csv_name, header=True, index=False)
    for i in samples.index: 
        folder_copy= ".\\ctd\\"+ samples["id"][i] + ".txt"
        shutil.copy("cdi_model_ctd.txt",folder_copy)
        contenido = open(folder_copy, "r",encoding='UTF-8').read()
        contenido = contenido.replace("new_ID",samples["id"][i])
        contenido = contenido.replace("new_NAME",samples["name"][i])
        contenido = contenido.replace(str("90.00"),str(samples["latitude"][i])) 
        contenido = contenido.replace(str("180.00"),str(samples["longitude"][i]))
        contenido = contenido.replace(str("2022-03-15T13:12:00"),str(samples["date_time"][i])) 
        contenido = contenido.replace(str("2022-02-01"),short_date)
        contenido = contenido.replace("CSR_CRUISE_NAME",samples["csr_name"][i])
        contenido = contenido.replace("SHORT_ID",cruise_id)
        contenido = contenido.replace("new_ABSTRACT",samples["abstract"][i]) 
        
        nombre_archivo = ".\\ctd\\" + samples["id"][i] + ".xml"
        with open(folder_copy, "w",encoding='UTF-8') as archivo:
            archivo.write(contenido)
            archivo.close()
            os.rename(folder_copy, nombre_archivo )

    delete_columns(csv_name)

    print("CDIs saved in CTD folder")

    os.remove ("samples.csv")
    os.remove ("cdi_model_ctd.txt")
#-----------------------------------------------------------------------
if cdi_input == "2": #CTD_ROS
  select_instrument = instrument_list[int(intrument_input) - 1]
  header_list=['longitude', 'latitude', 'End_lat', 'End_long', 'First_time', 'End_time','Instrument', 'Coments']
  samples_and_stations = pd.read_csv(url, names = header_list)
  samples = pd.DataFrame(samples_and_stations)

  if path.exists("cdi_model_ctd_ros.txt"):
    remove('cdi_model_ctd_ros.txt')

  shutil.copy("cdi_model_ctd_ros.xml", "cdi_model_ctd_ros_1.xml")
  filename = "cdi_model_ctd_ros_1.xml"

  infilename = os.path.join(folder,filename)
  newname = infilename.replace('cdi_model_ctd_ros_1.xml', 'cdi_model_ctd_ros.txt')
  output = os.rename(infilename, newname)
  cdi_model = "_ctd_ros"
  cdi_text = "CTD - rosette"
  
  
  instrument = samples.loc[samples['Instrument'] == select_instrument]
  instrument.to_csv("samples.csv", index=False)
  
  samples = pd.read_csv("samples.csv")
  samples.index = np.arange(1, len(samples) + 1) # que els index comencin per 1 no per 0
  samples = samples.rename_axis('index').reset_index()
  samples.set_index('index')
  total_lines=len(samples.axes[0])
  
  update_columns(samples,fila)

  lista_abstract=[]
  for i in range(0,total_lines):
    name=str(samples.loc [i,"index"])
    name = name.zfill(2) 
    ctd_ros_text = "CTD and rosette"
      
    name3= "Water column data acquired on board the R/V " + vessel + " with a SeaBird SBE911 plus " + ctd_ros_text + " during the " + cruise_name + " cruise."
    fila=fila+1
    lista_abstract.append(name3)
  samples['abstract'] = lista_abstract

  csv_name= ".\\ctd_ros\\" + "samples"+ cdi_model + ".csv"
  samples.to_csv(csv_name, header=True, index=False)
  
  for i in samples.index: 
      folder_copy= ".\\ctd_ros\\"+ samples["id"][i] + ".txt"
      shutil.copy("cdi_model_ctd_ros.txt",folder_copy)
      contenido = open(folder_copy, "r",encoding='UTF-8').read()
      contenido = contenido.replace("new_ID",samples["id"][i])
      contenido = contenido.replace("new_NAME",samples["name"][i])
      contenido = contenido.replace(str("90.00"),str(samples["latitude"][i])) 
      contenido = contenido.replace(str("180.00"),str(samples["longitude"][i]))
      contenido = contenido.replace(str("2022-03-15T13:12:00"),str(samples["date_time"][i])) 
      contenido = contenido.replace(str("2022-02-01"),short_date)
      contenido = contenido.replace("CSR_CRUISE_NAME",samples["csr_name"][i])
      contenido = contenido.replace("SHORT_ID",cruise_id)
      contenido = contenido.replace("new_ABSTRACT",samples["abstract"][i]) 
      
      nombre_archivo = ".\\ctd_ros\\" + samples["id"][i] + ".xml"
      with open(folder_copy, "w",encoding='UTF-8') as archivo:
            archivo.write(contenido)
            archivo.close()
            os.rename(folder_copy, nombre_archivo )
  delete_columns(csv_name)
  print("CDIs guardats a la carpeta CTD_ROS")
  os.remove ("samples.csv")
  os.remove ("cdi_model_ctd_ros.txt")
#-----------------------------------------------------------------------
if  cdi_input == "3": #CTD_ROS_LADCP
  select_instrument = instrument_list[int(intrument_input) - 1]
  header_list=['longitude', 'latitude', 'End_lat', 'End_long', 'First_time', 'End_time','Instrument', 'Coments']
  samples_and_stations = pd.read_csv(url, names = header_list)
  samples = pd.DataFrame(samples_and_stations)

  if path.exists("cdi_model_ctd_ros_ladcp.txt"):
    remove('cdi_model_ctd_ros_ladcp.txt')

  shutil.copy("cdi_model_ctd_ros_ladcp.xml", "cdi_model_ctd_ros_ladcp_1.xml")
  filename = "cdi_model_ctd_ros_ladcp_1.xml"

  infilename = os.path.join(folder,filename)
  newname = infilename.replace('cdi_model_ctd_ros_ladcp_1.xml', 'cdi_model_ctd_ros_ladcp.txt')
  output = os.rename(infilename, newname)
  cdi_model = "_ctd_ros_ladcp"
  cdi_text = "CTD-rosette and LADCP"
  
  instrument = samples.loc[samples['Instrument'] == select_instrument]
  instrument.to_csv("samples.csv", index=False)
  
  samples = pd.read_csv("samples.csv")
  samples.index = np.arange(1, len(samples) + 1) # que els index comencin per 1 no per 0
  samples = samples.rename_axis('index').reset_index()
  samples.set_index('index')
  total_lines=len(samples.axes[0])
  
  update_columns(samples,fila)
  
  for i in range(0,total_lines):
    name=str(samples.loc [i,"index"])
    name = name.zfill(2) 
    ctd_ros_ladcp_text = "CTD, rosette and LADCP"
    name3= "Water column data acquired on board the R/V " + vessel + " with a SeaBird SBE911 plus " + ctd_ros_ladcp_text + " during the " + cruise_name + " cruise."
    fila=fila+1
    lista_abstract.append(name3)
  samples['abstract'] = lista_abstract

  csv_name= ".\\ctd_ros_ladcp\\" + "samples"+ cdi_model + ".csv"
  samples.to_csv(csv_name, header=True, index=False)
  
  for i in samples.index: 
      folder_copy= ".\\ctd_ros_ladcp\\"+ samples["id"][i] + ".txt"
      shutil.copy("cdi_model_ctd_ros_ladcp.txt",folder_copy)
      contenido = open(folder_copy, "r",encoding='UTF-8').read()
      contenido = contenido.replace("new_ID",samples["id"][i])
      contenido = contenido.replace("new_NAME",samples["name"][i])
      contenido = contenido.replace(str("90.00"),str(samples["latitude"][i])) 
      contenido = contenido.replace(str("180.00"),str(samples["longitude"][i]))
      contenido = contenido.replace(str("2022-03-15T13:12:00"),str(samples["date_time"][i])) 
      contenido = contenido.replace(str("2022-02-01"),short_date)
      contenido = contenido.replace("CSR_CRUISE_NAME",samples["csr_name"][i])
      contenido = contenido.replace("SHORT_ID",cruise_id)
      contenido = contenido.replace("new_ABSTRACT",samples["abstract"][i]) 
      
      nombre_archivo = ".\\ctd_ros_ladcp\\" + samples["id"][i] + ".xml"
      with open(folder_copy, "w",encoding='UTF-8') as archivo:
            archivo.write(contenido)
            archivo.close()
            os.rename(folder_copy, nombre_archivo )
  delete_columns(csv_name)
  print("CDIs guardats a la carpeta CTD_ROS_LADCP")
  
  os.remove ("samples.csv")
  os.remove ("cdi_model_ctd_ros_ladcp.txt")
#-----------------------------------------------------------------------
if cdi_input == "4": #CTD_UND 
  select_instrument = instrument_list[int(intrument_input) - 1]
  
  header_list=['longitude', 'latitude', 'End_lat', 'End_long', 'First_time', 'End_time','Instrument', 'Coments']
  samples_and_stations = pd.read_csv(url, names = header_list)
  
  samples = pd.DataFrame(samples_and_stations)

  if path.exists("cdi_model_ctd_und.txt"):
    remove('cdi_model_ctd_und.txt')

  shutil.copy("cdi_model_ctd_und.xml", "cdi_model_ctd_und_1.xml")
  filename = "cdi_model_ctd_und_1.xml"

  infilename = os.path.join(folder,filename)
  newname = infilename.replace('/newcdi/cdi_model_ctd_und_1.xml', '/newcdi/cdi_model_ctd_und.txt')
  output = os.rename(infilename, newname)
  cdi_model = "_ctd_und"
  cdi_text= "CTD undulant"
  
  instrument = samples.loc[samples['Instrument'] == select_instrument]
  instrument.to_csv("samples.csv", index=False)
  
  samples = pd.read_csv("samples.csv")
  samples.index = np.arange(1, len(samples) + 1) # que els index comencin per 1 no per 0
  samples = samples.rename_axis('index').reset_index()
  samples.set_index('index')
  total_lines=len(samples.axes[0])

  update_columns(samples,fila)
  
  lista_abstract=[]
  for i in range(0,total_lines):
    name=str(samples.loc [i,"index"])
    name = name.zfill(2) 
    name3= "Water column data acquired on board the R/V " + vessel + " with a SeaSoar towed CTD during the " + cruise_name + " cruise."
    fila=fila+1
    lista_abstract.append(name3)
  samples['abstract'] = lista_abstract

  csv_name= ".\\ctd_und\\" + "samples"+ cdi_model + ".csv"
  samples.to_csv(csv_name, header=True, index=False)
  
  for i in samples.index: 
      folder_copy= ".\\ctd_und\\"+ samples["id"][i] + ".txt"
      shutil.copy("/newcdi/cdi_model_ctd_und.txt",folder_copy)
      contenido = open(folder_copy, "r",encoding='UTF-8').read()
      contenido = contenido.replace("new_ID",samples["id"][i])
      contenido = contenido.replace("new_NAME",samples["name"][i])
      contenido = contenido.replace(str("90.00"),str(samples["latitude"][i])) 
      contenido = contenido.replace(str("180.00"),str(samples["longitude"][i]))
      contenido = contenido.replace(str("2022-03-15T13:12:00"),str(samples["date_time"][i])) 
      contenido = contenido.replace(str("2022-02-01"),short_date)
      contenido = contenido.replace("CSR_CRUISE_NAME",samples["csr_name"][i])
      contenido = contenido.replace("SHORT_ID",cruise_id)
      contenido = contenido.replace("new_ABSTRACT",samples["abstract"][i]) 
      
      nombre_archivo = ".\\ctd_und\\" + samples["id"][i] + ".xml"
      with open(folder_copy, "w",encoding='UTF-8') as archivo:
            archivo.write(contenido)
            archivo.close()
            os.rename(folder_copy, nombre_archivo )

  print("CDIs guardats a la carpeta CTD_UND")
  delete_columns(csv_name)
  os.remove ("samples.csv")
  os.remove ("cdi_model_ctd_und.txt") 
#------------------------------------------
if cdi_input == "5": #DRE
  select_instrument = instrument_list[int(intrument_input) - 1]
  
  header_list=['longitude', 'latitude', 'End_lat', 'End_long', 'First_time', 'End_time','Instrument', 'Coments']
  samples_and_stations = pd.read_csv(url, names = header_list)
  samples = pd.DataFrame(samples_and_stations)
  
  if path.exists("cdi_model_dre.txt"):
    remove('cdi_model_dre.txt')

  
  shutil.copy("cdi_model_dre.xml", "cdi_model_dre_1.xml")
  filename = "cdi_model_dre_1.xml"

  infilename = os.path.join(folder,filename)
  newname = infilename.replace('cdi_model_dre_1.xml', 'cdi_model_dre.txt')
  output = os.rename(infilename, newname)
  cdi_model = "_dre"
  cdi_text="dredge"
  
  instrument = samples.loc[samples['Instrument'] == select_instrument]
  instrument.to_csv("samples.csv", index=False)
  
  samples = pd.read_csv("samples.csv")
  samples.index = np.arange(1, len(samples) + 1) # que els index comencin per 1 no per 0
  samples = samples.rename_axis('index').reset_index()
  samples.set_index('index')
  total_lines=len(samples.axes[0])
  
  update_columns(samples,fila)
  
  lista_abstract=[]
  for i in range(0,total_lines): 
    name=str(samples.loc [i,"index"])
    name = name.zfill(2) #fem que el nom sigui de 2 digits i ho ompli amb 0 a la esquerre
    name3= "Data from samples acquired on board the R/V "+ vessel +" with a sediment dredge during the " + cruise_name +" cruise."
    fila=fila+1
    lista_abstract.append(name3)
  samples['abstract'] = lista_abstract

  csv_name= ".\\dre\\" + "samples"+ cdi_model + ".csv"
  samples.to_csv(csv_name, header=True, index=False)
  #Primer fer un model de CDI amb les organitzacions i la pestanya CRUISE/STATION
  for i in samples.index: 
      folder_copy= ".\\dre\\"+ samples["id"][i] + ".txt"
      shutil.copy("cdi_model_dre.txt",folder_copy)
      contenido = open(folder_copy, "r",encoding='UTF-8').read()
      contenido = contenido.replace("new_ID",samples["id"][i])
      contenido = contenido.replace("new_NAME",samples["name"][i])
      contenido = contenido.replace(str("90.00"),str(samples["latitude"][i])) 
      contenido = contenido.replace(str("180.00"),str(samples["longitude"][i]))
      contenido = contenido.replace(str("2022-03-15T13:12:00"),str(samples["date_time"][i])) 
      contenido = contenido.replace(str("2022-02-01"),short_date)
      contenido = contenido.replace("CSR_CRUISE_NAME",samples["csr_name"][i])
      contenido = contenido.replace("SHORT_ID",cruise_id)
      contenido = contenido.replace("new_ABSTRACT",samples["abstract"][i]) 
      
      nombre_archivo = ".\\dre\\" + samples["id"][i] + ".xml"
      with open(folder_copy, "w",encoding='UTF-8') as archivo:
            archivo.write(contenido)
            archivo.close()
            os.rename(folder_copy, nombre_archivo )

  print("CDIs guardats a la carpeta DRE")
  delete_columns(csv_name)
  os.remove ("samples.csv")
  os.remove ("cdi_model_dre.txt")
#-----------------------------------------------------------------------
if cdi_input == "6": #MOC
  select_instrument = instrument_list[int(intrument_input) - 1]
    
  header_list=['longitude', 'latitude', 'End_lat', 'End_long', 'First_time', 'End_time','Instrument', 'Coments']
  samples_and_stations = pd.read_csv(url, names = header_list)
  samples = pd.DataFrame(samples_and_stations)
  
  if path.exists("cdi_model_moc.txt"):
    remove('cdi_model_moc.txt')
  
  shutil.copy("cdi_model_moc.xml", "cdi_model_moc_1.xml")
  filename = "cdi_model_moc_1.xml"

  infilename = os.path.join(folder,filename)
  newname = infilename.replace('cdi_model_moc_1.xml', 'cdi_model_moc.txt')
  output = os.rename(infilename, newname)
  cdi_model = "_moc"
  cdi_text = "mocness"
  
  instrument = samples.loc[samples['Instrument'] == select_instrument]
  instrument.to_csv("samples.csv", index=False)
  
  samples = pd.read_csv("samples.csv")
  samples.index = np.arange(1, len(samples) + 1) # que els index comencin per 1 no per 0
  samples = samples.rename_axis('index').reset_index()
  samples.set_index('index')
  total_lines=len(samples.axes[0])
  update_columns(samples,fila)
    
  lista_abstract=[]
  for i in range(0,total_lines):
    name=str(samples.loc [i,"index"])
    name = name.zfill(2) #fem que el nom sigui de 2 digits i ho ompli amb 0 a la esquerre
    name3= "Data from biological samples acquired on board the R/V "+ vessel + " with a MOCNESS net during the "+ cruise_name +" cruise."
    fila=fila+1
    lista_abstract.append(name3)
  samples['abstract'] = lista_abstract

  csv_name= ".\\moc\\" + "samples"+ cdi_model + ".csv"
  samples.to_csv(csv_name, header=True, index=False)
  #Primer fer un model de CDI amb les organitzacions i la pestanya CRUISE/STATION
  for i in samples.index: 
      folder_copy= ".\\moc\\"+ samples["id"][i] + ".txt"
      shutil.copy("cdi_model_moc.txt",folder_copy)
      contenido = open(folder_copy, "r",encoding='UTF-8').read()
      contenido = contenido.replace("new_ID",samples["id"][i])
      contenido = contenido.replace("new_NAME",samples["name"][i])
      contenido = contenido.replace(str("90.00"),str(samples["latitude"][i])) 
      contenido = contenido.replace(str("180.00"),str(samples["longitude"][i]))
      contenido = contenido.replace(str("2022-03-15T13:12:00"),str(samples["date_time"][i])) 
      contenido = contenido.replace(str("2022-02-01"),short_date)
      contenido = contenido.replace("CSR_CRUISE_NAME",samples["csr_name"][i])
      contenido = contenido.replace("SHORT_ID",cruise_id)
      contenido = contenido.replace("new_ABSTRACT",samples["abstract"][i]) 
      
      nombre_archivo = ".\\moc\\" + samples["id"][i] + ".xml"
      with open(folder_copy, "w",encoding='UTF-8') as archivo:
            archivo.write(contenido)
            archivo.close()
            os.rename(folder_copy, nombre_archivo )

  print("CDIs guardats a la carpeta MOC")
  delete_columns(csv_name)
  os.remove ("samples.csv")
  os.remove ("cdi_model_moc.txt")  
#------------------------------------------------------------
if cdi_input == "7": #ROV 
  select_instrument = instrument_list[int(intrument_input) - 1] 

  header_list=['longitude', 'latitude', 'End_lat', 'End_long', 'First_time', 'End_time','Instrument', 'Coments']
  samples_and_stations = pd.read_csv(url, names = header_list)
  samples = pd.DataFrame(samples_and_stations)

  if path.exists("cdi_model_rov.txt"):
    remove('cdi_model_rov.txt')

  shutil.copy("cdi_model_rov.xml", "/newcdi/cdi_model_rov_1.xml")
  filename = "cdi_model_rov_1.xml"

  infilename = os.path.join(folder,filename)
  newname = infilename.replace('/newcdi/cdi_model_rov_1.xml', '/newcdi/cdi_model_rov.txt')
  output = os.rename(infilename, newname)
  cdi_model = "_rov"
  cdi_text ="ROV"
  
  rov_model = input(("Introdueix el model del ROV (EX. ROV Liropus 2000 ) + enter:"))
  rov_model=str(rov_model)
  instrument = samples.loc[samples['Instrument'] == select_instrument]
  instrument.to_csv("samples.csv", index=False)
  
  samples = pd.read_csv("samples.csv")
  samples.index = np.arange(1, len(samples) + 1) # que els index comencin per 1 no per 0
  samples = samples.rename_axis('index').reset_index()
  samples.set_index('index')
  total_lines=len(samples.axes[0])
  
  update_columns(samples,fila)
  final_time_generation (samples, fila)
  
  lista_abstract=[] 
  for i in range(0,total_lines):
    name=str(samples.loc [i,"index"])
    name = name.zfill(2) 
    rov_text = "ROV"  
    name3= "ROV dive data from " + rov_model + " acquired on board the R/V " + vessel + " during the " + cruise_name + " cruise."
    fila=fila+1
    lista_abstract.append(name3)
  samples['abstract'] = lista_abstract
  
  csv_name= ".\\rov\\" + "samples"+ cdi_model  +  ".csv"
  samples.to_csv(csv_name, header=True, index=False)
  for i in samples.index: 
      folder_copy= ".\\rov\\"+ samples["id"][i] + ".txt"
      shutil.copy("cdi_model_rov.txt",folder_copy)
      contenido = open(folder_copy, "r",encoding='UTF-8').read()
      contenido = contenido.replace("new_ID",samples["id"][i])
      contenido = contenido.replace("new_NAME",samples["name"][i])
      contenido = contenido.replace(str("90.00"),str(samples["latitude"][i])) 
      contenido = contenido.replace(str("180.00"),str(samples["longitude"][i]))
      contenido = contenido.replace(str("2022-03-15T13:12:00"),str(samples["date_time"][i])) 
      contenido = contenido.replace(str("2022-03-16T13:12:00"),str(samples["date_time_end"][i])) 
      contenido = contenido.replace(str("2022-02-01"),short_date)
      contenido = contenido.replace("CSR_CRUISE_NAME",samples["csr_name"][i])
      contenido = contenido.replace("SHORT_ID",cruise_id)
      contenido = contenido.replace("new_ABSTRACT",samples["abstract"][i]) 
      
      nombre_archivo = ".\\rov\\" + samples["id"][i] + ".xml"
      with open(folder_copy, "w",encoding='UTF-8') as archivo:
            archivo.write(contenido)
            archivo.close()
            os.rename(folder_copy, nombre_archivo )

  delete_columns(csv_name)

  print("CDIs guardats a la carpeta ROV")
  os.remove ("samples.csv")
  os.remove ("cdi_model_rov.txt") 
#-----------------------------------------------------------------------
if cdi_input == "8": #SVP
  select_instrument = instrument_list[int(intrument_input) - 1]

  header_list=['longitude', 'latitude', 'End_lat', 'End_long', 'First_time', 'End_time','Instrument', 'Coments']
  samples_and_stations = pd.read_csv(url, names = header_list)
  samples = pd.DataFrame(samples_and_stations)
  
  if path.exists("cdi_model_svp.txt"):
    remove('cdi_model_svp.txt')
  
  shutil.copy("cdi_model_svp.xml", "cdi_model_svp_1.xml")
  filename = "cdi_model_svp_1.xml"

  infilename = os.path.join(folder,filename)
  newname = infilename.replace('cdi_model_svp_1.xml', 'cdi_model_svp.txt')
  output = os.rename(infilename, newname)
  cdi_model = "_svp"
  cdi_text="SVP"
  
  instrument = samples.loc[samples['Instrument'] == select_instrument]
  instrument.to_csv("samples.csv", index=False)
  
  samples = pd.read_csv("samples.csv")
  samples.index = np.arange(1, len(samples) + 1) # que els index comencin per 1 no per 0
  samples = samples.rename_axis('index').reset_index()
  samples.set_index('index')
  total_lines=len(samples.axes[0])
  update_columns(samples,fila)
    
  lista_abstract=[]
  for i in range(0,total_lines):
    name=str(samples.loc [i,"index"])
    name = name.zfill(2) #fem que el nom sigui de 2 digits i ho ompli amb 0 a la esquerre
    name3= "Water column data launched on board the R/V "+ vessel +" during the " + cruise_name +" cruise."
    fila=fila+1
    lista_abstract.append(name3)
  samples['abstract'] = lista_abstract

  csv_name= ".\\svp\\" + "samples"+ cdi_model + ".csv"
  samples.to_csv(csv_name, header=True, index=False)
  #Primer fer un model de CDI amb les organitzacions i la pestanya CRUISE/STATION
  for i in samples.index: 
      folder_copy= ".\\svp\\"+ samples["id"][i] + ".txt"
      shutil.copy("cdi_model_svp.txt",folder_copy)
      contenido = open(folder_copy, "r",encoding='UTF-8').read()
      contenido = contenido.replace("new_ID",samples["id"][i])
      contenido = contenido.replace("new_NAME",samples["name"][i])
      contenido = contenido.replace(str("90.00"),str(samples["latitude"][i])) 
      contenido = contenido.replace(str("180.00"),str(samples["longitude"][i]))
      contenido = contenido.replace(str("2022-03-15T13:12:00"),str(samples["date_time"][i])) 
      contenido = contenido.replace(str("2022-02-01"),short_date)
      contenido = contenido.replace("CSR_CRUISE_NAME",samples["csr_name"][i])
      contenido = contenido.replace("SHORT_ID",cruise_id)
      contenido = contenido.replace("new_ABSTRACT",samples["abstract"][i]) 
      
      nombre_archivo = ".\\svp\\" + samples["id"][i] + ".xml"
      with open(folder_copy, "w",encoding='UTF-8') as archivo:
            archivo.write(contenido)
            archivo.close()
            os.rename(folder_copy, nombre_archivo )

  print("CDIs guardats a la carpeta SVP")
  delete_columns(csv_name)
  os.remove ("samples.csv")
  os.remove ("cdi_model_svp.txt")
#-----------------------------------------------------------------------
if cdi_input == "9": #TRA
  select_instrument = instrument_list[int(intrument_input) - 1]

  header_list=['longitude', 'latitude', 'End_lat', 'End_long', 'First_time', 'End_time','Instrument', 'Coments']
  samples_and_stations = pd.read_csv(url, names = header_list)
  samples = pd.DataFrame(samples_and_stations)
  
  if path.exists("cdi_model_tra.txt"):
    remove('cdi_model_tra.txt')
  
  shutil.copy("cdi_model_tra.xml", "cdi_model_tra_1.xml")
  filename = "cdi_model_tra_1.xml"

  infilename = os.path.join(folder,filename)
  newname = infilename.replace('cdi_model_tra_1.xml', 'cdi_model_tra.txt')
  output = os.rename(infilename, newname)
  cdi_model = "_tra"
  cdi_text = "sediment trap"
  
  instrument = samples.loc[samples['Instrument'] == select_instrument]
  instrument.to_csv("samples.csv", index=False)
  
  samples = pd.read_csv("/newcdi/samples.csv")
  samples.index = np.arange(1, len(samples) + 1) # que els index comencin per 1 no per 0
  samples = samples.rename_axis('index').reset_index()
  samples.set_index('index')
  total_lines=len(samples.axes[0])
  
  update_columns(samples,fila)
  
  lista_abstract=[]
  for i in range(0,total_lines): 
    name=str(samples.loc [i,"index"])
    name = name.zfill(2) #fem que el nom sigui de 2 digits i ho ompli amb 0 a la esquerre
    name3= "Data from sediment traps deployed and acquired on board the R/V "+ vessel +" during the " + cruise_name +" cruise."
    fila=fila+1
    lista_abstract.append(name3)
  samples['abstract'] = lista_abstract

  csv_name= ".\\tra\\" + "samples"+ cdi_model + ".csv"
  samples.to_csv(csv_name, header=True, index=False)
  #Primer fer un model de CDI amb les organitzacions i la pestanya CRUISE/STATION
  for i in samples.index: 
      folder_copy= ".\\tra\\"+ samples["id"][i] + ".txt"
      shutil.copy("cdi_model_tra.txt",folder_copy)
      contenido = open(folder_copy, "r",encoding='UTF-8').read()
      contenido = contenido.replace("new_ID",samples["id"][i])
      contenido = contenido.replace("new_NAME",samples["name"][i])
      contenido = contenido.replace(str("90.00"),str(samples["latitude"][i])) 
      contenido = contenido.replace(str("180.00"),str(samples["longitude"][i]))
      contenido = contenido.replace(str("2022-03-15T13:12:00"),str(samples["date_time"][i])) 
      contenido = contenido.replace(str("2022-02-01"),short_date)
      contenido = contenido.replace("CSR_CRUISE_NAME",samples["csr_name"][i])
      contenido = contenido.replace("SHORT_ID",cruise_id)
      contenido = contenido.replace("new_ABSTRACT",samples["abstract"][i]) 
      
      nombre_archivo = ".\\tra\\" + samples["id"][i] + ".xml"
      with open(folder_copy, "w",encoding='UTF-8') as archivo:
            archivo.write(contenido)
            archivo.close()
            os.rename(folder_copy, nombre_archivo )
  delete_columns(csv_name)
  
  print("CDIs guardats a la carpeta TRA")
  os.remove ("samples.csv")
  os.remove ("cdi_model_tra.txt")
#-----------------------------------------------------------------------
if cdi_input == "10": #XBT
  select_instrument = instrument_list[int(intrument_input) - 1]
  
  header_list=['longitude', 'latitude', 'End_lat', 'End_long', 'First_time', 'End_time','Instrument', 'Coments']
  samples_and_stations = pd.read_csv(url, names = header_list)
  samples = pd.DataFrame(samples_and_stations)
  
  if path.exists("cdi_model_xbt.txt"):
    remove('cdi_model_xbt.txt')
  
  shutil.copy("cdi_model_xbt.xml", "cdi_model_xbt_1.xml")
  filename = "cdi_model_xbt_1.xml"

  infilename = os.path.join(folder,filename)
  newname = infilename.replace('cdi_model_xbt_1.xml', 'cdi_model_xbt.txt')
  output = os.rename(infilename, newname)
  cdi_model = "_xbt"
  cdi_text = "XBT"
  
  instrument = samples.loc[samples['Instrument'] == select_instrument]
  instrument.to_csv("samples.csv", index=False)
  
  samples = pd.read_csv("samples.csv")
  samples.index = np.arange(1, len(samples) + 1) # que els index comencin per 1 no per 0
  samples = samples.rename_axis('index').reset_index()
  samples.set_index('index')
  total_lines=len(samples.axes[0])

  update_columns(samples,fila)
  
  lista_abstract=[]
  for i in range(0,total_lines):
    name=str(samples.loc [i,"index"])
    name = name.zfill(2) #fem que el nom sigui de 2 digits i ho ompli amb 0 a la esquerre
    name3= "Water column data launched on board the R/V "+ vessel +" during the " + cruise_name +" cruise."
    fila=fila+1
    lista_abstract.append(name3)
  samples['abstract'] = lista_abstract

  csv_name= ".\\xbt\\" + "samples"+ cdi_model + ".csv"
  samples.to_csv(csv_name, header=True, index=False)
  #Primer fer un model de CDI amb les organitzacions i la pestanya CRUISE/STATION
  for i in samples.index: 
      folder_copy= ".\\xbt\\"+ samples["id"][i] + ".txt"
      shutil.copy("cdi_model_xbt.txt",folder_copy)
      contenido = open(folder_copy, "r",encoding='UTF-8').read()
      contenido = contenido.replace("new_ID",samples["id"][i])
      contenido = contenido.replace("new_NAME",samples["name"][i])
      contenido = contenido.replace(str("90.00"),str(samples["latitude"][i])) 
      contenido = contenido.replace(str("180.00"),str(samples["longitude"][i]))
      contenido = contenido.replace(str("2022-03-15T13:12:00"),str(samples["date_time"][i])) 
      contenido = contenido.replace(str("2022-02-01"),short_date)
      contenido = contenido.replace("CSR_CRUISE_NAME",samples["csr_name"][i])
      contenido = contenido.replace("SHORT_ID",cruise_id)
      contenido = contenido.replace("new_ABSTRACT",samples["abstract"][i]) 
      
      nombre_archivo = ".\\xbt\\" + samples["id"][i] + ".xml"
      with open(folder_copy, "w",encoding='UTF-8') as archivo:
            archivo.write(contenido)
            archivo.close()
            os.rename(folder_copy, nombre_archivo )

  print("CDIs guardats a la carpeta XBT")
  delete_columns(csv_name)
  os.remove ("samples.csv")
  os.remove ("cdi_model_xbt.txt")
#-----------------------------------------------------------------------
if cdi_input == "11": #XSV
  select_instrument = instrument_list[int(intrument_input) - 1]
  
  header_list=['longitude', 'latitude', 'End_lat', 'End_long', 'First_time', 'End_time','Instrument', 'Coments']
  samples_and_stations = pd.read_csv(url, names = header_list)
  samples = pd.DataFrame(samples_and_stations)
  
  if path.exists("cdi_model_xsv.txt"):
    remove('cdi_model_xsv.txt')
    
  shutil.copy("cdi_model_xsv.xml", "cdi_model_xsv_1.xml")
  filename = "cdi_model_xsv_1.xml"

  infilename = os.path.join(folder,filename)
  newname = infilename.replace('cdi_model_xsv_1.xml', 'cdi_model_xsv.txt')
  output = os.rename(infilename, newname)
  cdi_model = "_xsv"
  cdi_text="XSV"
  
  instrument = samples.loc[samples['Instrument'] == select_instrument]
  instrument.to_csv("samples.csv", index=False)
    
  samples = pd.read_csv("samples.csv")
  samples.index = np.arange(1, len(samples) + 1) # que els index comencin per 1 no per 0
  samples = samples.rename_axis('index').reset_index()
  samples.set_index('index')
  total_lines=len(samples.axes[0])
  update_columns(samples,fila)
    
  lista_abstract=[]
  for i in range(0,total_lines):
    name=str(samples.loc [i,"index"])
    name = name.zfill(2) #fem que el nom sigui de 2 digits i ho ompli amb 0 a la esquerre
    name3= "Water column data launched on board the R/V "+ vessel +" during the " + cruise_name +" cruise."
    fila=fila+1
    lista_abstract.append(name3)
  samples['abstract'] = lista_abstract

  csv_name= ".\\xsv\\" + "samples"+ cdi_model + ".csv"
  samples.to_csv(csv_name, header=True, index=False)
  #Primer fer un model de CDI amb les organitzacions i la pestanya CRUISE/STATION
  for i in samples.index: 
      folder_copy= ".\\xsv\\"+ samples["id"][i] + ".txt"
      shutil.copy("cdi_model_xsv.txt",folder_copy)
      contenido = open(folder_copy, "r",encoding='UTF-8').read()
      contenido = contenido.replace("new_ID",samples["id"][i])
      contenido = contenido.replace("new_NAME",samples["name"][i])
      contenido = contenido.replace(str("90.00"),str(samples["latitude"][i])) 
      contenido = contenido.replace(str("180.00"),str(samples["longitude"][i]))
      contenido = contenido.replace(str("2022-03-15T13:12:00"),str(samples["date_time"][i])) 
      contenido = contenido.replace(str("2022-02-01"),short_date)
      contenido = contenido.replace("CSR_CRUISE_NAME",samples["csr_name"][i])
      contenido = contenido.replace("SHORT_ID",cruise_id)
      contenido = contenido.replace("new_ABSTRACT",samples["abstract"][i]) 
      
      nombre_archivo = ".\\xsv\\" + samples["id"][i] + ".xml"
      with open(folder_copy, "w",encoding='UTF-8') as archivo:
            archivo.write(contenido)
            archivo.close()
            os.rename(folder_copy, nombre_archivo )

  print("CDIs guardats a la carpeta XSV")
  delete_columns(csv_name)
  os.remove ("samples.csv")
  os.remove ("cdi_model_xsv.txt")
#-----------------------------------------------------------------------

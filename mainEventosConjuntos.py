#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 17 10:44:24 2023

@author: carlossalcidoa
"""
import pandas as pd
import glob
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import time
import re

#-----------------------------------------------------------------------------------------------------------------------------------
#PARTE 0: AQUI DECLARAMOS LOS DATAFRAMES QUE VAMOS A NECESITAR, TAMBIÉN LEEMOS LOS CSV QUE VAMOS A USAR
dfEventos = pd.DataFrame(columns=["Evento", "Clave", "Boletos_Cortesias", "Boletos_Leones", "Asistencia_Total", "Asistencia_Regular",
                                  "Asistencia_Cortesias", "Asistencia_Leones", "Uso_Cortesias","Total_CS", "Total_Boletos"])
dfEC = pd.read_csv('Eventos-Categorías.csv')
dfClientes = pd.DataFrame(columns=["nombre", "correo", "evento", "cant_total_boletos","cant_cortesias","cant_rewards",
                                   "uso_cortesias", "uso_rewards","gasto_c.s.", "gasto_boletos", "gasto_rewards", 
                                   "codigo_pais", "codigo_area","num_telefono"])

#OBTENER TODOS LOS ARCHIVOS CSV DENTRO DE LA CARPETA especificada
carpeta = '/Users/carlossalcidoa/Documents/CUCEI-INCO/ServicioS/EventosConjuntos/drive-download-20231017T165452Z-001'
archivos_csv = glob.glob(carpeta + '/*.csv')

#LEEMOS EL CSV DE SALDOS, COMO EL ARCHIVO TIENE ERRORES, TENEMOS QUE HACER TODO ESTO
columnas_a_mantener = ['Nombre', 'Nombres adicionales', 'Apellido', 'Apellidos adicionales','Correo electrónico',
                       'Código de país', 'Código de área', 'Número telefónico', 'Fecha de registro',
                       'ZWAP MONEY', 'ZWAP POINTS']#Estas son las columnas con las que nos quedamos, las otras están vacías
file_in = pd.read_csv('saldos.csv', usecols=columnas_a_mantener)
file_in.to_csv("saldos_correccion.csv", index=0)
dfSaldo = pd.read_csv('saldos_correccion.csv')



#-----------------------------------------------------------------------------------------------------------------------------------
#PARTE 1: TOMAMOS LOS DATAFRAMES, LES CAMBIAMOS EL NOMBRE Y AGREGAMOS EL GENERO, GUARDAMOS LOS CSV CON ESTOS CAMBIOS
dataframes = []
# Leer cada archivo CSV y agregarlo a la lista de DataFrames
for archivo in archivos_csv:
    df = pd.read_csv(archivo, encoding='iso-8859-1')
    dataframes.append(df)

#ACCEDER A LOS DATAFRAMES
for df in dataframes:
    #print(df.head())
    for index, row in dfEC.iterrows():
        nombre = row['EVENTO']
        if nombre in df['Evento'].values:
            categoria = row['Categoría ']
            df.loc[df['Evento'] == nombre, 'Genero'] = categoria#AQUÍ CAMBIAMOS UNA COLUMNA QUE ESTABA MAL EN LOS CSV ORIGINALES

#GUARDAR LOS DF COMO ARCHIVOS CSV
for i, df in enumerate(dataframes):
    nombre_evento = df['Evento'].iloc[0]
    nombre_archivo = f'{nombre_evento}.csv'#NOMBRAR AL ARCHIVO SEGÚN EL EVENTO
    df.to_csv(nombre_archivo, index=False)



#-----------------------------------------------------------------------------------------------------------------------------------
#PARTE 2: AQUÍ OCURRE LO MÁS IMPORTANTE, ITERAMOS SOBRE LOS DATAFRAMES PARA FORMAR LAS TRES TABLAS: COMPRAS, EVENTOS, CLIENTES
for df in dataframes:
    
    #-----------------------------------------------------------------------------------------------------------------------------------
    #PARTE 2.1: TABLA COMPRAS, EXTRAER LOS VALORES DE LAS COLUMNAS PARA CREAR LA CLAVE
    referencia = df.loc[0, "Referencia"]
    usuario = df.loc[0, "Usuario"]
    fecha_venta = df.loc[0, "Fecha de Venta"]
    evento = df.loc[0, "Evento"]
    funcion = df.loc[0, "Función"]
    clave = str(referencia)+str(usuario)+str(fecha_venta)+str(evento)+str(funcion)
    
    
    
    #-----------------------------------------------------------------------------------------------------------------------------------
    #PARTE 2.2: TABLA EVENTOS, COMENZAMOS A HCER LOS CALCULOS
    #CALCULAR EL PORCENTAJE DE CORTESÍAS
    total_filas = len(df)
    cortesias = len(df[df['Método de Pago'].str.contains(r'\bCortesia\b', case=False, na=False)])#aquí nos basabamos en la columna 'tipo de precio', pero la cambié porque esta es mas concistente
    porcentaje_cortesias = (cortesias / total_filas) * 100
    
    #CALCULAR EL PORCENTAJE DE REW
    # Verificar y reemplazar valores NaN en la columna 'Método de Pago'
    df['Método de Pago'].fillna('', inplace=True)
    reg_count = len(df[df['Método de Pago'].str.contains('rew', case=False)])
    porcentaje_reg = (reg_count / total_filas) * 100
    
    #CALCULAR EL PORCENTAJE DE LECTURAS SI, es decir, asistencias totales
    df['Cantidad de Lecturas'] = df['Cantidad de Lecturas'].replace({'SI': 1, 'NO': 0}).astype(int)
    lecturas_si = len(df[df['Cantidad de Lecturas'] >= 1])
    lecturas = (lecturas_si / total_filas) * 100
    
    #CALCULAR EL PORCENTAJE DE ASISTENCIA CORTESIA.
    df['Cantidad de Lecturas'] = df['Cantidad de Lecturas'].replace({'SI': 1, 'NO': 0}).astype(int)
    
    lecturas_cortesia = len(df[(df['Método de Pago'].str.contains(r'Cortesia', case=False, na=False)) & (df['Cantidad de Lecturas'] >= 1)])
    if(lecturas_si>0):
        lecturas_c = (lecturas_cortesia / total_filas) * 100 #lecturas_si) * 100
    else:
        lecturas_c=0
    
    #CALCULAR EL PORCENTAJE DE Asistencia de REW
    df['Método de Pago'].fillna('', inplace=True)# Verificar y reemplazar valores NaN en la columna 'Método de Pago'
    lecturas_l = len(df[(df['Método de Pago'].str.contains('rew', case=False)) & (df['Cantidad de Lecturas'] >= 1)])
    if(reg_count>0):
        porcentaje_lecturas_l = (lecturas_l / total_filas) * 100# reg_count) * 100
    else:
        porcentaje_lecturas_l=0
        
    #USO DE CORTESIAS
    if(cortesias==0):
        uso_cortesias = 0
    else:
        uso_cortesias = (lecturas_cortesia / cortesias) * 100
    
    #CALCULAR GANACIAS CS
    df['C.S.'] = df['C.S.'].str.replace('$', '').str.replace(',', '').astype(float)
    total_cs = df['C.S.'].sum()

    #CALCULAR GANANCIAS BOLETOS
    df['Precio S.C.'] = df['Precio S.C.'].str.replace('$', '').str.replace(',', '').astype(float)
    total_boletos = df['Precio S.C.'].sum()

    
    #AGREGAR AL DF DE EVENTOS
    nueva_fila={"Evento": evento, 
                "Clave":clave, 
                "Boletos_Cortesias": porcentaje_cortesias, 
                "Boletos_Leones": porcentaje_reg, 
                "Asistencia_Total":lecturas,
                "Asistencia_Regular":lecturas - (lecturas_c + porcentaje_lecturas_l),
                "Asistencia_Cortesias":lecturas_c, 
                "Asistencia_Leones": porcentaje_lecturas_l,
                "Uso_Cortesias":uso_cortesias,
                "Total_CS":total_cs,
                "Total_Boletos":total_boletos
                }
    dfEventos = dfEventos.append(nueva_fila, ignore_index=True)
    
    
    
    #-----------------------------------------------------------------------------------------------------------------------------------
    #PARTE 2.3: TABLA CLIENTES, NOS BASAMOS EN LOS CLIENTES CON CORREO, LOS DEMÁS NO IMPORTAN
    df['Correo'].fillna('', inplace=True)
    correos_validos = df[df['Correo'].str.contains('@')]['Correo'].unique()    
    
    for correo in correos_validos:#SEGUIMOS EN EL MISMO FOR, Y APROVECHAMOS ESO PARA AGREGAR LOS CLIENTES DEL DF ACTUAL
        nombre = df[df['Correo'] == correo]['Nombre de Cliente'].values[0]  # Tomar el primer valor 'nombre' correspondiente al correo
        evento = df[df['Correo'] == correo]['Evento'].values[0]  # Obtener el Evento correspondiente, aunque toda la columna es igual.
        cant_boletos = df[df['Correo'] == correo].shape[0]#Obtener cuantas veces se repite el correo, esto nos da la cantida de boletos
        cant_cortesias = (df[df['Correo'] == correo]['Método de Pago'].str.contains('Cortesia', case=False)).sum()#cuántas veces se repite la cadena 'cortesia' en metodo de pago
        cant_rewards = (df[df['Correo'] == correo]['Tipo De Precio'].str.count(re.compile(r'REG', re.I))).sum()#cuántas veces está la palabra 'reg'
        #Sumar el gasto en CS
        gasto_cs = df[df['Correo'] == correo]['C.S.']#tomamos los cs que corresponen al correo
        gasto_cs = gasto_cs.astype(str)#convertir en string
        gasto_cs = gasto_cs.str.replace('$', '')#les quitamos el $
        gasto_cs = gasto_cs.astype(float)#los hacemos float
        suma_cs = gasto_cs.sum()#los sumamos
        #Sumar el gasto en boletos (SC), NO rewards
        gasto_boletos = df[(df['Correo'] == correo) & (~df['Tipo De Precio'].str.contains('REG', case=False))]['Precio S.C.']#tomamos los boletos que corresponen al correo y no son reg
        gasto_boletos = gasto_boletos.astype(str)#convertir en string
        gasto_boletos = gasto_boletos.str.replace('$', '')#les quitamos el $
        gasto_boletos = gasto_boletos.astype(float)#los hacemos float
        suma_boletos = gasto_boletos.sum()#los sumamos
        #Sumar el gasto en boletos (sí REWARDS)
        gasto_rewards = df[(df['Correo'] == correo) & (df['Tipo De Precio'].str.contains('REG', case=False))]['Precio S.C.']#tomamos los boletos que corresponden al correo y si son reg
        gasto_rewards = gasto_rewards.astype(str)#convertir en string
        gasto_rewards = gasto_rewards.str.replace('$', '')#les quitamos el $
        gasto_rewards = gasto_rewards.astype(float)#los hacemos float
        suma_rewards = gasto_rewards.sum()#los sumamos
        
        #agregar el porcentaje de uso de las cortesias, si no hay cortesias, mandar null
        if(cant_cortesias>=1):
            uso_cortesias = df[(df['Correo'] == correo) & (df['Método de Pago'].str.contains('Cortesia', case=False)) & ((df['Cantidad de Lecturas'] == 'SI') | (df['Cantidad de Lecturas'].astype(str).str.isnumeric() & (df['Cantidad de Lecturas'].astype(int) >= 1)))].shape[0]
        else:
            uso_cortesias=None
        #agregar el porcentaje de uso de rewards, si no hay rewards, mandar null
        if(cant_rewards>=1):
            uso_rewards = df[(df['Correo'] == correo) & (df['Tipo De Precio'].str.contains('REG', case=False))].shape[0]
        else:
            uso_rewards=None
        
        
        codigo_pais=''
        codigo_area=''
        numero_telefono=''
        correo_in_saldo = correo in dfSaldo['Correo electrónico'].values
        if correo_in_saldo:
            try:
                #UNA VEZ QUE ENCONTRAMOS EL CORREO DENTRO DEL CSV SALDO, BUSCAMOS EL NUMERO CORRESPONDIENTE A ESE CORREO
                codigo_pais = dfSaldo.loc[dfSaldo['Correo electrónico'] == correo, 'Código de país'].values[0]
                codigo_area = dfSaldo.loc[dfSaldo['Correo electrónico'] == correo, 'Código de área'].values[0]
                numero_telefono = dfSaldo.loc[dfSaldo['Correo electrónico'] == correo, 'Número telefónico'].values[0]              
                if codigo_pais == '':
                    codigo_pais = None
                else:
                    codigo_pais = int(codigo_pais)
                
                if codigo_area == '':
                    codigo_area = None
                else:
                    codigo_area = int(codigo_area)
                
                if numero_telefono == '':
                    numero_telefono = None
                else:
                    numero_telefono = int(numero_telefono)

                    
            except ValueError:
                print(f"Error: Los datos para el correo {correo} están incompletos en dfSaldo.")
        else:
            codigo_pais = None
            codigo_area = None
            numero_telefono = None
        #VAMOS A AGREGAR ESTA FILA al dataframe
        nueva_fila={"nombre": nombre,
                    "correo": correo,
                    "evento": evento,
                    "cant_total_boletos":cant_boletos,
                    "cant_cortesias":cant_cortesias,
                    "cant_rewards":cant_rewards,
                    "uso_cortesias":uso_cortesias,
                    "uso_rewards":uso_rewards,
                    "gasto_c.s.":suma_cs,
                    "gasto_boletos":suma_boletos,
                    "gasto_rewards":suma_rewards,
                    "codigo_pais":codigo_pais,
                    "codigo_area":codigo_area,
                    "num_telefono":numero_telefono}
        dfClientes = dfClientes.append(nueva_fila, ignore_index=True)#AGREGAR AL DATAFRAME
        
    


#-----------------------------------------------------------------------------------------------------------------------------------
#PARTE 2.4: ÚLTIMOS AJUSTES  
dfEventos.to_csv("TablaEventos.csv", index=False)
dfClientes.to_csv("TablaClientes.csv", index=False)

#AGREGAR LA CLAVE A LOS DF DE LA TABLA COMPRAS
for df in dataframes:
    for index, row in dfEventos.iterrows():
        clave = row['Clave']
        nombre = row['Evento']
        if nombre in df['Evento'].values:
            # Asignar la misma clave a todas las filas del DataFrame
            df['Clave'] = clave


dfCompras = pd.concat(dataframes, ignore_index=True)
#print(dfCompras)
dfCompras.to_csv("TablaCompras.csv", index=False)




#-----------------------------------------------------------------------------------------------------------------------------------
#PARTE 3: PARA TERMINAR, CONECTAMOS A PHPMYADMIN Y SUBIMOS LOS DATAFRAMES
try:
    engine = create_engine("mysql+mysqlconnector://root:@localhost:3306/EventosConjuntos")
    print("Conexión exitosa a la base de datos")
    
    #OBTENER LISTA DE CLAVES YA EXISTENTES EN LA BASE DE DATOS
    existing_event_claves = engine.execute("SELECT clave FROM eventos").fetchall()
    existing_event_claves = [clave[0] for clave in existing_event_claves]#Aqui se guardan las llaves primarias que ya estan en la base de datos
    # Obtener correos existentes en la tabla 'Clientes'
    existing_client_emails = engine.execute("SELECT correo FROM clientes").fetchall()
    existing_client_emails = [correo[0] for correo in existing_client_emails]
    
    # AGREGARA EVENTOS
    new_events = dfEventos[~dfEventos['Clave'].isin(existing_event_claves)]
    if not new_events.empty:
        new_events.to_sql(name='eventos', con=engine, if_exists='append', index=False)

    # AGREGAR COMPRAS
    chunk_size = 1000#el chunk size es para hacer inserciones de mil en mil, así no truena el programa, pues trabajamos con 50 mil
    for i in range(0, len(dfCompras), chunk_size):
        dfCompras_chunk = dfCompras[i:i+chunk_size]
        new_compras = dfCompras_chunk[~dfCompras_chunk['Clave'].isin(existing_event_claves)]#Aqui se hace la misma verificación para no agregarlos si ya está la clave
        if not new_compras.empty:
            new_compras.to_sql(name='compras', con=engine, if_exists='append', index=False)
 
    #AGREGAR CLIENTES
    chunk_size = 1000
    for i in range(0, len(dfClientes), chunk_size):
        dfClientes_chunk = dfClientes[i:i + chunk_size]
        new_clients = dfClientes_chunk[~dfClientes_chunk['correo'].isin(existing_client_emails)]
        # Verificar si hay nuevos registros
        if not new_clients.empty:
            new_clients.to_sql(name='clientes', con=engine, if_exists='append', index=False)
    
    print("Proceso de inserción completo.") 

except Exception as e:
    print("Error al conectar a la base de datos:", e)


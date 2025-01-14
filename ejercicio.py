import redis
import os
import time

import os

def clear_console():
    os.system('cls' if os.name == 'nt' else 'clear')

clear_console()


def li():
    print()
    print('=======================================')
    print()
li()


conexionRedis = redis.ConnectionPool(host='localhost', port=6379, db=2,decode_responses=True)
bd = redis.Redis(connection_pool=conexionRedis)
bd.flushall()

def anadir(clave, valor):
    if (bd.get(clave)):
        old = bd.get(clave)
        if (bd.set(clave, valor)):
            print(f'UPDATED: [{old} => {valor}]')
        else:
            print(f'ERROR UPDATING VALUE TO {valor}')
    else:
        if (bd.set(clave, valor)):
            print(f'OK: [{clave} => {valor}]')
        else:
            print(f'ERROR UPLOADING VALUE: [{clave} => {valor}]')
            
            
def mostrar_registro(clave):
    valor = bd.get(clave)
    if (valor):
        print(f'[{clave} => {valor}]')
    else:
        print('ERROR KEY NOT FOUND, TRY EXECUTING EXERCISE 1 BEFORE TRY AGAIN')    
    
def eliminar(clave):
    valor = bd.get(clave)
    if valor:
        bd.delete(clave)
        print(f'DELETED: [{clave} => {valor}]')
    else:
        print(f'ERROR DELETING ON [{clave}], KEY NOT FOUND')
        
def mostrar_por_patron(patron='', mostrar=True):
    if patron:
        if mostrar:
            print(f'Values for [{patron}]:')
    else:
        if mostrar:
            print(f'Showing all values:')    
        
    claves = []
    for clave in bd.scan_iter(match=f'{patron}*'):
        try:
            if mostrar:
                print(f'[{clave}] => [{bd.get(clave)}]')
            claves.append(clave)
        except: 
            try:
                if mostrar:
                    print(f'[{clave}] => @{bd.type(clave)}@')
                    # claves.append(clave)
            except:
                if mostrar:
                    print(f'ERROR GETTING VALUE FOR [{clave}]')
                    #hora actual
                    timestamp = time.time()
                    hora_hhmmss = time.strftime('%H:%M:%S', time.localtime(timestamp))
                    print(hora_hhmmss) 
                    
    return claves


def aumentar_valor_en(patron, cantidad):
    registros = mostrar_por_patron(patron, False)
    hay_numericos = False
    if registros:
        for registro in registros:
            if bd.get(registro).isdigit():
                hay_numericos = True
                break
        if hay_numericos:
            for registro in registros:
                if bd.get(registro).isdigit():
                    bd.incr(registro, cantidad)
                    print(f'UPDATED: [{registro} => {bd.get(registro)}]')
        else:
            print(f'No numeric values found for "{patron}" pattern')
    else:
        print(f'Nothing to update for [{patron}] pattern')


def eliminar_mayor_que(patron, mayor):
    registros = mostrar_por_patron(patron, False)
    hay_numericos = False
    if registros:
        for registro in registros:
            if bd.get(registro).isdigit():
                hay_numericos = True
                break
        if hay_numericos:
            for registro in registros:
                try:
                    valor = bd.get(registro)
                    if bd.get(registro).isdigit() and float(bd.get(registro)) > mayor:
                        bd.delete(registro)
                        print(f'DELETED: [{registro} => {valor}]')
                except:
                    print(f'ERROR DELETING ON [{registro}] => {valor}')
        else:
            print(f'No numeric values found for "{patron}" pattern')
    else:
        print('Nothing to delete')
        
def insertar_json(json):
    try:
        bd.json().set("artista_mas_escuchados", "$", json)
        print('OK: JSON inserted')
    except Exception as e:
        print(f"ERROR PROCESSING JSON: {e}")
        
        
def filtrar_json(titulo_json, clave, valor):
    try:
        resultado = bd.json().get(titulo_json, f'$[?(@.{clave} == {valor})]')
        return resultado
    except Exception as e:
        print(f"ERROR PROCESING FILTER: {e}")
        return []


def crear_lista_en_redis(nombre_lista, elementos):
    try:
        if bd.delete(nombre_lista):
            print(f'"{nombre_lista}" found and deleted')
        
        for elemento in elementos:
            bd.rpush(nombre_lista, elemento)
        
        print(f'"{nombre_lista}" created with {len(elementos)} elements')
    except Exception as e:
        print(f"ERROR CREATING LIST: {e}")
        
        
def filtrar_lista(nombre_lista, filtro):
    try:
        elementos = bd.lrange(nombre_lista, 0, -1)
        for elemento in elementos:
            if filtro in elemento:
                print(f'[{elemento}]')
    except Exception as e:
        print(f"ERROR FILTERING LIST: {e}")
        
        
def insertar_time_series(nombre_serie, timestamp=None, valor=0):
        try:
            timestamp = timestamp or int(time.time() * 1000)
            
            bd.execute_command('TS.ADD', nombre_serie, timestamp, valor)
            print(f'OK [{nombre_serie}]: [{timestamp}] -> [{valor}]')
        except Exception as e:
            print(f'ERROR SETTING TIME SERIES: {e}')
            
            
def obtener_time_series(nombre_serie, desde=None, hasta=None):
    try:
        desde = desde or '-'
        hasta = hasta or '+'
        
        datos = bd.execute_command('TS.RANGE', nombre_serie, desde, hasta)
        return datos
    except Exception as e:
        print(f"RERROR GETTING TIME SERIES: {e}")
        return []
    
    
def insertar_bitfield(nombre_clave, offset, bits, valor):
    try:
        resultado = bd.execute_command('BITFIELD', nombre_clave, 'SET', f'u{bits}', offset, valor)
        print(f'Inserted in [{nombre_clave}]: Offset [{offset}], Bits [{bits}], Valor [{valor}]')
        return resultado
    except Exception as e:
        print(f'ERROR SETTING BITFIELD: {e}')

def obtener_bitfield(nombre_clave, offset, bits):
    try:
        resultado = bd.execute_command('BITFIELD', nombre_clave, 'GET', f'u{bits}', offset)
        return resultado[0]
    except Exception as e:
        print(f'ERROR GETTING BITFIELDS: {e}')
        return None

###############################################################
###############################################################

    
#Crear registros clave-valor(0.5 puntos)
def ej1():
    anadir('artista_1', 'Drake')
    anadir('artista_2', 'Ariana Grande')
    anadir('artista_3', 'Jason Derulo')
    anadir('artista_4', 'Ariana Grande')
    anadir('duracion_1', '02:20')
    anadir('n_visitas_web_hoy', 2000)
    anadir('n_registros_web_hoy', 1000)
    li()

#Obtener y mostrar el número de claves registradas (0.5 puntos)
def ej2():
    print('Registered keys length:', len(bd.keys()))
    li()
    
#Obtener y mostrar un registro en base a una clave (0.5 puntos)
def ej3():
    mostrar_registro('artista_4')
    li()

#Actualizar el valor de una clave y mostrar el nuevo valor(0.5 puntos)
def ej4():
    anadir('artista_4', 'Le fiche')
    li()
    
#Eliminar una clave-valor y mostrar la clave y el valor eliminado(0.5 puntos)
def ej5():
    eliminar('artista_4')
    li()
    
#Obtener y mostrar todas las claves guardadas (0.5 puntos)
def ej6():
    if len(bd.keys()) > 0:
        print(bd.keys())
    else:
        print('NO KEYS REGISTERED')
    li()
    
#Obtener y mostrar todos los valores guardados(0.5 puntos)
def ej7():
    mostrar_por_patron()
    li()
    
#Obtener y mostrar varios registros con una clave con un patrón en común usando * (0.5 puntos)
def ej8():
    mostrar_por_patron('artista')
    li()
    
#Obtener y mostrar varios registros con una clave con un patrón en común usando [] (0.5 puntos)
def ej9():
    mostrar_por_patron('*n_[0-9]*')
    li()
    
#Obtener y mostrar varios registros con una clave con un patrón en común usando ? (0.5 puntos)
def ej10():
    mostrar_por_patron('artista_?')
    li()
    
#11 - Obtener y mostrar varios registros y filtrarlos por un valor en concreto. (0.5 puntos)
def ej11():
    valor_a_filtrar = 'Ariana Grande'
    claves = mostrar_por_patron('', False)
    for clave in claves:
        tipo = bd.type(clave)
        if tipo == 'string':
            valor = bd.get(clave)
            if valor == valor_a_filtrar:
                print(f'[{clave} => {valor}]')
    li()

#12 - Actualizar una serie de registros en base a un filtro (por ejemplo aumentar su valor en 1 )(0.5 puntos)
def ej12():
    aumentar_valor_en('n_', 10)
    li()
    
#13 - Eliminar una serie de registros en base a un filtro (0.5 puntos)
def ej13():
    print('(numeric fiter)\n')
    eliminar_mayor_que('n_', 1500)
    li()
    
#14 - Crear una estructura en JSON de array de los datos que vayais a almacenar(0.5 puntos)
def ej14():
    artistas_json = [
    {'nombre': 'Eduardo', 'apellido': 'Garcia', 'edad': 32},
    {'nombre': 'ElAlfa', 'apellido': 'ElJefe', 'edad': 200},
    {'nombre': 'Aitor', 'apellido': 'Tilla', 'edad': 24},
    {'nombre': 'Paco', 'apellido': 'Jones', 'edad': 41},
    {'nombre': 'Larry', 'apellido': 'Capija', 'edad': 26}
]
    insertar_json(artistas_json)
    li()
    
#15 - Realizar un filtro por cada atributo de la estructura JSON anterior (0.5 puntos)
def ej15():
    titulo_json = 'artista_mas_escuchados'
    clave = 'edad'
    valor = 24
    
    filtrado = filtrar_json(titulo_json, clave, valor)
    if filtrado:
        print(f'Filtered by age 24: {filtrado}')
    else:
        print(f'No results for {clave} = {valor}')
    li()
    
#16 - Crear una lista en Redis (0.5 puntos)
def ej16():
    artistas = [
        'Eduardo Garcia',
        'ElAlfa ElJefe',
        'Aitor Tilla',
        'Paco Jones',
        'Larry Capija'
    ]
    
    crear_lista_en_redis('artistas', artistas)
    li()
    
#17 - Obtener elementos de una lista con un filtro en concreto(0.5 puntos)
def ej17():
    filtrar_lista('artistas', 'Edu')
    li()

#18 - En Redis hay otras formas de almacenar datos: Set, Hashes, SortedSet,Streams, Geopatial,
#Bitmaps, Bitfields,Probabilistic y Time Series. Elige dos de estos tipos, y crea una función
#que los guarde en la base de datos y otra que los obtenga. (1.5 puntos)
def ej18():
    insertar_time_series('time_series', valor=10)
    tss = obtener_time_series('time_series')
    if tss:
        print(f'Time Series: {tss}')
    else:
        print('No time series found')
        
        
    insertar_bitfield('bitfield', 0, 8, 255)
    insertar_bitfield('bitfield', 1, 8, 255)
    print(f'Bitfield: {obtener_bitfield("bitfield", 0, 8)}')
    li()
    
ej1()
ej2()
ej3()
ej4()
ej5()
ej6()
ej7()
ej8()
ej9()
ej10()
ej11()
ej12()
ej13()
ej14()
ej15()
ej16()
ej17()
ej18()


funciones = {
    1: ej1, 2: ej2, 3: ej3, 4: ej4, 5: ej5,
    6: ej6, 7: ej7, 8: ej8, 9: ej9, 10: ej10,
    11: ej11, 12: ej12, 13: ej13, 14: ej14,
    15: ej15, 16: ej16, 17: ej17, 18: ej18,
}

def mostrar_menu():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("\n--- Menu ---")
    for i in range(1, 19):
        print(f"{i}. Execute ej{i}")
    print("19. Exit")
    
def pulsa_para_continuar():
    input("Press Enter to continue...")
    os.system('cls' if os.name == 'nt' else 'clear')

pulsa_para_continuar()
while True:
        mostrar_menu()
        try:
            opcion = int(input("Select an option: "))
            print(opcion)
            if opcion == 19:
                print("diabolico")
                break
            elif opcion in funciones:
                os.system('cls' if os.name == 'nt' else 'clear')
                print(opcion)
                funciones[opcion]()
            else:
                print("Not a valid option")
        except ValueError:
            print("Insert a valid number")
            
        pulsa_para_continuar()

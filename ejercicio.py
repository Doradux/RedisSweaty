import redis
import os

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
        if mostrar:
            print(f'[{clave}] => [{bd.get(clave)}]')
        claves.append(clave)
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


###############################################################
###############################################################

    
#Crear registros clave-valor(0.5 puntos)
def ej1():
    print('e1\n')
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
    print('e2\n')
    print('Registered keys length:', len(bd.keys()))
    li()
    
#Obtener y mostrar un registro en base a una clave (0.5 puntos)
def ej3():
    print('e3\n')
    mostrar_registro('artista_4')
    li()

#Actualizar el valor de una clave y mostrar el nuevo valor(0.5 puntos)
def ej4():
    print('e4\n')
    anadir('artista_4', 'Le fiche')
    li()
    
#Eliminar una clave-valor y mostrar la clave y el valor eliminado(0.5 puntos)
def ej5():
    print('e5\n')
    eliminar('artista_4')
    li()
    
#Obtener y mostrar todas las claves guardadas (0.5 puntos)
def ej6():
    print('e6\n')
    if len(bd.keys()) > 0:
        print(bd.keys())
    else:
        print('NO KEYS REGISTERED')
    li()
    
#Obtener y mostrar todos los valores guardados(0.5 puntos)
def ej7():
    print('e7\n')
    mostrar_por_patron()
    li()
    
#Obtener y mostrar varios registros con una clave con un patrón en común usando * (0.5 puntos)
def ej8():
    print('e8\n')
    mostrar_por_patron('artista')
    li()
    
#Obtener y mostrar varios registros con una clave con un patrón en común usando [] (0.5 puntos)
def ej9():
    print('e9\n')
    mostrar_por_patron('*n_[0-9]*')
    li()
    
#Obtener y mostrar varios registros con una clave con un patrón en común usando ? (0.5 puntos)
def ej10():
    print('e10\n')
    mostrar_por_patron('artista_?')
    li()
    
#11 - Obtener y mostrar varios registros y filtrarlos por un valor en concreto. (0.5 puntos)
def ej11():
    print('e11\n')
    valor_a_filtrar = 'Ariana Grande'
    claves = mostrar_por_patron('', False)
    for clave in claves:
        valor = bd.get(clave)
        if valor == valor_a_filtrar:
            print(f'[{clave} => {valor}]')
    li()

#12 - Actualizar una serie de registros en base a un filtro (por ejemplo aumentar su valor en 1 )(0.5 puntos)
def ej12():
    print('e12\n')
    aumentar_valor_en('n_', 10)
    li()
    
#13 - Eliminar una serie de registros en base a un filtro (0.5 puntos)
def ej13():
    print('e13 (numeric fiter)\n')
    eliminar_mayor_que('n_', 1500)
    li()
    
#14 - Crear una estructura en JSON de array de los datos que vayais a almacenar(0.5 puntos)
def ej14():
    print('e14\n')
    bd.json().set("artista_mas_escuchados", "$", {"nombre": "Eduardo", "apellido": "Garcia", "edad": 32})
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

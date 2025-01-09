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
    if (bd.get('artista_4')):
        old = bd.get('artista_4')
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
        
def mostrar_por_patron(patron=''):
    if patron:
        print(f'Values for [{patron}]:')
    else:
        print(f'Showing all values:')    
        
    for clave in bd.scan_iter(f'{patron}*'):
        print(bd.get(clave))


###############################################################
###############################################################

    
#Crear registros clave-valor(0.5 puntos)
def ej1():
    anadir('artista_1', 'Drake')
    anadir('artista_2', 'Ariana Grande')
    anadir('artista_3', 'Jason Derulo')
    anadir('artista_4', 'Ariana Grande')
    anadir('duracion_1', '02:20')
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

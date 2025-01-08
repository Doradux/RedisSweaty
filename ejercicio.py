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

def anadir(clave, valor):
    if (bd.get('artista_4')):
        old = bd.get('artista_4')
        if (bd.set(clave, valor)):
            print(f'OK [{old} => {valor}]')
    else:
        if (bd.set(clave, valor)):
            print(f'OK [{clave} => {valor}]')
    
    

def ej1():
    anadir('artista_1', 'Drake')
    anadir('artista_2', 'Ariana Grande')
    anadir('artista_3', 'Jason Derulo')
    anadir('artista_4', 'Ariana Grande')
    anadir('duracion_1', '02:20')
    li()
    
def ej2():
    print(bd.keys())
    print('Numero de claves registradas:', len(bd.keys()))
    li()
    
def ej3(value):
    if (bd.get(value)):
        print(value, '=>', bd.get(value))
    else:
        print('Ejecute el ejercicio 1 o 4 antes de continuar.')
        
    li()
    
def ej4():
    anadir('artista_4', 'Le fiche')
    li()
    
def ej5():
    print(bd.delete('artista_4'))
    li()
    
ej1()
ej2()
ej3('artista_4')
ej4()
ej5()

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


conexionRedis = redis.ConnectionPool(host='localhost', port=6379, db=0)
baseDatosRedis = redis.Redis(connection_pool=conexionRedis)

conexionRedis = redis.ConnectionPool(host='localhost', port=6379, db=0,decode_responses=True)

baseDatosRedis.flushdb()

baseDatosRedis.set('libro_1', 'Quijote')
baseDatosRedis.set('libro_2', 'Hamlet') #hex=10

print(baseDatosRedis.get("libro_1"))
print(baseDatosRedis.get("libro_1").decode("utf-8"))
print(baseDatosRedis.get("libro_2"))
li()

baseDatosRedis.set("libro_1","El señor de los anillos")

#baseDatosRedis.delete("libro_1")
#baseDatosRedis.delete("libro_2")

claves = baseDatosRedis.keys()
print(claves)
li()

for clave in claves:
	print('Clave:', clave , ' y Valor: ', baseDatosRedis.get(clave))
li()

baseDatosRedis.set("libro_1","Quijote")
baseDatosRedis.set("libro_2","Hamlet")
baseDatosRedis.set("libro_3","Otelo")
baseDatosRedis.set("comic_1","Mortadelo y Filemón")
baseDatosRedis.set("comic_2","Superman")

print("Los Libros:")
for clave in baseDatosRedis.scan_iter('libro*'):
   print(clave)
   
print("Los Comics:")   
for clave in baseDatosRedis.scan_iter('comic*'):
   print(clave)
li()

res1 = baseDatosRedis.json().set("usuarios:1", "$", {"nombre": "Jorge", "apellido": "Baron", "edad": 37})
res2 = baseDatosRedis.json().set("usuarios:2", "$", {"nombre": "Lucía", "apellido": "Benitez", "edad": 24})

baseDatosRedis.json().set("usuarios_array", "$", [])
res3 = baseDatosRedis.json().arrappend("usuarios_array", "$", {"nombre": "Pepe", "apellido": "Sanchez", "edad": 45})
res4 = baseDatosRedis.json().arrappend("usuarios_array", "$", {"nombre": "Calisto", "apellido": "Melibea", "edad": 67})

res1 = baseDatosRedis.json().get("usuarios_array", '$[?(@.edad > 50)]')
res2 = baseDatosRedis.json().get("usuarios_array", '$[?(@.nombre == "Pepe")]')
print(res1)
print(res2)
li()

baseDatosRedis.lpush("usuarios:hobbies", "futbol:1") 
baseDatosRedis.lpush("usuarios:hobbies", "tenis:2")
baseDatosRedis.lpush("usuarios:hobbies", "rugby:2")

print(baseDatosRedis.lrange("usuarios:hobbies",0,-1))
li()

#print(baseDatosRedis.rpop("usuarios:hobbies"))
#li()
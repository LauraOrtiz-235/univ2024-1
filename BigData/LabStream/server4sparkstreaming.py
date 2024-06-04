import socket
import threading
import schedule
import time

# Definir el archivo a enviar
#archivo = "archivo.txt"

archivo = "data.csv"

import socket
import time

# Configuración del servidor y puerto
host = 'localhost'
port = 9999


# Función para enviar una línea de texto por el socket
def send_line(line, client_socket):
    client_socket.send((line + '\n').encode('utf-8'))  # Se añade el salto de línea 

# Abrir el archivo y esperar a que un cliente se conecte
with open(archivo, 'r') as file:
    # Crear el socket del servidor
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f'Esperando conexión del cliente en {host}:{port}...')
    client_socket, client_address = server_socket.accept()
    print(f'Cliente conectado desde {client_address}')
    
    # Enviar cada línea de texto
    for line in file:
        # Eliminar el salto de línea y espacios en blanco al final
        line = line.strip()+"\n"
        # Enviar la línea por el socket
        send_line(line, client_socket)
        # Esperar antes de enviar la siguiente línea
        print(f'se envia el stream \n', line)
        time.sleep(5)
        
    # Cerrar el socket del servidor
    server_socket.close()









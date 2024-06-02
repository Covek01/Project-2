
import socket
import time
import threading
from datetime import datetime
import functools as f


FILE_LIST = [
    './database/arsenal.csv',
    './database/aston_villa.csv',
    './database/bournemouth.csv',
    './database/brentford.csv',
    './database/brighton.csv',
    './database/burnley.csv',
    './database/chelsea.csv',
    './database/crystal_palace.csv',
    './database/everton.csv',
    './database/fulham.csv',
    './database/liverpool.csv',
    './database/luton.csv',
    './database/manchester_city.csv',
    './database/manchester_united.csv',
    './database/newcastle_united.csv',
    './database/nottingham_forest.csv',
    './database/sheffield_united.csv',
    './database/tottenham_hotspur.csv',
    './database/west_ham_united.csv',
    './database/wolverhampton_wanderers.csv'
]

def handle_client(client_socket, path):
    try:
        arsenal = open(path, "r")


        dataArsenal = arsenal.readline()

        while True:
            dataArsenal = arsenal.readline()
            client_socket.send(dataArsenal.encode())

            if (dataArsenal == ''):
                break
            
            time.sleep(5)
    except:
        print('ERROR WHILE SENDING ON SOCKET')
    finally:
        arsenal.close()

        client_socket.close()



def start_server(host: str, port: int, file: str):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()
    print(f"Server listening on {host}:{port}...")

    while True:
        client_socket, address = server_socket.accept()
        print(f"Client connected from {address[0]}:{address[1]}")
        client_thread = threading.Thread(target=handle_client, args=(client_socket, file))
        client_thread.start()


if __name__ == "__main__":    
    host = 'localhost' 
    port = 10000  
    threads = list()
    for file in FILE_LIST:
        thread = threading.Thread(target=start_server, args=(host, port, file))
        threads.append(thread)
        port = port + 1

    for t in threads:
        t.start()
    for t in threads:
        t.join()
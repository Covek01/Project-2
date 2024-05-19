import sys
import socket
import time
import threading
import requests
from datetime import datetime
from airplane import Airplane
import json
import functools as f

def generate_airplanes(num_airplanes):
    return [Airplane(f"AB{123+i}") for i in range(num_airplanes)]

def handle_client(client_socket, airplanes):
    url = "https://opensky-network.org/api/states/all"
    params = {
        "lamax": 44.75,
        "lomin": 20.47637,
        "lamin": 35.51353,
        "lomax": 21.83642
    }
    try:
        while True:
            values_list = list()
            for airplane in airplanes:
                airplane.update()
            utc_time = datetime.now().isoformat()
            # response = requests.get(url, params=params)
            # if (response.status_code == 200):
            #     data = response.json()
            #     date = data.date
            #     utc_time = datetime.fromtimestamp(date)


                #0 - Unique 24b address
                #1 - callsign
                #2 - country
                #5 - longitude
                #6 - latitude
                #7 - barometric altitude
                #8 - on ground
                #9 - velocity
                #13 - geo altitude
                # results = map(lambda state:
                #         [utc_time, state[0], state[1], state[2], state[5], state[6], state[7], state[8], state[9], state[13]], data.states)
            results = list(map(lambda state:
                    [utc_time, *[value for value in state.__dict__.values()][:-1]], airplanes))
            for result in results:
                resultsString = f.reduce(lambda i, j: str(i) + ", " + str(j), result)
                # print(resultsString)
                sent = client_socket.send((resultsString + '\n').encode('utf-8'))
                if (sent == False):
                    print("BAD SENDING DATA")

            time.sleep(5)
            
    
            # client_socket.send(resultsString.encode())
            # if (response.status_code == 429):
            #     break
    finally:
        client_socket.close()
    # with open(data_file_path, mode="r", encoding='utf-8-sig') as data_file:
    #     lines = data_file.readlines()
    #     try:
    #         i = 0
    #         while i< len(lines):
    #             print("sending")
    #             client_socket.send(lines[i].encode())
    #             client_socket.send(lines[i+1].encode())
    #             client_socket.send(lines[i+2].encode())
    #             client_socket.send(lines[i+3].encode())
    #             i = i+4
    #             time.sleep(5)
    #     except:
    #         print("error")
    #     finally:
    #         print("closing")
    #         client_socket.close()

def start_server(host: str, port: int, num_of_planes: int):
    airplanes = generate_airplanes(num_of_planes)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()
    print(f"Server listening on {host}:{port}...")

    while True:
        client_socket, address = server_socket.accept()
        print(f"Client connected from {address[0]}:{address[1]}")
        client_thread = threading.Thread(target=handle_client, args=(client_socket, airplanes))
        client_thread.start()


if __name__ == "__main__":
    # arguments = sys.argv

    # if (len(arguments) < 3):
    #     print("LESS THAN 2 ARGUMENTS")
    
    host = 'localhost' 
    port = 9999  
    start_server(host, port, 5)
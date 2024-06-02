
import socket
import time
import threading
from datetime import datetime
import functools as f



def handle_client(client_socket):
    try:
        arsenal = open("./database/arsenal.csv", "r")
        aston_villa = open("./database/aston_villa.csv", "r")
        bournemouth = open("./database/bournemouth.csv", "r")
        brentford = open("./database/brentford.csv", "r")
        brighton = open("./database/brighton.csv", "r")
        burnley = open("./database/burnley.csv", "r")
        chelsea = open("./database/chelsea.csv", "r")
        crystal_palace = open("./database/crystal_palace.csv", "r")
        everton = open("./database/everton.csv", "r")
        fulham = open("./database/fulham.csv", "r")
        liverpool = open("./database/liverpool.csv", "r")
        luton = open("./database/luton.csv", "r")
        manchester_city = open("./database/manchester_city.csv", "r")
        manchester_united = open("./database/manchester_united.csv", "r")
        newcastle_united = open("./database/newcastle_united.csv", "r")
        nottingham_forest = open("./database/nottingham_forest.csv", "r")
        sheffield_united = open("./database/sheffield_united.csv", "r")
        tottenham_hotspur = open("./database/tottenham_hotspur.csv", "r")
        west_ham_united = open("./database/west_ham_united.csv", "r")
        wolverhampton_wanderers = open("./database/wolverhampton_wanderers.csv", "r")


        dataArsenal = arsenal.readline()
        dataAstonVilla = aston_villa.readline()
        dataBournemouth = bournemouth.readline()
        dataBrentford = brentford.readline()
        dataBrighton = brighton.readline()
        dataBurnley = burnley.readline()
        dataChelsea = chelsea.readline()
        dataCrystalPalace = crystal_palace.readline()
        dataEverton = everton.readline()
        dataFulham = fulham.readline()
        dataLiverpool = liverpool.readline()
        dataLuton = luton.readline()
        dataManchesterCity = manchester_city.readline()
        dataManchesterUnited = manchester_united.readline()
        dataNewcastleUnited = newcastle_united.readline()
        dataNottinghamForest = nottingham_forest.readline()
        dataSheffieldUnited = sheffield_united.readline()
        dataTottenhamHotspur = tottenham_hotspur.readline()
        dataWestHamUnited = west_ham_united.readline()
        dataWolverhamptonWanderers = wolverhampton_wanderers.readline()


        while True:
                    # Read the first line from each file
            dataArsenal = arsenal.readline()
            dataAstonVilla = aston_villa.readline()
            dataBournemouth = bournemouth.readline()
            dataBrentford = brentford.readline()
            dataBrighton = brighton.readline()
            dataBurnley = burnley.readline()
            dataChelsea = chelsea.readline()
            dataCrystalPalace = crystal_palace.readline()
            dataEverton = everton.readline()
            dataFulham = fulham.readline()
            dataLiverpool = liverpool.readline()
            dataLuton = luton.readline()
            dataManchesterCity = manchester_city.readline()
            dataManchesterUnited = manchester_united.readline()
            dataNewcastleUnited = newcastle_united.readline()
            dataNottinghamForest = nottingham_forest.readline()
            dataSheffieldUnited = sheffield_united.readline()
            dataTottenhamHotspur = tottenham_hotspur.readline()
            dataWestHamUnited = west_ham_united.readline()
            dataWolverhamptonWanderers = wolverhampton_wanderers.readline()

            print(dataArsenal.encode())
            client_socket.send(dataArsenal.encode())
            client_socket.send(dataAstonVilla.encode())
            client_socket.send(dataBournemouth.encode())
            client_socket.send(dataBrentford.encode())
            client_socket.send(dataBrighton.encode())
            client_socket.send(dataBurnley.encode())
            client_socket.send(dataChelsea.encode())
            client_socket.send(dataCrystalPalace.encode())
            client_socket.send(dataEverton.encode())
            client_socket.send(dataFulham.encode())
            client_socket.send(dataLiverpool.encode())
            client_socket.send(dataLuton.encode())
            client_socket.send(dataManchesterCity.encode())
            client_socket.send(dataManchesterUnited.encode())
            client_socket.send(dataNewcastleUnited.encode())
            client_socket.send(dataNottinghamForest.encode())
            client_socket.send(dataSheffieldUnited.encode())
            client_socket.send(dataTottenhamHotspur.encode())
            client_socket.send(dataWestHamUnited.encode())
            client_socket.send(dataWolverhamptonWanderers.encode())

            if (dataArsenal == '' or dataAstonVilla == '' or dataBournemouth == '' or dataBrentford == '' or 
            dataBrighton == '' or dataBurnley == '' or dataChelsea == '' or dataCrystalPalace == '' or 
            dataEverton == '' or dataFulham == '' or dataLiverpool == '' or dataLuton == '' or 
            dataManchesterCity == '' or dataManchesterUnited == '' or dataNewcastleUnited == '' or 
            dataNottinghamForest == '' or dataSheffieldUnited == '' or dataTottenhamHotspur == '' or 
            dataWestHamUnited == '' or dataWolverhamptonWanderers == ''):
                break
            
            time.sleep(5)

    finally:
        arsenal.close()
        aston_villa.close()
        bournemouth.close()
        brentford.close()
        brighton.close()
        burnley.close()
        chelsea.close()
        crystal_palace.close()
        everton.close()
        fulham.close()
        liverpool.close()
        luton.close()
        manchester_city.close()
        manchester_united.close()
        newcastle_united.close()
        nottingham_forest.close()
        sheffield_united.close()
        tottenham_hotspur.close()
        west_ham_united.close()
        wolverhampton_wanderers.close()

        client_socket.close()



def start_server(host: str, port: int):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()
    print(f"Server listening on {host}:{port}...")

    while True:
        client_socket, address = server_socket.accept()
        print(f"Client connected from {address[0]}:{address[1]}")
        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.start()


if __name__ == "__main__":
    # arguments = sys.argv

    # if (len(arguments) < 3):
    #     print("LESS THAN 2 ARGUMENTS")
    
    host = 'localhost' 
    port = 9999  
    start_server(host, port)
import socket
import pickle
import select
import threading
import random
import time
import os
from tqdm import tqdm

UDPIP = "52.152.172.24"
UDPPort = 20001
bufferSize = 200

TCPIP = "127.0.0.1"
tcp_port = -1

port2data = {}
data2port = {}
tcp_sockets = []


## create PDU
class PDU:
    def __init__(self, type, data):
        self.type = type
        self.data = data


## create UDP socket in order to connect to central server
udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)


def create_tcp_socket():
    tcp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    while True:
        try:
            port = random.randint(30000, 65000)
            tcp_socket.bind((TCPIP, port))
            tcp_socket.listen()
            break
        except:
            print("error in creating socket, trying again...")
            continue

    return port, tcp_socket


def binary_to_pdu(binary_pdu):
    return pickle.loads(binary_pdu)


def pdu_to_binary(pdu):
    return pickle.dumps(pdu)


def select_file_name():
    pass


def register_file(file_name):
    global tcp_port
    pdu = PDU('R', (file_name, (TCPIP, tcp_port)))
    binary_pdu = pdu_to_binary(pdu)
    udp_socket.sendto(binary_pdu, (UDPIP, UDPPort))
    message, address = udp_socket.recvfrom(bufferSize)
    pdu = binary_to_pdu(message)
    if pdu.type == 'A':
        port2data[tcp_port] = file_name
        data2port[file_name] = tcp_port
        # tcp_socket.bind((TCPIP, port))
        # tcp_socket.listen()
    print(pdu.data)


## remove a file from central server
def remove_file(filename):
    pass


## download file from another peer
def download_file(filename, ip, port):
    ## create TCP connection
    ## request download (D type PDU)
    ## recieve file (E,C or L type PDUs)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip, port))
        pdu = PDU('D', filename)
        s.sendall(pdu_to_binary(pdu))
        pdu = None
        print("Connected")
        data = []
        bd = bytes()  # b""
        while True:
            d = s.recv(bufferSize)
            if len(d) == 0:
                break
            bd += d
            try:
                pdu = binary_to_pdu(bd)
                data.append(pdu.data)
                s.sendall(b'yes')
            except pickle.UnpicklingError as e:
                continue

            bd = bytes()
            if pdu.type == 'L': break

        print("Downloaded.")
        s.shutdown(socket.SHUT_WR)
        s.close()
        with open(f"/Users/alireza/Desktop/Code/991/Internet Engineering/HW1/P1/downloaded/{filename}", 'wb') as file:
            file.writelines(data)
            file.close()
    except KeyboardInterrupt:
        raise ConnectionError("Download error.")


def upload_file(file_name, s, address):
    with open(f"/Users/alireza/Desktop/Code/991/Internet Engineering/HW1/P1/{file_name}", 'rb') as file:
        counter = 0
        bytes100 = bytes()
        print("sending...")
        while True:
            byte = file.read(1)
            if not byte:
                pdu = PDU('L', bytes100)
                binary_pdu = pdu_to_binary(pdu)
                s.sendall(binary_pdu)
                break
            counter += 1
            bytes100 += byte

            if counter == 99:
                pdu = PDU('F', bytes100)
                binary_pdu = pdu_to_binary(pdu)
                s.sendall(binary_pdu)
                d = s.recv(bufferSize)
                if d.decode() == 'yes':
                    pass
                else:
                    print("Failed to send file, cancelling...")
                    print("Enter command: ")
                    return -1
                counter = 0
                bytes100 = bytes()
        print(f"sent {file_name} to {address}.")
        print("Enter command: ")
    # print("LAST CHUNK IS SENT")
    # pdu = PDU('L', bytes100)
    # binary_pdu = pdu_to_binary(pdu)
    # print(sys.getsizeof(binary_pdu), sys.getsizeof(pdu))
    # socket.sendall(binary_pdu)
    # socket.sendall(' '.encode())
    # s.shutdown(socket.SHUT_WR)
    # s.close()
    # print("upload finished")


## create TCP socket listening to other peers requesting files in this peer
## port must be available (not in used by the others sockets)
## random port number? ask user? your choice!

## one socket for all other peers? one socket per peer? one socket per file? your choice!

## one socket for all peers approach:
## listen to other peers infinitely
## put this function in seperate thread? process? your choice!
def download_socket():
    global tcp_port
    tcp_port, tcp_socket = create_tcp_socket()
    tcp_sockets = [tcp_socket]
    ## infinite service loop
    ## use select
    while True:
        try:
            readables, _, _ = select.select(tcp_sockets, [], [])
            for r in readables:
                if r == tcp_socket:
                    client, addr = r.accept()
                    print(f"got new connection from {addr}")
                    message = client.recv(bufferSize)
                    pdu = binary_to_pdu(message)
                    threading.Thread(target=upload_file, args=(pdu.data, client, addr)).start()
        except (KeyboardInterrupt, SystemExit):
            print("exiting...")
            exit()


if __name__ == '__main__':
    ## main driver
    ## ask user for input and answers user's requests according to input
    t = threading.Thread(target=download_socket)
    t.daemon = True
    t.start()

    while True:

        print("Enter command: ")
        command = input()
        ## based on user input:
        ## O -> get files list from central server -> ask user for a file -> download that file
        ## L -> get local files
        ## R -> register a new file on central server
        ## U -> remove a registered file from central server
        ## E -> exit program

        if command == 'O':
            ## fetch files list from server
            ## ask user for specific file
            ## fetch that file's address from server
            ## download that file from the owner peer
            data = []
            filename = "data.pickle"
            pdu = pdu_to_binary(PDU('O', ''))
            udp_socket.sendto(pdu, (UDPIP, UDPPort))
            bd = bytes()
            while True:
                d = udp_socket.recv(bufferSize)
                if len(d) == 0:
                    break
                bd += d
                try:
                    pdu = binary_to_pdu(bd)
                    data.append(pdu.data)
                except pickle.UnpicklingError as e:
                    continue

                bd = bytes()
                if pdu.type == 'L': break

            with open(f"/Users/alireza/Desktop/Code/991/Internet Engineering/HW1/P1/downloaded/{filename}",
                      'wb') as file:
                file.writelines(data)
            with open(f"/Users/alireza/Desktop/Code/991/Internet Engineering/HW1/P1/downloaded/{filename}",
                      'wb') as file:
                file.writelines(data)
            with open(f"/Users/alireza/Desktop/Code/991/Internet Engineering/HW1/P1/downloaded/{filename}",
                      'rb') as file:
                data_center = pickle.load(file)

            print("\n*********************\n")
            for key, value in data_center.items():
                print(f"Peer with UDP ip, port {key} registered files:")
                for v in value:
                    name, (ip, port) = v
                    print(f"    '{name}' with TCP ip {ip} and port {port}.")
                print("\n*********************\n")

        elif command == 'L':
            pass

        elif command == 'R':
            print("Enter file name:")
            file_name = input()
            register_file(file_name)

        elif command == 'S':
            print("Enter file name:")
            file_name = input()

            if file_name in port2data.values():
                print("File is available locally")
                continue

            pdu = PDU('S', file_name)
            udp_socket.sendto(pdu_to_binary(pdu), (UDPIP, UDPPort))
            message, address = udp_socket.recvfrom(bufferSize)
            pdu = binary_to_pdu(message)
            if pdu.type == 'S':
                ip, port = pdu.data
                print(pdu.data, ip, port, file_name)
                print("Downloading file...")
                t = time.time()
                try:
                    download_file(file_name, ip, port)
                    print(f"Finished downloading in {(time.time() - t) / 60} minutes.")
                    register_file(file_name)
                except ConnectionError as e:
                    print(e)
            elif pdu.type == 'E':
                print(pdu.data)


        elif command == 'U':
            print("Enter file name:")
            file_name = input()
            pdu = PDU('U', file_name)
            udp_socket.sendto(pdu_to_binary(pdu), (UDPIP, UDPPort))
            message, address = udp_socket.recvfrom(bufferSize)
            pdu = binary_to_pdu(message)
            if pdu.type == 'A':
                print("Removed successfully.")
                os.remove(file_name)
                del port2data[data2port[file_name]]
                del data2port[file_name]
            elif pdu.type == 'E':
                print(pdu.data)

        elif command == 'E':
            for fn, value in data2port.items():
                pdu = PDU('U', fn)
                udp_socket.sendto(pdu_to_binary(pdu), (UDPIP, UDPPort))
                udp_socket.recvfrom(bufferSize)
            print("Exiting the program...")
            exit()

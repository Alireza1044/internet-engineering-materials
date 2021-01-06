import socket
import pickle
import threading
import random

UDPIP = "10.0.0.4"
UDPPort = 20001
bufferSize = 200

## create UDP socket
udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
udp_socket.bind((UDPIP, UDPPort))

data_center = {}


## create PDU
class PDU:
    def __init__(self, type, data):
        self.type = type
        self.data = data


## this function converts recieved binary data to PDU
def binary_to_pdu(binary_data):
    return pickle.loads(binary_data)


## this function converts PDU to binary data
## so we are able to send it through socket
def pdu_to_binary(pdu):
    return pickle.dumps(pdu)


def find_file(file_name):
    global data_center
    addresses = []
    for key, value in data_center.items():
        for fn, address in value:
            if file_name == fn:
                addresses.append(address)
    if len(addresses) == 0:
        return None
    idx = random.randint(0, len(addresses) - 1)
    return addresses[idx]


def remove_file(file_name, address):
    global data_center

    if address in data_center:
        for item in data_center[address]:
            if file_name in item:
                data_center[address].remove(item)
                return True
    return False


def create_error(data):
    return PDU('E', data)


def create_ack(data):
    return PDU('A', data)


def upload_files_list(file_name, address):
    with open(f"{file_name}", 'rb') as file:
        counter = 0
        bytes100 = bytes()
        print("sending...")
        while True:
            byte = file.read(1)
            if not byte:
                pdu = PDU('L', bytes100)
                binary_pdu = pdu_to_binary(pdu)
                udp_socket.sendto(binary_pdu, address)
                break
            counter += 1
            bytes100 += byte

            if counter == 99:
                pdu = PDU('O', bytes100)
                binary_pdu = pdu_to_binary(pdu)
                udp_socket.sendto(binary_pdu, address)
                counter = 0
                bytes100 = bytes()
        print(f"sent {file_name} to {address}.")
        print("Enter command: ")


if __name__ == '__main__':

    while True:
        print("\n--------------------\n")
        print(f"Listening on local ip {UDPIP}:{UDPPort}...\n")

        ## Recieve messages from client (UDP socket)
        message, address = udp_socket.recvfrom(bufferSize)


        ## convert recieved binary data to PDU
        pdu = binary_to_pdu(message)

        print(f"received data {pdu.data} from {address}\n")

        ## if the pair wants to register file
        if pdu.type == 'R':
            file_name, addr = pdu.data
            print(f"Peer {address} wants to register file {file_name} with TCP IP, Port {addr[0]}:{addr[1]}\n")
            if address in data_center:
                if len([item for item in data_center[address] if
                        file_name in item]) > 0:
                    error = create_error("File was already registered.")
                    print("Error: This file was already registered by this peer.")
                    udp_socket.sendto(pdu_to_binary(error), address)
                    continue
                else:
                    data_center[address].append((file_name, (address[0], addr[1])))
                    print("Successful.")
            else:
                data_center[address] = [(file_name, (address[0], addr[1]))]
                print("Successful.")
            ack = create_ack("File was registered successfully.")
            udp_socket.sendto(pdu_to_binary(ack), address)

        ## if the pait wants to download a file
        elif pdu.type == 'S':
            file_name = pdu.data
            print(f"Peer {address} wants to download file {file_name}\n")
            file_addr = find_file(file_name)
            if file_addr == None:
                err = PDU('E', 'File not found!')
                print("Error: File not found!")
                udp_socket.sendto(pdu_to_binary(err), address)
                continue
            print(f"Peer address sent.")
            ack = PDU('S', file_addr)
            print(file_addr)
            udp_socket.sendto(pdu_to_binary(ack), address)


        ## if the pair wants to remove a specific file
        elif pdu.type == 'U':
            file_name = pdu.data
            print(f"Peer {address} wants to remove file {file_name}\n")
            if remove_file(file_name, address):
                pdu = PDU('A', "Deleted successfully.")
                print(f"Deleted successfully.")
                udp_socket.sendto(pdu_to_binary(pdu), address)
                continue
            err = PDU('E', f"Couldn't remove the file {file_name}")
            print("Error: Couldn't remove the file")
            udp_socket.sendto(pdu_to_binary(err), address)


        ## if pair wants to know the list of all files available in the network
        elif pdu.type == 'O':
            print(f"Peer {address} wants a list of files registered in the server.\n")
            filename = "data.pickle"
            with open(f"{filename}", 'wb') as file:
                pickle.dump(data_center, file)
            upload_files_list(filename, address)

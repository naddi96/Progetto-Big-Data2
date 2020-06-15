
import socket
import time
HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 444        # Port to listen on (non-privileged ports are > 1023)
wait_time=0


def start_from_ram():
    file1 = open('bus-breakdown-and-delays.csv', 'r') 
    # file1.readline()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        conn, addr = s.accept()
        with conn:
            print('Connected by', addr)
            for x in file1.readlines()[1:]:
                time.sleep(wait_time)
                conn.sendall( bytes(x, 'utf-8') )

def start_from_disk():
    file1 = open('bus-breakdown-and-delays.csv', 'r') 
    file1.readline()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        conn, addr = s.accept()
        with conn:
            print('Connected by', addr)
            while True:
                x=file1.readline()
                if (x==""):
                    break
                time.sleep(wait_time)
                conn.sendall( bytes(x, 'utf-8') )

start_from_ram()
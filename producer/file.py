
import socket
import time
HOST = '127.0.0.1'  # Standard loopback interface address (localhost)
PORT = 444        # Port to listen on (non-privileged ports are > 1023)
file1 = open('bus-breakdown-and-delays.csv', 'r') 
count = 0

file1.readline()
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    with conn:
        print('Connected by', addr)
        while True:
            time.sleep(0.3)
            conn.sendall( bytes(file1.readline(), 'utf-8') )


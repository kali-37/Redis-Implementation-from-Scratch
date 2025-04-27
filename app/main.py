import socket  


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    con ,usr_addr = server_socket.accept() 
    con.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()

import socket  


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.accept() 


if __name__ == "__main__":
    main()

import socket  
import asyncio

HOST = "127.0.0.1"
PORT = 6379

async def redis_serv(con, usr_addr):
    loop = asyncio.get_running_loop()
    try:
        while True:
                data = await loop.sock_recv(con,1024)
                if not data : 
                    break
                message = data.decode().strip()
                if "PING" in message:
                    await loop.sock_sendall(con,b"+PONG\r\n")
    except ConnectionResetError:
        pass
    finally:
        con.close()

async def main():
    loop = asyncio.get_running_loop()
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
    s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    s.bind((HOST,PORT))
    s.listen()
    s.setblocking(False)
    try:
        while True:
            con , usr_addr = await loop.sock_accept(s)
            con.setblocking(False)
            asyncio.create_task(redis_serv(con,usr_addr))
    except KeyboardInterrupt:
        s.close()
    finally:
        s.close()
        

if __name__ == "__main__":
    asyncio.run(main())

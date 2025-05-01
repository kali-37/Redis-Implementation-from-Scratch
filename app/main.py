import socket
import asyncio
import time
from dataclasses import dataclass
from enum import Enum


HOST = "127.0.0.1"
PORT = 6379


@dataclass
class KeyValuePair:
    key: str
    value: str
    expiry: float= 0  # 0 means no expiry


class Commands(Enum):
    PING = "PING"
    ECHO = "ECHO"
    SET = "SET"
    GET = "GET"
    PX = "PX"
    EX = "EX"


class ProtocolCommands(Enum):
    ASTERISK = "*"
    DOLLAR = "$"


@dataclass
class DataTypes:
    SIMPLE_STRINGS: str = "+"
    SIMPLE_ERRORS: str = "-"
    INTEGERS: str = ":"
    BULK_STRINGS: str = "$"
    ARRAYS: str = "*"
    NULLS: str = "_"
    BOOLEANS: str = "#"
    DOUBLES: str = ","
    BIG_NUMBERS: str = "("
    BULK_ERRORS: str = "!"
    VERBATIM_STRINGS: str = "="
    MAPS: str = "%"
    ATTRIBUTES: str = ""
    SETS: str = "~"
    PUSHES: str = ">"


def is_command(command: str):
    try:
        return Commands(command.upper())
    except ValueError:
        return None


class BulkRepr:
    def __init__(self, list_of_strings):
        self.list_of_strings = list_of_strings

    def create_normal_repr(self):
        repr_str = ""
        for string in self.list_of_strings:
            repr_str += f"${len(string)}\r\n{string}\r\n"
        return repr_str
    
    def __repr__(self):
        return self.create_normal_repr()


class Protocol:
    def __init__(self, data):
        self.data = data.decode('utf-8')
        self.pointer = 0
        self.array_len = 0
        self.list_of_strings = []

    def is_unbound(self):
        return self.pointer < len(self.data)

    def safe_increment(self, pointer_inc=1):
        self.pointer += pointer_inc
        return self.is_unbound()

    def handle_asterisk(self):
        self.safe_increment()
        
        array_len_str = ""
        while self.is_unbound() and self.data[self.pointer].isdigit():
            array_len_str += self.data[self.pointer]
            self.safe_increment()
        
        if array_len_str:
            self.array_len = int(array_len_str)
        
        if self.is_unbound() and self.data[self.pointer:self.pointer+2] == "\r\n":
            self.safe_increment(2)

    def handle_dollar(self):
        self.safe_increment()
        
        length_str = ""
        while self.is_unbound() and self.data[self.pointer].isdigit():
            length_str += self.data[self.pointer]
            self.safe_increment()
        
        if not length_str:
            return False
            
        string_length = int(length_str)
        
        if self.is_unbound() and self.data[self.pointer:self.pointer+2] == "\r\n":
            self.safe_increment(2)
        else:
            return False
            
        if self.pointer + string_length <= len(self.data):
            message = self.data[self.pointer:self.pointer+string_length]
            self.list_of_strings.append(message)
            self.safe_increment(string_length)
            
            if self.is_unbound() and self.data[self.pointer:self.pointer+2] == "\r\n":
                self.safe_increment(2)
            
            return True
        return False

    def identify_protocol(self):
        while self.is_unbound():
            current_val = self.data[self.pointer]
            
            if current_val == "*":
                self.handle_asterisk()
            elif current_val == "$":
                if not self.handle_dollar():
                    break
            else:
                if not self.safe_increment():
                    break
                
        return self.list_of_strings


redis_store = {}


def check_expiry(key):
    """Check if a key has expired and remove it if necessary"""
    if key in redis_store and redis_store[key].expiry != 0:
        print(f"EXPIRY TIME {redis_store[key].expiry} and CURRENT TIME { time.time()}")
        if redis_store[key].expiry < time.time():
            del redis_store[key]
            return False
    return key in redis_store


async def redis_serv(con, usr_addr):
    loop = asyncio.get_running_loop()
    try:
        while True:
            data = await loop.sock_recv(con, 1024)
            if not data:
                break
                
            protocol = Protocol(data)
            commands = protocol.identify_protocol()
            
            if not commands:
                await loop.sock_sendall(con, b"-ERR Protocol error\r\n")
                continue
            
            command = commands[0].upper() if commands else ""
            
            if command == "PING":
                await loop.sock_sendall(con, b"+PONG\r\n")
            
            elif command == "ECHO":
                if len(commands) > 1:
                    response = f"+{commands[1]}\r\n"
                    await loop.sock_sendall(con, response.encode('utf-8'))
                else:
                    await loop.sock_sendall(con, b"-ERR wrong number of arguments for 'echo' command\r\n")
            
            elif command == "SET":
                if len(commands) < 3:
                    await loop.sock_sendall(con, b"-ERR wrong number of arguments for 'set' command\r\n")
                else:
                    key = commands[1]
                    value = commands[2]
                    expiry = 0
                    
                    if len(commands) > 3 and len(commands) >= 5:
                        if commands[3].upper() == "PX":
                            try:
                                expiry =time.time() + int(commands[4]) / 1000
                                print("EXPIRY was: ",expiry)
                            except ValueError:
                                await loop.sock_sendall(con, b"-ERR invalid expire time in 'set' command\r\n")
                                continue
                        elif commands[3].upper() == "EX":
                            try:
                                expiry =time.time() + int(commands[4])
                            except ValueError:
                                await loop.sock_sendall(con, b"-ERR invalid expire time in 'set' command\r\n")
                                continue
                    
                    redis_store[key] = KeyValuePair(key=key, value=value, expiry=expiry)
                    await loop.sock_sendall(con, b"+OK\r\n")
            
            elif command == "GET":
                if len(commands) != 2:
                    await loop.sock_sendall(con, b"-ERR wrong number of arguments for 'get' command\r\n")
                else:
                    key = commands[1]
                    if check_expiry(key):
                        value = redis_store[key].value
                        response = f"${len(value)}\r\n{value}\r\n"
                        await loop.sock_sendall(con, response.encode('utf-8'))
                    else:
                        await loop.sock_sendall(con, "$-1\r\n".encode('utf-8'))
            
            else:
                error_msg = f"-ERR unknown command '{command}'\r\n"
                await loop.sock_sendall(con, error_msg.encode('utf-8'))

    except ConnectionResetError:
        pass
    finally:
        con.close()


async def main():
    loop = asyncio.get_running_loop()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen()
    s.setblocking(False)
    print(f"Redis server running on {HOST}:{PORT}")
    
    try:
        while True:
            con, usr_addr = await loop.sock_accept(s)
            con.setblocking(False)
            asyncio.create_task(redis_serv(con, usr_addr))
    except KeyboardInterrupt:
        print("Server shutting down...")
    finally:
        s.close()


if __name__ == "__main__":
    asyncio.run(main())
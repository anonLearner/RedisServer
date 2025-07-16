import threading
import time
import socket  # noqa: F401


"""
In Redis serialization protocol,

for strings, we need to send a response in the format: $<length>\r\n<data>\r\n
for arrays, we need to send a response in the format: *<number-of-elements>\r\n<element-1>...<element-n>

# | Prefix | Type          | Purpose                                     |
# |--------|---------------|---------------------------------------------|
# | `*`    | Array         | Introduces a collection of items            |
# | `$`    | Bulk String   | Introduces a string with a specified length |
# | `:`    | Integer       | A numeric response                          |
# | `+`    | Simple String | Human-readable success messages             |
# | `-`    | Error         | Error messages                              |

"""

data_in_memory = {}
expiration_times = {}

def parse_data(data: str):
    """
    Parses the incoming data and returns a command.
    The data is expected to be in the format of Redis protocol.
    """
    def parse_next(data):
        if not data:
            return None, ''
        if data[0] == '*':
            # Array type
            crlf = data.find('\r\n')
            count = int(data[1:crlf])
            rest = data[crlf+2:]
            arr = []
            for _ in range(count):
                elem, rest = parse_next(rest)
                arr.append(elem)
            return arr, rest
        elif data[0] == '$':
            crlf = data.find('\r\n')
            length = int(data[1:crlf])
            start = crlf+2
            end = start+length
            bulk = data[start:end]
            rest = data[end+2:]  # skip \r\n after bulk string
            return bulk, rest
        elif data[0] == ':':
            crlf = data.find('\r\n')
            num = int(data[1:crlf])
            rest = data[crlf+2:]
            return num, rest
        elif data[0] == '+':
            crlf = data.find('\r\n')
            simple = data[1:crlf]
            rest = data[crlf+2:]
            return simple, rest
        elif data[0] == '-':
            crlf = data.find('\r\n')
            err = f"Error: {data[1:crlf]}"
            rest = data[crlf+2:]
            return err, rest
        else:
            return None, ''

    result, _ = parse_next(data)
    return result
    
def format_resp(value):
    if isinstance(value, str):
        # Bulk string for echo, simple string for OK/PONG
        if value.startswith("Error: "):
            return f"-{value[7:]}\r\n"
        elif value in ("OK", "PONG"):
            return f"+{value}\r\n"
        else:
            return f"${len(value)}\r\n{value}\r\n"
    elif isinstance(value, int):
        return f":{value}\r\n"
    elif isinstance(value, list):
        resp = f"*{len(value)}\r\n"
        for item in value:
            resp += format_resp(item)
        return resp
    elif value is None:
        return "$-1\r\n"  # Null bulk string
    else:
        return f"-Error: Unknown type\r\n"

def send_command(client_conn, response):
    command = response[0].lower() if response and isinstance(response, list) and response[0] else None
    if command is None:
        resp = format_resp("Error: Unknown command")
    elif command == "ping":
        resp = format_resp("PONG")
    elif command == "quit":
        resp = format_resp("OK")
    elif command == "echo":
        resp_str = response[1] if len(response) > 1 else ""
        resp = format_resp(resp_str)
    elif command == "set":
        if len(response) < 3:
            resp = format_resp("Error: SET command requires a key and a value")
        else:
            key = response[1]
            value = response[2]
            # If there are more than 3 arguments, check for PX/EX
            if len(response) > 3:
                option = response[3].lower()
                if option == "px" and len(response) > 4:
                    # Store expiration as absolute time in ms
                    import time
                    expiration_time = int(response[4])
                    expiration_times[key] = time.time() * 1000 + expiration_time
                elif option == "ex" and len(response) > 4:
                    # seconds are given, convert to ms
                    expiration_time = int(response[4]) * 1000
                    expiration_times[key] = time.time() * 1000 + expiration_time
            # Store the key-value pair
            data_in_memory[key] = value
            resp = format_resp("OK")
    elif command == "get":
        if len(response) < 2:
            resp = format_resp("Error: GET command requires a key")
        else:
            key = response[1]
            if expiration_times.get(key, 0) < time.time() * 1000:
                # If the key has expired, return -1
                value = None
            else:
                value = data_in_memory.get(key, None)
            resp = format_resp(value) #format_resp is handling the None case, sending -1
    else:
        resp = format_resp("Error: Unknown command")
    client_conn.sendall(resp.encode('utf-8'))

def handle_client(client_conn):
    while True:
        data:bytes = client_conn.recv(1024)
        if not data:
            break
        data = data.decode('utf-8')
        command = parse_data(data)
        send_command(client_conn, command)
        
    client_conn.close()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
    while True: 
        conn, addr = server_socket.accept()  # wait for client
        print(f"Accepted connection from {addr}")
        # Handle each connection in a new thread      
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()

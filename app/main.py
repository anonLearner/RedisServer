import threading

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

def parse_data(data: str):
    """
    Parses the incoming data and returns a command.
    The data is expected to be in the format of Redis protocol.
    """
    if data.startswith('*'):
        # Array type
        elements = data[1:].strip().split('\r\n')
        "but this list also contains different types of elements, so we need to parse them individually"
        elements = [parse_data(element) for element in elements if element]
        return elements
    elif data.startswith('$'):
        # Bulk string type
        length = int(data[1:data.index('\r\n')])
        return data[data.index('\r\n') + 2:data.index('\r\n') + 2 + length]
    elif data.startswith(':'):
        # Integer type
        return int(data[1:].strip())
    elif data.startswith('+'):
        # Simple string type
        return data[1:].strip()
    elif data.startswith('-'):
        # Error type
        return f"Error: {data[1:].strip()}"
    else:
        return None
    
def send_command(client_conn, response):
    command = response[0].lower()
    if command is None:
        return
    elif command == "ping":
        response = "+PONG\r\n"
    elif command == "quit":
        response = "+OK\r\n"
    elif command == "echo":
        resp_str = response[1]
        response = f"${len(resp_str)}\r\n{resp_str}\r\n"
    else:
        response = "-Error: Unknown command\r\n"
    client_conn.sendall(response)

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

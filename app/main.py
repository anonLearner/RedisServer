import threading
import time
import socket
import argparse


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
config = {}
REPLICA_NODES = []


def parse_data(data: str):
    """
    Parses the incoming data and returns (command, rest_of_buffer).
    The data is expected to be in the format of Redis protocol.
    """

    def parse_next(data):
        if not data:
            return None, ""
        if data[0] == "*":
            crlf = data.find("\r\n")
            if crlf == -1:
                return None, data  # incomplete
            try:
                count = int(data[1:crlf])
            except ValueError:
                return None, data
            rest = data[crlf + 2 :]
            arr = []
            for _ in range(count):
                elem, rest = parse_next(rest)
                if elem is None:
                    return None, data  # incomplete
                arr.append(elem)
            return arr, rest
        elif data[0] == "$":
            crlf = data.find("\r\n")
            if crlf == -1:
                return None, data  # incomplete
            try:
                length = int(data[1:crlf])
            except ValueError:
                return None, data
            start = crlf + 2
            end = start + length
            if len(data) < end + 2:
                return None, data  # incomplete
            bulk = data[start:end]
            rest = data[end + 2 :]  # skip \r\n after bulk string
            return bulk, rest
        elif data[0] == ":":
            crlf = data.find("\r\n")
            if crlf == -1:
                return None, data
            try:
                num = int(data[1:crlf])
            except ValueError:
                return None, data
            rest = data[crlf + 2 :]
            return num, rest
        elif data[0] == "+":
            crlf = data.find("\r\n")
            if crlf == -1:
                return None, data
            simple = data[1:crlf]
            rest = data[crlf + 2 :]
            return simple, rest
        elif data[0] == "-":
            crlf = data.find("\r\n")
            if crlf == -1:
                return None, data
            err = f"Error: {data[1:crlf]}"
            rest = data[crlf + 2 :]
            return err, rest
        else:
            return None, data

    result, rest = parse_next(data)
    return result, rest  # <-- fix: always return a tuple


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


def read_keys_from_rdb_file():
    rdb_file_loc = config["dir"] + "/" + config["dbfilename"]
    keys_from_file = []
    try:
        with open(rdb_file_loc, "rb") as f:
            # Read bytes until b'\xfb' is found
            while True:
                byte = f.read(1)
                if byte == b"\xfb":
                    break
            import struct

            next_bytes = f.read(1)
            total_keys = struct.unpack("<B", next_bytes)[0]
            next_bytes = f.read(1)
            expiry_keys = struct.unpack("<B", next_bytes)[0]

            for _ in range(total_keys):
                expiry_time = None
                expiry_flag = f.read(1)
                if expiry_flag == b"\xfc":
                    expiry_time = int.from_bytes(f.read(8), byteorder="little")
                elif expiry_flag == b"\xfd":
                    expiry_time = int.from_bytes(f.read(4), byteorder="little") * 1000
                else:
                    # No expiry, rewind one byte
                    f.seek(-1, 1)
                value_type = f.read(1)
                # Read key name
                key_len = struct.unpack("<B", f.read(1))[0]
                key_name = f.read(key_len).decode("utf-8")
                # Read value
                val_len = struct.unpack("<B", f.read(1))[0]
                value = f.read(val_len).decode("utf-8")

                data_in_memory[key_name] = value
                if expiry_time is not None:
                    expiration_times[key_name] = expiry_time

    except FileNotFoundError:
        return None  # Return empty list if file does not exist
    except Exception as e:
        return [f"Error reading RDB file: {str(e)}"]


def start_replica_sync(command):
    if REPLICA_NODES:
        for replica in REPLICA_NODES:
            try:
                replica.sendall(format_resp(command).encode("utf-8"))
            except Exception as e:
                print(f"Error sending command to replica: {e}")


def send_command(client_conn, response, replica):
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
                    expiration_time = int(response[4])
                    expiration_times[key] = time.time() * 1000 + expiration_time
                elif option == "ex" and len(response) > 4:
                    # seconds are given, convert to ms
                    expiration_time = int(response[4]) * 1000
                    expiration_times[key] = time.time() * 1000 + expiration_time
            # Store the key-value pair
            data_in_memory[key] = value
            start_replica_sync(response)
            resp = format_resp("OK")
    elif command == "get":
        if len(response) < 2:
            resp = format_resp("Error: GET command requires a key")
        else:
            key = response[1]
            expiry = expiration_times.get(key)
            if expiry is not None and int(time.time() * 1000) > expiry:
                value = None
            else:
                value = data_in_memory.get(key, None)
            resp = format_resp(value)
    elif command == "config":
        if len(response) < 3:
            resp = format_resp("Error: CONFIG SET command requires a parameter and a value")
        else:
            subcommand = response[1].lower()
            if subcommand == "set":
                if len(response) < 4:
                    resp = format_resp("Error: CONFIG SET command requires a parameter and a value")
                else:
                    param = response[2]
                    value = response[3]
                    config[param] = value
                    resp = format_resp("OK")
            elif subcommand == "get":
                param = response[2]
                if config.get(param):
                    resp = format_resp([param, config[param]])
                else:
                    resp = format_resp(None)
            else:
                resp = format_resp(f"Error: Unknown CONFIG subcommand '{subcommand}'")
    elif command == "keys":
        if len(response) < 2:
            resp = format_resp("Error: KEYS command requires a pattern")
        else:
            pattern = response[1]
            keys = None
            if pattern == "*":
                keys = list(data_in_memory.keys())
            resp = format_resp(keys)
    elif command == "info":
        if len(response) < 2:
            resp = format_resp("Error: INFO command requires an argument")
        else:
            argument = response[1]
            if argument == "replication":
                if config.get("replicaof"):
                    replica_info = f"role:slave"
                else:
                    replica_info = "role:master"
                replica_info += f"\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0"
                resp = format_resp(replica_info)
    elif command == "replconf":
        if len(response) >= 3 and response[1].lower() == "getack" and response[2] == "*":
            resp = format_resp(["REPLCONF", "ACK", "0"])
            client_conn.sendall(resp.encode("utf-8"))
            return  # Important: do not fall through and send again
        else:
            resp = format_resp("OK")
            if not replica:
                client_conn.sendall(resp.encode("utf-8"))
            return
    elif command == "psync":
        REPLICA_NODES.append(client_conn)
        resp = format_resp("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0")
        client_conn.sendall(resp.encode("utf-8"))
        empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        client_conn.sendall(
            b"$"
            + str(len(bytes.fromhex(empty_rdb_hex))).encode("utf-8")
            + b"\r\n"
            + bytes.fromhex(empty_rdb_hex)
        )

    else:
        resp = format_resp("Error: Unknown command")

    if (not replica) and (command != "psync"):
        client_conn.sendall(resp.encode("utf-8"))


def handle_client(client_conn, replica=False):
    buffer = b""
    while True:
        data = client_conn.recv(4096)
        if not data:
            break
        buffer += data
        while buffer:
            # Handle RDB file as a bulk string in replica mode
            if replica and buffer.startswith(b"$"):
                crlf = buffer.find(b"\r\n")
                if crlf == -1:
                    break  # Incomplete header
                try:
                    length = int(buffer[1:crlf])
                except ValueError:
                    break  # Malformed header
                total_len = crlf + 2 + length + 2  # header + data + trailing \r\n
                if len(buffer) < total_len:
                    break  # Wait for full RDB file
                # Optionally, store the RDB file:
                # rdb_data = buffer[crlf+2:crlf+2+length]
                buffer = buffer[total_len:]
                continue

            # Try to decode as much as possible for RESP command parsing
            try:
                decoded = buffer.decode("utf-8", errors="replace")
            except Exception:
                break  # Wait for more data

            command, rest = parse_data(decoded)
            if command is not None:
                # Figure out how many bytes were consumed
                bytes_consumed = len(decoded) - len(rest)
                send_command(client_conn, command, replica)
                buffer = buffer[bytes_consumed:]
            else:
                break
    client_conn.close()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Parse command-line arguments for --dir, --dbfilename and --port
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=str, default="/tmp/redis-data")
    parser.add_argument("--dbfilename", type=str, default="rdbfile")
    parser.add_argument("--port", type=int, default="6379")
    parser.add_argument(
        "--replicaof",
        type=str,
        default=None,
        help="IP addr and port of the replica server",
    )
    args = parser.parse_args()

    # Store in config dict
    config["dir"] = args.dir
    config["dbfilename"] = args.dbfilename
    config["port"] = args.port
    if args.replicaof is None:
        config["replicaof"] = None
    else:

        def send_to_master_node(conn, data, wait_for_cmd="OK", decode=True):
            conn.send(format_resp(data).encode("utf-8"))
            response = conn.recv(4028)
            if decode:
                response = response.decode("utf-8")
                parsed, _ = parse_data(response)
                if wait_for_cmd not in str(parsed):
                    raise Exception(
                        f"Expected response '{wait_for_cmd}', but got '{response}'"
                    )

        config["replicaof"] = args.replicaof.split()
        master_socket = socket.create_connection(
            (config["replicaof"][0], int(config["replicaof"][1]))
        )
        send_to_master_node(master_socket, ["PING"], "PONG")
        send_to_master_node(master_socket, ["REPLCONF", "listening-port", str(config["port"])], "OK"      )
        send_to_master_node(master_socket, ["REPLCONF", "capa", "psync2"], "OK")
        send_to_master_node(master_socket, ["PSYNC", "?", "-1"], "FULLRESYNC", decode=False)
        # connection to master node is established, start handling client connections
        threading.Thread(
            target=handle_client, args=(master_socket, True), daemon=True
        ).start()

    if args.dir and args.dbfilename:
        read_keys_from_rdb_file()

    server_socket = socket.create_server(("localhost", config["port"]), reuse_port=True)
    while True:
        conn, addr = server_socket.accept()  # wait for client
        print(f"Accepted connection from {addr}")
        # Handle each connection in a new thread
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
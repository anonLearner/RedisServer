import threading
import time
import socket
import argparse

data_in_memory = {}
expiration_times = {}
config = {}
REPLICA_NODES = []
REPLICA_ACKS = {}  # maps replica socket to last acknowledged offset
GLOBAL_OFFSET = 0  # incremented for each write

# --- RESP Utilities ---

def parse_data(data: str):
    def parse_next(data):
        if not data:
            return None, ""
        if data[0] == "*":
            crlf = data.find("\r\n")
            if crlf == -1:
                return None, data
            try:
                count = int(data[1:crlf])
            except ValueError:
                return None, data
            rest = data[crlf + 2 :]
            arr = []
            for _ in range(count):
                elem, rest = parse_next(rest)
                if elem is None:
                    return None, data
                arr.append(elem)
            return arr, rest
        elif data[0] == "$":
            crlf = data.find("\r\n")
            if crlf == -1:
                return None, data
            try:
                length = int(data[1:crlf])
            except ValueError:
                return None, data
            start = crlf + 2
            end = start + length
            if len(data) < end + 2:
                return None, data
            bulk = data[start:end]
            rest = data[end + 2 :]
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
    return result, rest

def format_resp(value):
    if isinstance(value, str):
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
        return "$-1\r\n"
    else:
        return f"-Error: Unknown type\r\n"

# --- Command Registry ---

COMMAND_HANDLERS = {}

def command_handler(name):
    def decorator(func):
        COMMAND_HANDLERS[name.lower()] = func
        return func
    return decorator

# --- Command Handlers ---

@command_handler("ping")
def handle_ping(client_conn, response, replica):
    return format_resp("PONG")

@command_handler("echo")
def handle_echo(client_conn, response, replica):
    resp_str = response[1] if len(response) > 1 else ""
    return format_resp(resp_str)

@command_handler("quit")
def handle_quit(client_conn, response, replica):
    return format_resp("OK")

@command_handler("set")
def handle_set(client_conn, response, replica):
    if len(response) < 3:
        return format_resp("Error: SET command requires a key and a value")
    key = response[1]
    value = response[2]
    if len(response) > 3:
        option = response[3].lower()
        if option == "px" and len(response) > 4:
            expiration_time = int(response[4])
            expiration_times[key] = time.time() * 1000 + expiration_time
        elif option == "ex" and len(response) > 4:
            expiration_time = int(response[4]) * 1000
            expiration_times[key] = time.time() * 1000 + expiration_time
    data_in_memory[key] = value
    global GLOBAL_OFFSET
    if not replica:
        start_replica_sync(response)
        GLOBAL_OFFSET += len(format_resp(response).encode("utf-8"))
    return format_resp("OK")

@command_handler("get")
def handle_get(client_conn, response, replica):
    if len(response) < 2:
        return format_resp("Error: GET command requires a key")
    key = response[1]
    expiry = expiration_times.get(key)
    if expiry is not None and int(time.time() * 1000) > expiry:
        value = None
    else:
        value = data_in_memory.get(key, None)
    return format_resp(value)

@command_handler("config")
def handle_config(client_conn, response, replica):
    if len(response) < 3:
        return format_resp("Error: CONFIG SET command requires a parameter and a value")
    subcommand = response[1].lower()
    if subcommand == "set":
        if len(response) < 4:
            return format_resp("Error: CONFIG SET command requires a parameter and a value")
        param = response[2]
        value = response[3]
        config[param] = value
        return format_resp("OK")
    elif subcommand == "get":
        param = response[2]
        if config.get(param):
            return format_resp([param, config[param]])
        else:
            return format_resp(None)
    else:
        return format_resp(f"Error: Unknown CONFIG subcommand '{subcommand}'")

@command_handler("keys")
def handle_keys(client_conn, response, replica):
    if len(response) < 2:
        return format_resp("Error: KEYS command requires a pattern")
    pattern = response[1]
    keys = None
    if pattern == "*":
        keys = list(data_in_memory.keys())
    return format_resp(keys)

@command_handler("info")
def handle_info(client_conn, response, replica):
    if len(response) < 2:
        return format_resp("Error: INFO command requires an argument")
    argument = response[1]
    if argument == "replication":
        if config.get("replicaof"):
            replica_info = f"role:slave"
        else:
            replica_info = "role:master"
        replica_info += f"\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0"
        return format_resp(replica_info)
    return format_resp("")

@command_handler("replconf")
def handle_replconf(client_conn, response, replica):
    if len(response) >= 3 and response[1].lower() == "listening-port":
        resp = format_resp("OK")
        client_conn.sendall(resp.encode("utf-8"))
        return None
    elif len(response) >= 3 and response[1].lower() == "capa":
        resp = format_resp("OK")
        client_conn.sendall(resp.encode("utf-8"))
        return None
    elif len(response) >= 3 and response[1].lower() == "ack":
        try:
            ack_offset = int(response[2])
            REPLICA_ACKS[client_conn] = ack_offset
            print(f"[DEBUG] Updated REPLICA_ACKS (from send_command): {REPLICA_ACKS}")
        except Exception as e:
            print(f"[DEBUG] Failed to parse ACK offset: {e}")
        return None
    elif len(response) >= 3 and response[1].lower() == "getack" and response[2] == "*":
        resp = format_resp(["REPLCONF", "ACK", f"{config['offset']}"])
        print(f"[DEBUG] Sending REPLCONF ACK: {resp.strip()}")
        client_conn.sendall(resp.encode("utf-8"))
        return None
    else:
        print(f"[DEBUG] Unknown REPLCONF subcommand: {response}")
        return None

@command_handler("psync")
def handle_psync(client_conn, response, replica):
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
    return None

@command_handler("wait")
def handle_wait(client_conn, response, replica):
    print(f"[DEBUG][WAIT] Received WAIT command: {response}")
    if len(response) < 3:
        resp = format_resp("Error: WAIT command requires a number of replicas and a timeout")
    else:
        num_replicas = int(response[1])
        timeout = int(response[2]) / 1000.0
        print(f"[DEBUG][WAIT] num_replicas={num_replicas}, timeout={timeout}s, GLOBAL_OFFSET={GLOBAL_OFFSET}")

        if GLOBAL_OFFSET == 0:
            resp = format_resp(len(REPLICA_NODES))
            print(f"[DEBUG][WAIT] No writes yet, returning connected replicas: {len(REPLICA_NODES)}")
        else:
            start_time = time.time()
            target_offset = GLOBAL_OFFSET
            print(f"[DEBUG][WAIT] Waiting for offset: {target_offset}")

            for replica in REPLICA_NODES:
                try:
                    msg = format_resp(["REPLCONF", "GETACK", "*"])
                    print(f"[DEBUG][WAIT] Sending REPLCONF GETACK * to replica {replica.getpeername()}")
                    replica.sendall(msg.encode("utf-8"))
                except Exception as e:
                    print(f"[DEBUG][WAIT] Failed to send REPLCONF GETACK *: {e}")

            while True:
                elapsed = time.time() - start_time
                acknowledged = sum(1 for ack in REPLICA_ACKS.values() if ack >= target_offset)
                print(f"[DEBUG][WAIT] Replicas acknowledged offset {target_offset}: {acknowledged}/{num_replicas}")
                if acknowledged >= num_replicas:
                    print(f"[DEBUG][WAIT] Required replicas acknowledged, breaking loop.")
                    break
                if timeout > 0 and elapsed >= timeout:
                    print(f"[DEBUG][WAIT] Timeout reached ({timeout}s), breaking loop.")
                    break
                time.sleep(0.05)
            resp = format_resp(acknowledged)
            print(f"[DEBUG][WAIT] Final acknowledged count: {acknowledged}")
    print(f"[DEBUG][WAIT] Sending response to client: {resp.strip()}")
    client_conn.sendall(resp.encode("utf-8"))
    return None

# --- Replication Utilities ---

def start_replica_sync(command):
    if REPLICA_NODES:
        for replica in REPLICA_NODES:
            try:
                print(f"[DEBUG] Sending command to replica: {command}")
                replica.sendall(format_resp(command).encode("utf-8"))
            except Exception as e:
                print(f"Error sending command to replica: {e}")

def read_keys_from_rdb_file():
    rdb_file_loc = config["dir"] + "/" + config["dbfilename"]
    try:
        with open(rdb_file_loc, "rb") as f:
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
                    f.seek(-1, 1)
                value_type = f.read(1)
                key_len = struct.unpack("<B", f.read(1))[0]
                key_name = f.read(key_len).decode("utf-8")
                val_len = struct.unpack("<B", f.read(1))[0]
                value = f.read(val_len).decode("utf-8")
                data_in_memory[key_name] = value
                if expiry_time is not None:
                    expiration_times[key_name] = expiry_time
    except FileNotFoundError:
        return None
    except Exception as e:
        return [f"Error reading RDB file: {str(e)}"]

# --- Command Dispatcher ---

def send_command(client_conn, response, replica):
    print(f"[DEBUG] send_command called with response: {response}, replica: {replica}")
    command = response[0].lower() if response and isinstance(response, list) and response[0] else None
    handler = COMMAND_HANDLERS.get(command)
    if handler:
        resp = handler(client_conn, response, replica)
    else:
        resp = format_resp("Error: Unknown command")
        if (not replica):
            print(f"[DEBUG] Sending response to client: {resp.strip()}")
            client_conn.sendall(resp.encode("utf-8"))
    if (not replica) and (command != "psync") and resp:
        print(f"[DEBUG] Sending response to client: {resp.strip()}")
        client_conn.sendall(resp.encode("utf-8"))

# --- Client Handler ---

def handle_client(client_conn, replica=False, initial_buffer=b""):
    print(f"[DEBUG] handle_client started. Replica: {replica}")
    buffer = initial_buffer
    try:
        if buffer:
            print(f"[DEBUG] Initial buffer length: {len(buffer)}")
        while True:
            data = client_conn.recv(4096)
            print(f"[DEBUG] Received {len(data)} bytes from socket")
            if not data and not buffer:
                print("[DEBUG] No data and buffer empty, closing connection.")
                break
            buffer += data
            while buffer:
                print(f"[DEBUG] Buffer length: {len(buffer)}")
                if buffer.startswith(b"+"):
                    crlf = buffer.find(b"\r\n")
                    if crlf == -1:
                        break
                    line = buffer[1:crlf].decode("utf-8", errors="replace")
                    print(f"[DEBUG] Parsed simple string: {line}")
                    send_command(client_conn, [line], replica)
                    buffer = buffer[crlf+2:]
                    continue
                if buffer.startswith(b"$"):
                    crlf = buffer.find(b"\r\n")
                    if crlf == -1:
                        print("[DEBUG] Incomplete bulk string header, waiting for more data.")
                        break
                    try:
                        length = int(buffer[1:crlf])
                        print(f"[DEBUG] Bulk string length: {length}")
                    except ValueError:
                        print("[DEBUG] Malformed bulk string header.")
                        break
                    total_len = crlf + 2 + length + 2
                    if len(buffer) < total_len:
                        print("[DEBUG] Incomplete bulk string data, waiting for more data.")
                        break
                    rdb_data = buffer[crlf+2:crlf+2+length]
                    print(f"[DEBUG] Bulk string data received ({len(rdb_data)} bytes)")
                    if replica:
                        config["offset"] += total_len
                        print(f"[DEBUG] Updated replica offset: {config['offset']}")
                    buffer = buffer[total_len:]
                    continue
                if buffer.startswith(b"*"):
                    try:
                        decoded = buffer.decode("utf-8", errors="replace")
                    except Exception as e:
                        print(f"[DEBUG] Exception decoding buffer: {e}")
                        break
                    command, rest = parse_data(decoded)
                    if command is not None:
                        bytes_consumed = len(decoded) - len(rest)
                        print(f"[DEBUG] Parsed array command: {command}")
                        if replica:
                            config["offset"] += bytes_consumed
                            print(f"[DEBUG] Updated replica offset: {config['offset']}")
                        send_command(client_conn, command, replica)
                        buffer = buffer[bytes_consumed:]
                        continue
                    else:
                        print("[DEBUG] Incomplete array command, waiting for more data.")
                        break
                if buffer.startswith(b":"):
                    crlf = buffer.find(b"\r\n")
                    if crlf == -1:
                        break
                    num = int(buffer[1:crlf])
                    print(f"[DEBUG] Parsed integer: {num}")
                    send_command(client_conn, [num], replica)
                    buffer = buffer[crlf+2:]
                    continue
                if buffer.startswith(b"-"):
                    crlf = buffer.find(b"\r\n")
                    if crlf == -1:
                        break
                    err = buffer[1:crlf].decode("utf-8", errors="replace")
                    print(f"[DEBUG] Parsed error: {err}")
                    send_command(client_conn, [f"Error: {err}"], replica)
                    buffer = buffer[crlf+2:]
                    continue
                print("[DEBUG] Unrecognized or incomplete RESP type, waiting for more data.")
                break
        print("[DEBUG] Closing client connection.")
    except Exception as e:
        print(f"[DEBUG] Exception in handle_client: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("[DEBUG] Closing client connection.")
        client_conn.close()

# --- Main ---

def main():
    print("Logs from your program will appear here!")
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
    config["dir"] = args.dir
    config["dbfilename"] = args.dbfilename
    config["port"] = args.port
    server_socket = socket.create_server(("localhost", config["port"]), reuse_port=True)
    if args.replicaof is None:
        config["replicaof"] = None
    else:
        config["offset"] = 0
        def send_to_master_node(conn, data, wait_for_cmd="OK", decode=True):
            print(f"[DEBUG] Sending to master: {data}")
            conn.send(format_resp(data).encode("utf-8"))
            if decode:
                response = conn.recv(4028)
                print(f"[DEBUG] Received {len(response)} bytes from master")
                response_str = response.decode("utf-8")
                parsed, _ = parse_data(response_str)
                print(f"[DEBUG] Parsed master response: {parsed}")
                if wait_for_cmd not in str(parsed):
                    raise Exception(
                        f"Expected response '{wait_for_cmd}', but got '{response_str}'"
                    )
                return b""
        config["replicaof"] = args.replicaof.split()
        master_socket = socket.create_connection(
            (config["replicaof"][0], int(config["replicaof"][1]))
        )
        send_to_master_node(master_socket, ["PING"], "PONG")
        send_to_master_node(master_socket, ["REPLCONF", "listening-port", str(config["port"])], "OK")
        send_to_master_node(master_socket, ["REPLCONF", "capa", "psync2"], "OK")
        send_to_master_node(master_socket, ["PSYNC", "?", "-1"], "FULLRESYNC", decode=False)
        def read_line(sock):
            line = b""
            while not line.endswith(b"\r\n"):
                chunk = sock.recv(1)
                if not chunk:
                    break
                line += chunk
            return line
        fullresync_line = read_line(master_socket)
        print(f"[DEBUG] FULLRESYNC line: {fullresync_line}")
        rdb_header = read_line(master_socket)
        print(f"[DEBUG] RDB header: {rdb_header}")
        if not rdb_header.startswith(b"$"):
            raise Exception("Expected RDB bulk string header")
        rdb_len = int(rdb_header[1:-2])
        rdb_and_extra = b""
        while len(rdb_and_extra) < rdb_len + 2:
            chunk = master_socket.recv(rdb_len + 2 - len(rdb_and_extra))
            if not chunk:
                break
            rdb_and_extra += chunk
        rdb_data = rdb_and_extra[:rdb_len]
        trailing_crlf = rdb_and_extra[rdb_len:rdb_len+2]
        leftover = rdb_and_extra[rdb_len:]
        if len(leftover) < 4 or not (leftover.startswith(b"*") or leftover.startswith(b"$") or leftover.startswith(b"+") or leftover.startswith(b"-") or leftover.startswith(b":")):
            more = master_socket.recv(4096)
            leftover += more
        print(f"[DEBUG] RDB file received ({len(rdb_data)} bytes)")
        print(f"[DEBUG] Leftover after RDB: {leftover[:60]}")
        buffer = leftover
        while buffer:
            try:
                decoded = buffer.decode("utf-8", errors="replace")
                command, rest = parse_data(decoded)
                if command is not None:
                    bytes_consumed = len(decoded) - len(rest)
                    print(f"[DEBUG] Directly parsed leftover command: {command}")
                    send_command(master_socket, command, True)
                    buffer = buffer[bytes_consumed:]
                else:
                    print("[DEBUG] Incomplete leftover command, waiting for more data.")
                    break
            except Exception as e:
                print(f"[DEBUG] Failed to parse leftover buffer: {e}")
                break
        print('[DEBUG] connection to master node is established, start handling client connections')
        threading.Thread(
            target=handle_client, args=(master_socket, True, b""), daemon=True
        ).start()
    if args.dir and args.dbfilename:
        read_keys_from_rdb_file()
    while True:
        conn, addr = server_socket.accept()
        print(f"[DEBUG] Accepted connection from {addr}")
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    main()
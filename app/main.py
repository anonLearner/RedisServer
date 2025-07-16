import threading

import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
    while True:
        conn, addr = server_socket.accept()  # wait for client
        print(f"Accepted connection from {addr}")
        # Handle each connection in a new thread

        def handle_client(client_conn):
            while True:
                data = client_conn.recv(1024)
                if not data:
                    break
                client_conn.sendall(b"+PONG\r\n")
            client_conn.close()

        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    main()

import socket
import sys
import json
import select
import time
from threading import Thread, Lock
from socketserver import ThreadingMixIn
import logging

class Tasks:
    def __init__(self):
        self.tasks = []
        self.lock = Lock()

    def append(self, task):
        with self.lock:
            self.tasks.append(task)

    def remove(self, task):
        with self.lock:
            self.tasks.remove(task)

class ThreadAcceptTaskRequests(Thread):
    def __init__(self, host, port):
        Thread.__init__(self)
        self.host = host
        self.port = port

    def run(self):
        # TCP streaming socket creation
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(32)
        sock.settimeout(1)

        while True:
            readable, _, _ = select.select([sock], [], [sock], 0)
            # the socket is not ready for reading
            if not readable:
                time.sleep(1)
                continue

            # accept the client socket (Master process: 5001 port)
            client_sock, _ = sock.accept()
            # strorage for data chunks of size 1024 bytes
            task_data_chunks = []
            # read all the chunks
            while True:
                try:
                    data = client_sock.recv(1024)
                except socket.timeout:
                    continue
                # if client closes no data available indicates the end of the message
                if not data:
                    break
                # collect the chunk
                task_data_chunks.append(data)
            # close the client socket (connection)
            client_sock.close()

            # join the chunks, convert to utf-8 (ASCII) format, convert to json format
            try:
                task = json.loads(b''.join(task_data_chunks).decode('utf-8'))
            except json.JSONDecodeError:
                pass

            task['arrival_time'] = time.time()
            print(task)
            tasks.append(task)

class ThreadSimulatesTaskExecution(Thread):
    def __init__(self):
        Thread.__init__(self)

    def send_update(self, task_update):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # connect to the master server port 5001 (TCP)
            s.connect(("localhost", 5001))
            #send update message
            s.send(json.dumps(task_update).encode())

    def run(self):
        while True:
            # Tasks exists
            if tasks:
                for task in tasks.tasks:
                    # decrease the remaining duration of each task by 1
                    task['duration'] = task['duration'] - 1
                    # remove tasks with remaining duration equal to zero
                    if task['duration'] == 0:
                        tasks.remove(task)
                        task['end_time'] = time.time()
                        print(task)
                        logging.info(task['task_id'] + ':' + str(task['end_time'] - task['arrival_time']))
                        self.send_update({'worker_id':worker_id, 'task_id':task['task_id'], 'type':task['type']})
            # sleep for 1 second
            time.sleep(1)

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print('Usage: python worker.py <port> <worker_id>')
        exit()

    # read port and worker_id command-line arguments
    port, worker_id = int(sys.argv[1]), int(sys.argv[2])

    # Task Pool
    tasks = Tasks()

    # logging
    logging.basicConfig(filename='worker_' + str(worker_id) + '.log', filemode='w', level=logging.INFO)

    st = ThreadSimulatesTaskExecution()
    att = ThreadAcceptTaskRequests('localhost', port)

    st.start()
    att.start()

    st.join()
    att.join()
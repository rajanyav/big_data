import socket
import sys
import json
import select
import random
import time
from threading import Thread, Lock
from socketserver import ThreadingMixIn
import logging

class WIRR:
    def __init__(self, size):
        self.index = -1
        self.size = size

    def next(self):
        self.index = (self.index + 1) % self.size
        return self.index

class Slots:
    def __init__(self, slots):
        self.slots = slots
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.slots = self.slots + 1

    def decrement(self):
        with self.lock:
            self.slots = self.slots - 1

    def isZero(self):
        with self.lock:
            return self.slots == 0

class Jobs:
    def __init__(self):
        self.jobs = []
        self.lock = Lock()

    def append(self, job):
        with self.lock:
            self.jobs.append(job)

    def remove(self, job):
        with self.lock:
            self.jobs.remove(job)

    def get(self, job_id):
        with self.lock:
            for j in self.jobs:
                if j['job_id'] == job_id:
                    return j

    def remove_mapper_task(self, job_id, task_id):
        with self.lock:
            ji, ti = 0, 0 # job_id , task_id
            for i in range(len(self.jobs)):    #traversing job array
                if self.jobs[i]['job_id'] == job_id:
                    ji = i
                    break
                else:
                    i = i + 1
            for j in range(len(self.jobs[ji]['map_tasks'])):
                if self.jobs[ji]['map_tasks'][j]['task_id'] == task_id:
                    ti = j
                    break
                else:
                    j = j + 1

            del self.jobs[ji]['map_tasks'][ti] #delete obj

    def remove_reducer_task(self, job_id, task_id):
        with self.lock:
            ji, ti = 0, 0
            for i in range(len(self.jobs)):
                if self.jobs[i]['job_id'] == job_id:
                    ji = i
                    break
                else:
                    i = i + 1
            for j in range(len(self.jobs[ji]['reduce_tasks'])):
                if self.jobs[ji]['reduce_tasks'][j]['task_id'] == task_id:
                    ti = j
                    break
                else:
                    j = j + 1

            del self.jobs[ji]['reduce_tasks'][ti]

    def get_reducer_tasks(self, job_id):
        with self.lock:
            for j in self.jobs:
                if j['job_id'] == job_id:
                    return j['reduce_tasks']

class ThreadAcceptJobRequests(Thread):
    def __init__(self, host, port):
        Thread.__init__(self)
        self.host = host
        self.port = port

    def run(self):
        # TCP streaming socket creation
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port)) # assigns ip and port number to socket instances
        sock.listen(32)
        sock.settimeout(1)

        while True:
            readable, _, _ = select.select([sock], [], [sock], 0)
            # the socket is not ready for reading
            if not readable:
                time.sleep(1)
                continue

            # accept the client socket (Requests process)
            client_sock, _ = sock.accept()
            # storage for data chunks of size 1024 bytes
            job_data_chunks = []
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
                job_data_chunks.append(data)
            # close the client socket (connection)
            client_sock.close()

            # join the chunks, convert to utf-8 (ASCII) format, convert to json format
            try:
                job = json.loads(b''.join(job_data_chunks).decode('utf-8'))
            except json.JSONDecodeError:
                pass

            job['arrival_time'] = time.time()
            print(job)
            jobs.append(job)

class ThreadUpdateTaskCompletion(Thread):
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

            print(task)

            for worker in workers:
                if worker['worker_id'] == task['worker_id']:
                    worker['slots'] = worker['slots'] + 1
                    slots.increment()

            job_id = task['task_id'][:task['task_id'].index('_')]
            if task['type'] == 'M':
                jobs.remove_mapper_task(job_id, task['task_id'])
            else:
                jobs.remove_reducer_task(job_id, task['task_id'])
                if not jobs.get_reducer_tasks(job_id):
                    job = jobs.get(job_id)
                    jobs.remove(job)
                    job['end_time'] = time.time()
                    logging.info(str(job_id) + ':' + str(job['end_time'] - job['arrival_time']))

class ThreadScheduleTasks(Thread):
    def __init__(self):
        Thread.__init__(self)

    # return free worker index
    def get_free_worker(self):
        if sch_alg == 'RR':
            while True:
                ci = wi_RR.next()
                if workers[ci]['slots'] > 0:
                    break
            return ci
        elif sch_alg == 'LL':
            while True:
                index, slots = -1, 0

                for i in range(len(workers)):
                    if workers[i]['slots'] > slots:
                        index = i
                        slots = workers[i]['slots']

                if index == -1:
                    time.sleep(1)
                else:
                    return index
        # RANDOM Scheduling Algorithm
        else:
            while True:
                index = random.randint(0, len(workers)-1)
                if workers[index]['slots'] > 0:
                    return index

    def send_task_request(self, worker_port, task):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', worker_port))
            #send task
            s.send(json.dumps(task).encode()) #convert phyton objs to json string
            #returns encoded versionnf the given string

    def run(self):
        while True:
            for job in jobs.jobs:
                # schedule map tasks first
                for task in job['map_tasks']:
                    if slots.isZero():
                        break
                    if task['duration'] != 0:
                        wi = self.get_free_worker()
                        task['type'] = 'M'
                        self.send_task_request(workers[wi]['port'], task)
                        task['duration'] = 0
                        workers[wi]['slots'] = workers[wi]['slots'] - 1
                        slots.decrement()
                if not job['map_tasks']:
                    # all map tasks completed schedule reduce tasks
                    for task in job['reduce_tasks']:
                        if slots.isZero():
                            break
                        if task['duration'] != 0:
                            wi = self.get_free_worker()
                            task['type'] = 'R'
                            self.send_task_request(workers[wi]['port'], task)
                            task['duration'] = 0
                            workers[wi]['slots'] = workers[wi]['slots'] - 1
                            slots.decrement()

                if num_slots == 0:
                    break
            time.sleep(1)

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print('Usage: python master.py <path_to_config_json_file> <scheduling_algorithm_one_out_of_RANDOM_RR_and_LL>')
        exit()

    # get the configuration json file path
    conf_path = sys.argv[1]
    print('loading workers configuration from ', conf_path)
    with open(conf_path) as f:
        workers = json.load(f)['workers']

    # initialize num_slots
    num_slots = 0
    for index in range(len(workers)):
        num_slots += workers[index]['slots']
    slots = Slots(num_slots)

    # get the scheduling algorithm
    sch_alg = sys.argv[2]
    if sch_alg not in ['RR','LL','RANDOM']:
        print('scheduling algorith should be one out of RR, LL and RANDOM')
        exit()

    # RR Algorithm current worker index
    wi_RR = WIRR(len(workers))

    # job pool
    jobs = Jobs()

    # logging
    logging.basicConfig(filename='master.log', filemode='w', level=logging.INFO)

    st = ThreadScheduleTasks()
    ut = ThreadUpdateTaskCompletion('localhost', 5001)
    at = ThreadAcceptJobRequests('localhost', 5000)

    st.start()
    ut.start()
    at.start()

    st.join()
    ut.join()
    at.join()

#he join() method takes all items in an iterable and joins them into one string.

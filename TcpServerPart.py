# Master
import random
import math
import os
import select
import socket as so
import time
import json
from threading import Thread, Lock
from slave import MySocket, handle_receive, handle_send, ACK_COMMAND, ACK_TYPE
from functools import partial
from eas import sio
# import asyncio

clients = {}
clients_lock = Lock()
master_addr = ('localhost', 32080)
active_job = None

def remove_client(clients, client):
	print(f"{client.addr} disconnected")
	clients_lock.acquire()
	del clients[client.addr]
	clients_lock.release()
	sio.emit('lost worker', client.describe())

class Task:
	def __init__(self, id, start=0, length=1024):
		self.id = id
		self.input_file = ''
		self.implementation_file = ''
		self.file_start = start
		self.file_length = length
		self.assigned = ''
		self.output_file = ''
		self.stage = 'map'

	def describe(self):
		return {
			"input_file": self.input_file,
			"implementation_file": self.implementation_file,
			"file_start": self.file_start,
			"file_length": self.file_length,
			"output_file": self.output_file,
			"stage": self.stage
		}

	def getname(self):
		return self.input_file + "_" + str(self.id)

class Job:
	CHUNK_SIZE = 14096
	MAP = 0
	SHUFFLE = 1
	REDUCE = 2
	COMPLETED = 3
	OUT_DIR = 'output/'
	def __init__(self):
		# Map part first
		self.id = '-1'
		self.input_file = 'data.txt'
		self.implementation_file = 'map_functor'
		self.n_parts = 0
		self.parts = []
		self.assigned = []
		self.done = []
		self.stage = Job.MAP

		self.genId()
		self.prepareFile()
	
	def genId(self):
		self.id = str(int(time.time())) + str(random.randint(100, 999)) + self.implementation_file
		return self.id

	def prepareFile(self):
		# Creating folder for job
		try:
			os.mkdir(Job.OUT_DIR + self.id)
		except:
			pass

		if self.stage == Job.MAP:
			file_size = os.stat(self.input_file).st_size
			self.n_parts = math.ceil(file_size / Job.CHUNK_SIZE)
			self.parts = []

			for i in range(self.n_parts):
				task = Task(i, i*Job.CHUNK_SIZE, Job.CHUNK_SIZE)
				task.input_file = self.input_file
				task.implementation_file = self.implementation_file
				task.output_file = Job.OUT_DIR + self.id + '/' + task.getname()
				self.parts.append(task)
		elif self.stage == Job.SHUFFLE:
			mapped_keys = self.shuffle()
			try:
				os.mkdir(Job.OUT_DIR + self.id + "/reduce")
			except:
				pass

			# Prepare reduce tasks
			self.done = []
			self.parts = []
			for i, mk in enumerate(mapped_keys):
				task = Task(i, 0, -1)
				task.input_file = Job.OUT_DIR + self.id + "/shuffle/key" + str(i) + ".txt"
				task.implementation_file = self.implementation_file
				task.stage = "reduce"
				task.output_file = Job.OUT_DIR + self.id + "/reduce/key" + str(i) + ".txt"
				self.parts.append(task)

			self.stage = Job.REDUCE
			self.n_parts = len(self.parts)

	def shuffle(self):
		complete_map = {}
		for com_task in self.done:
			with open(com_task.output_file, "r") as f:
				data = json.load(f)
				for k, v in data.items():
					if k not in complete_map:
						complete_map[k] = v
					else:
						complete_map[k] += v
		
		i = 0
		try:
			os.mkdir(Job.OUT_DIR + self.id + "/shuffle")
		except:
			pass

		for k, v in complete_map.items():
			with open(Job.OUT_DIR + self.id + "/shuffle/key" + str(i) + ".txt", "w") as f:
				json.dump({
					"key": k,
					"values": v
				}, f)
			i += 1

		return complete_map.keys()

	def combine_results(self):
		complete_map = {}
		for com_task in self.done:
			with open(com_task.output_file, "r") as f:
				data = json.load(f)
				for k, v in data.items():
					if k not in complete_map:
						complete_map[k] = v
					else:
						complete_map[k] += v
		
		with open(Job.OUT_DIR + self.id + "/" + self.input_file + "_reduced.txt", "w") as f:
			json.dump(complete_map, f)

	def finished(self):
		return len(self.done) == self.n_parts

	def tasksAvailable(self):
		return len(self.parts) > 0

	def getFreePart(self):
		if len(self.parts) > 0:
			ret = self.parts[0]
			# self.assigned.append(ret)
			self.parts = self.parts[1:]
			return ret
		return None

	def assignTask(self, task):
		self.assigned.append(task)
		print("TASK ASSIGNED")

	def releaseTask(self, task):
		# Client disconnected and haven't completed the task.
		# if len(self.assigned) > 0:
			# task = next(i for i in self.assigned if i.assigned == client)
			# task.assigned = ''
			# Put task back into available list
		self.parts.append(task)
		self.assigned.remove(task)

	def taskDone(self, task):
		# task = next(i for i in self.assigned if i[1] == client)
		self.done.append(task)
		self.assigned.remove(task)

		if len(self.done) == self.n_parts and len(self.parts) == 0:
			if self.stage == Job.MAP:
				self.stage = Job.SHUFFLE
				self.prepareFile()
			else:
				self.combine_results() # make this optional
				self.stage = Job.COMPLETED
				with open(Job.OUT_DIR + self.id + "/finished.txt", 'w') as f:
					# f.write()
					pass
				# pass # Koniec roboty ツ

	def debug(self):
		return ""
		# return str(self.parts) + ", " + str(self.assigned) + ", " + str(self.done)

	def __str__(self):
		return self.id
	# def 

class SlaveHandler(Thread):
	ROLE_UNKNOWN = 0
	ROLE_WORKER = 1
	ROLE_CLIENT = 2
	PingInterval = 3000
	def __init__(self, addr, mysocket, remover):
		Thread.__init__(self)
		self.addr = addr
		self.s = mysocket
		self.remover = remover
		self.status = 'Initializing'
		self.directory = ''
		self.task = None
		self.send_queue = []
		self.last_ping = SlaveHandler.PingInterval + 1

	def read(self):
		global active_job
		try:
			rec = handle_receive(self.s)
		except:
			self.status = 'ForRemove'
			print("Lost connection to client")
			return

		message = json.loads(rec)
		command = message.get('command')
		if command == 'initial info':
			self.directory = message.get('directory')
			self.status = 'Ready'
		elif command == 'error':
			if message['type'] == 'assign to busy attempt':
				self.status = 'Busy'

		elif command == 'pong':
			pass

		elif command == 'finish':
			active_job.taskDone(self.task)
			self.task = None
			self.status = 'Ready'
			print("-- TASK FINISHED --")
			print(active_job.debug())
			# --
			# Should assign new tasks but not in this function
			# --

		elif command == 'end':
			self.status = 'ForRemove'

		print('Received', rec)
		
		sio.emit('msg worker', {'address': self.addr, 'message': message})
		# self.send_queue.append("ok")
	
	def send(self):
		for i in self.send_queue:
			handle_send(self.s, i)
		self.send_queue.clear()
	
	def ping(self):
		cur = int(time.time() * 1000) # miliseconds
		if (cur - self.last_ping) > SlaveHandler.PingInterval:
			self.last_ping = cur
			self.send_queue.append(json.dumps({
				'command': 'ping',
				'time': cur
			}))

	def sendJobRequest(self, task):
		task.assigned = self.addr
		self.task = task
		self.send_queue.append(json.dumps({
			'command': 'task',
			'task_params': self.task.describe()
		}))

		self.status = 'Busy'

	def run(self):
		pass
		# try:
		# 	while True:
		# 		rec = self.s.receive() # recv with timeout and then ping for status
		# 		print('Received', rec)
		# 		if rec == 'end':
		# 			break
				
		# 		sio.emit('msg worker', {'address': self.addr, 'message': rec})
		# 		self.s.send("ok")
		# 	self.s.close()
		# except:
		# 	print("client disconnected chyba")
		# self.remover(self)

	def describe(self):
		return {
			'address': self.addr,
			'status': self.status
		}

# Give react gui user's quick rundown about current state.
def rundown():
	return {
		'master_address': f'{master_addr[0]}:{master_addr[1]}',
		'clients': [c.describe() for k, c in clients.items()]
	}

def execute_job(implementation_file, data_file):
	print("Job execed")
	global active_job
	if active_job == None:
		# Correctness of implementation will be checked in worker
		active_job = Job()
		active_job.input_file = data_file
		active_job.implementation_file = implementation_file
		assign_tasks()
		return {'id': active_job.id, 'path': Job.OUT_DIR + active_job.id} # id of the job or something
	else:
		return {'error': 'server is busy with other work, try again later'}

	# if there are no clients available, wait for them 
	# signal all ready clients to exec job

	# later if new clients will connect and there are jobs remaining, assign jobs to clients

def check_job_status(id):
	global active_job
	if active_job != None and active_job.id == id:
		return {'id': id, 'status': ["Map", "Shuffle", "Reduce", "Finished"][active_job.stage], 'path': Job.OUT_DIR + active_job.id}
	else:
		try:
			with open(Job.OUT_DIR + id + "/finished.txt"):
				pass
			return {'id': id, 'status': 'Finished', 'path': Job.OUT_DIR + id}
		except:
			return {'error': f'Cant find job with id {id}'}

def assign_tasks():
	global active_job
	jobwas = False
	while active_job is not None and not active_job.finished() and active_job.tasksAvailable():
		jobwas = True
		c = get_free_client()
		if c is not None:
			task = active_job.getFreePart()
			c.sendJobRequest(task)
			active_job.assignTask(task)
		else:
			break
	if jobwas:
		print("-- TASKS ASSIGNED --")
		print(active_job.debug())

def get_free_client():
	ret = None
	clients_lock.acquire()
	for k, c in clients.items():
		if c.status == 'Ready':
			ret = c
			break
	clients_lock.release()
	return ret

def deleteClient(client):
	sio.emit('lost worker', client.describe())
	global active_job
	if active_job is not None and client.task is not None:
		active_job.releaseTask(client.task)
		print("-- TASKS RELEASED --")
		print(active_job.debug())
	del clients[client.addr]

# Thread
def sockets():
	server = so.socket(so.AF_INET, so.SOCK_STREAM)
	server.setsockopt(so.SOL_SOCKET, so.SO_REUSEADDR, 1)
	server.setblocking(False)
	server.bind(('localhost', 32080))
	print(f'listening on {0}')
	server.listen()

	remove_client_bound = partial(remove_client, clients)

	global active_job
	# active_job = Job()

	while True:
		pot_rec = [i.s for k, i in clients.items()] + [server]
		pot_send = [i.s for k, i in clients.items() if len(i.send_queue) > 0]
		recvable, sendable, errable = select.select(pot_rec, pot_send, pot_rec, 3)

		for e in errable:
			print('error', e.getpeername())
		
		for r in recvable:
			if r == server:
				client_socket, address = server.accept()
				str_address = str(address)
				print(f'Client {str_address} connected')
				handler = SlaveHandler(str_address, client_socket, remove_client_bound)
				clients[str_address] = handler
				sio.emit('new worker', handler.describe())
			else:
				str_addr = str(r.getpeername())
				handler = clients.get(str_addr)
				if handler is not None:
					handler.read()
					if handler.status == 'ForRemove':
						r.close()
						deleteClient(handler)
				else:
					print(f"No handler for receiving socket {r.getpeername()}")

		for s in sendable:
			str_addr = str(s.getpeername())
			handler = clients.get(str_addr)
			if handler is not None:
				handler.send()
				if handler.status == 'ForRemove':
					s.close()
					deleteClient(handler)
					# sio.emit('lost worker', handler.describe())
			else:
				print(f"No handler for sending socket {s.getpeername()}")

		if active_job is not None and active_job.stage == Job.COMPLETED:
			active_job = None

		for k, c in clients.items():
			c.ping()
			if c.status == 'Ready':
				assign_tasks()

		# print("recv end")
		# handler.start()

		
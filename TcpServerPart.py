# Master
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

class Job:
	def __init__(self):
		self.name = 'MapReduceTest'
		self._parts_definition = list(range(12))
		self.parts = list(range(12))
		self.assigned = []
		self.done = []

	def finished(self):
		return len(self.done) == 12

	def tasksAvailable(self):
		return len(self.parts) > 0

	def getFreePart(self):
		if len(self.parts) > 0:
			ret = self.parts[0]
			# self.assigned.append(ret)
			self.parts = self.parts[1:]
			return ret
		return None

	def assignTask(self, partn, client):
		self.assigned.append((partn, client))
		print("TASK ASSIGNED")

	def releaseTask(self, client):
		# Client disconnected and haven't completed the task.
		if len(self.assigned) > 0:
			task = next(i for i in self.assigned if i[1] == client)
			# Put task back into available list
			self.parts.append(task[0])
			self.assigned.remove(task)

	def taskDone(self, client):
		task = next(i for i in self.assigned if i[1] == client)
		self.done.append(task[0])
		self.assigned.remove(task)

	def debug(self):
		return str(self.parts) + ", " + str(self.assigned) + ", " + str(self.done)

	def __str__(self):
		return self.name
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
		self.status = 'Ready'
		self.job = None
		self.job_role = ''
		self.progress = 0.0
		self.type = SlaveHandler.ROLE_UNKNOWN
		self.send_queue = []
		self.last_ping = SlaveHandler.PingInterval + 1
		self.latency = 0

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
		if command == str(ACK_TYPE):
			if message['data'] == 'part number':
				self.status = 'Busy'
				active_job.assignTask(self.job_role, self.addr)
				
				print("-- TASK ACK --")
				print(active_job.debug())
				# emit to front and send exec

		elif command == 'error':
			if message['type'] == 'assign to busy attempt':
				self.status = 'Busy'

		elif command == 'pong':
			self.latency = message['time'] - self.last_ping
			self.progress = message['progress']

		elif command == 'finish':
			active_job.taskDone(self.addr)
			self.job_role = None
			self.progress = 0
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

	def sendJobRequest(self, active_job, part):
		self.job = active_job
		self.job_role = part
		self.send_queue.append(json.dumps({
			'command': 'part number',
			'data': part
		}))

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

def execute_job():
	print("Job execed")
	global active_job
	if active_job == None:
		active_job = Job()
		assign_tasks()
	# if there are no clients available, wait for them 
	# signal all ready clients to exec job

	# later if new clients will connect and there are jobs remaining, assign jobs to clients

def assign_tasks():
	global active_job
	jobwas = False
	while active_job is not None and not active_job.finished() and active_job.tasksAvailable():
		jobwas = True
		c = get_free_client()
		if c is not None:
			part = active_job.getFreePart()
			c.status = 'Pending'
			c.sendJobRequest(active_job, part)
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
	if active_job is not None:
		active_job.releaseTask(client.addr)
	del clients[client.addr]
	if active_job is not None:
		print("-- TASKS RELEASED --")
		print(active_job.debug())

# Thread
def sockets():
	server = so.socket(so.AF_INET, so.SOCK_STREAM)
	server.setsockopt(so.SOL_SOCKET, so.SO_REUSEADDR, 1)
	server.setblocking(False)
	server.bind(('localhost', 32080))
	print(f'listening on {0}')
	server.listen()

	remove_client_bound = partial(remove_client, clients)

	# global active_job
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
				print(str(clients.keys()))
				sio.emit('new worker', handler.describe())
			else:
				str_addr = str(r.getpeername())
				handler = clients.get(str_addr)
				if handler is not None:
					handler.read()
					if handler.status == 'ForRemove':
						r.close()
						deleteClient(handler)
						# sio.emit('lost worker', handler.describe())
						# del clients[str_addr]
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

		for k, c in clients.items():
			c.ping()
			if c.status == 'Ready':
				assign_tasks()

		# print("recv end")
		# handler.start()

		
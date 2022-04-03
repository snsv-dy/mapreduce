# Master
import socket as so
from slave import MySocket
from threading import Thread
from functools import partial
from eas import sio
# import asyncio

clients = {}
master_addr = ('localhost', 32080)

def remove_client(clients, client):
	print(f"{client.addr} disconnected")
	sio.emit('lost worker', client.describe())
	del clients[client.addr]

class SlaveHandler(Thread):
	def __init__(self, addr, mysocket, remover):
		Thread.__init__(self)
		self.addr = addr
		self.s = mysocket
		self.remover = remover
		self.status = 'Ready'

	def run(self):
		try:
			while True:
				rec = self.s.receive() # recv with timeout and then ping for status
				print('Received', rec)
				if rec == 'end':
					break
				
				sio.emit('msg worker', {'address': self.addr, 'message': rec})
				self.s.send("ok")
			self.s.close()
		except:
			print("client disconnected chyba")
		self.remover(self)

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

def sockets():
	server = so.socket(so.AF_INET, so.SOCK_STREAM)
	server.setsockopt(so.SOL_SOCKET, so.SO_REUSEADDR, 1)
	server.bind(('localhost', 32080))
	print(f'listening on {0}')
	server.listen()

	# clients = {}
	remove_client_bound = partial(remove_client, clients)

	while True:
		client_socket, address = server.accept()
		str_address = str(address)
		print(f'Client {str_address} connected')
		s = MySocket(client_socket)
		handler = SlaveHandler(str_address, s, remove_client_bound)
		clients[str_address] = handler
		print(str(clients.keys()))
		sio.emit('new worker', handler.describe())

		handler.start()

		
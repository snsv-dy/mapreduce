# Not master
import time
import socket as so
import struct
import json
from slaveExecutor import Executor

MSGLEN = 5

def handle_receive(socket):
	chunks = []
	bytes_recd = 0
	
	chunk = socket.recv(4)
	if chunk == b'':
		raise RuntimeError('Socket connection broken')
	msglen, = struct.unpack('!I', chunk)
	# print("Message length", msglen)
	
	while bytes_recd < msglen:
		chunk = socket.recv(min(msglen - bytes_recd, 2048))
		if chunk == b'':
			raise RuntimeError('Socket connection broken')
		chunks.append(chunk)
		bytes_recd += len(chunk)

	return b''.join(chunks).decode("utf-8")

def handle_send(socket, smsg):
	msg = smsg.encode("utf-8")
	msglen = len(msg)
	bytes_len = struct.pack("!I", msglen)
	# print(f"msglen {msglen}")
	# print(f"bytes len {bytes_len}")
	
	sent = socket.send(bytes_len)
	if sent == 0:
		raise RuntimeError('Socket connection broken')
	totalsent = 0
	while totalsent < msglen:
		sent = socket.send(msg[totalsent:])
		if sent == 0:
			raise RuntimeError('Socket connection broken')
		totalsent += sent

class MySocket:
	def __init__(self, socket):
		self.socket = socket

	def receive(self):
		chunks = []
		bytes_recd = 0
		
		chunk = self.socket.recv(4)
		if chunk == b'':
			raise RuntimeError('Socket connection broken')
		msglen, = struct.unpack('!I', chunk)
		# print("Message length", msglen)

		while bytes_recd < msglen:
			chunk = self.socket.recv(min(msglen - bytes_recd, 2048))
			if chunk == b'':
				raise RuntimeError('Socket connection broken')
			chunks.append(chunk)
			bytes_recd += len(chunk)

		return b''.join(chunks).decode("utf-8")

	def send(self, smsg):
		msg = smsg.encode("utf-8")
		msglen = len(msg)
		bytes_len = struct.pack("!I", msglen)
		# print(f"msglen {msglen}")
		# print(f"bytes len {bytes_len}")
		
		sent = self.socket.send(bytes_len)
		if sent == 0:
			raise RuntimeError('Socket connection broken')
		totalsent = 0
		while totalsent < msglen:
			sent = self.socket.send(msg[totalsent:])
			if sent == 0:
				raise RuntimeError('Socket connection broken')
			totalsent += sent

	def close(self):
		
		self.socket.close()

ACK_COMMAND = 0
ACK_TYPE = 0

if __name__ == '__main__':
	socket = so.socket(so.AF_INET, so.SOCK_STREAM)
	socket.connect(('localhost', 32080))
	s = MySocket(socket)
	# Wysyłanie informacji o folderze
	s.send(json.dumps({
		'command': 'initial info',
		'directory': 'output'
	}))
	worker_thread = None
	while True:
		# inn = input()
		# s.send(inn)
		# if inn == 'end':
		# 	break
		# print('got ', s.receive())
		pack = json.loads(s.receive())
		command = pack.get('command')
		if command == 'task':
			if worker_thread == None:
				params = pack.get('task_params')
			# 	"input_file": 'data.txt',
			# "file_start": self.file_start,
			# "file_length": self.file_length,
			# "output_file": self.output_file
				worker_thread = Executor(params.get('input_file'), 
					params.get('implementation_file'),
					params.get('output_file'), 
					params.get('file_start'), 
					params.get('file_length'),
					params.get('stage'))
				worker_thread.start()
				worker_thread.join()

				s.send(json.dumps({
					'command': 'finish'
					}))
				worker_thread = None
				# part_number = pack.get('data')
				# # Start work here immediately
				# s.send(json.dumps({
				# 	'command': str(ACK_TYPE),
				# 	'data': 'part number'
				# 	}))
			else:
				s.send(json.dumps({
					'command': 'error',
					'type': 'assign to busy attempt',
					'data': 'Worker has already assigned work.'
					}))
			print(f'task:')
		# elif command == 'execute':
		# 	pass
		elif command == 'ping':
			t = int(time.time() * 1000)
			s.send(json.dumps({'command': 'pong', 'time': t})) 
			# if part_number is not None:
			# 	dummy_progress += 50.0
			# t = int(time.time() * 1000)
			# if dummy_progress == 100.0:
			# 	# dummy finish work
			# 	s.send(json.dumps({'command': 'finish', 'data': 'part' + str(part_number) + '.txt'}))
			# 	dummy_progress = 0.0
			# 	part_number = None
			# 	worker_thread = None
		elif command == 'die':
			# Make that client tries to reconnect after connection was lost.
			break

	socket.close()
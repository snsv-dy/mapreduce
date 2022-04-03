# Not master

import socket as so
import struct

MSGLEN = 5

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

if __name__ == '__main__':
	socket = so.socket(so.AF_INET, so.SOCK_STREAM)
	socket.connect(('localhost', 32080))
	s = MySocket(socket)
	print("Write end to close connection: ")
	while True:
		inn = input()
		s.send(inn)
		if inn == 'end':
			break
	socket.close()
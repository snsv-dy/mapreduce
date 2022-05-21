from threading import Thread
from time import sleep

class Executor(Thread):
	STATE_IDLE = 0
	STATE_WORKING = 1
	STATE_FINISHED = 2

	def __init__(self):
		self.state = Executor.STATE_IDLE

	def run(self):
		self.state = Executor.STATE_WORKING
		sleep(1.5)
		self.state = Executor.STATE_FINISHED
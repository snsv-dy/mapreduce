import json
from threading import Thread
from time import sleep
import importlib

def attach_emit(base):
	class Ab(base):
		def __init__(self):
			super().__init__()
			self.map_result = {}

		def emit(self, k, v):
			if k in self.map_result:
				self.map_result[k].append(v)
			else:
				self.map_result[k] = [v]

	print('emit attached')
	return Ab

class Executor(Thread):
	STATE_IDLE = 0
	STATE_WORKING = 1
	STATE_FINISHED = 2

	def __init__(self, input_file, implementation_file, output_file, start, length, stage):
		super().__init__()
		self.input_file = input_file
		self.implementation_file = implementation_file
		self.file_start = start
		self.file_length = length
		self.output_file = output_file
		self.stage = stage

		self.state = Executor.STATE_IDLE
		print("Executor init: ")
		print("input_file", self.input_file)
		print("implementation", self.implementation_file)
		print("start", self.file_start)
		print("length", self.file_length)
		print("output_file", self.output_file)
		print("stage", self.stage)

		# The map in question
		# self.map_result = {}

	def run(self):
		self.state = Executor.STATE_WORKING
		# sleep(1.5)

		with open(self.input_file, "r") as f:
			f.seek(self.file_start)
			string = f.read(self.file_length) if self.file_length != -1 else f.read()
			# print("Opened file")
			# print(string)
			impl = importlib.import_module(self.implementation_file)
			impl = importlib.reload(impl)
			impl = getattr(impl, dir(impl)[0]) # Pierwszy alfabetycznie

			impl = importlib.import_module(self.implementation_file)
			impl = getattr(impl, dir(impl)[0]) # Pierwszy alfabetycznie obiekt.
			impl = attach_emit(impl)

			impl_obj = impl()

			if self.stage == "map":
				impl_obj.map_(self.input_file, string)
			else:
				jdata = json.loads(string)
				impl_obj.reduce_(jdata['key'], jdata['values'])

			with open(self.output_file, "w") as of:
				json.dump(impl_obj.map_result, of)

		self.state = Executor.STATE_FINISHED

	
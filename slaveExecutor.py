import json
from threading import Thread
from time import sleep

class Executor(Thread):
	STATE_IDLE = 0
	STATE_WORKING = 1
	STATE_FINISHED = 2

	def __init__(self, input_file, output_file, start, length, stage):
		super().__init__()
		self.input_file = input_file
		self.file_start = start
		self.file_length = length
		self.output_file = output_file
		self.stage = stage

		self.state = Executor.STATE_IDLE
		print("Executor init: ")
		print("input_file", self.input_file)
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

			map_result = {}
			def emit(k, v):
				if k in map_result:
					map_result[k].append(v)
				else:
					map_result[k] = [v]

			def map(k, v):
				import re
				for i in re.split('\s', v):
					emit(i, "1")

			def reduce(key, values):
				emit(key, len(values))

			if self.stage == "map":
				map(self.input_file, string)
			else:
				jdata = json.loads(string)
				reduce(jdata['key'], jdata['values'])

			with open(self.output_file, "w") as of:
				json.dump(map_result, of)

		self.state = Executor.STATE_FINISHED

	
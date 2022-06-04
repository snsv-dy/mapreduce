import re
import time


class MapReduce:

	def emit(self, k, v):
		pass

	def map_(self, k, v):
		pass
		# self.emit("abc", "A")

	def reduce_(self, k, v):
		pass
		# self.emit(k, v[0])

class ACountWords(MapReduce):
	def map_(self, k, v):
		import re
		for i in re.split('\s', v):
			self.emit(i, "1")

	def reduce_(self, key, values):
		self.emit(key, len(values))


class CountWords(MapReduce):
	pass
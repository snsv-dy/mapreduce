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
		# import time
		for i in re.split('\s', v):
			self.emit(i, "1")
		# time.sleep(0.5)

	def reduce_(self, key, values):
		self.emit(key, len(values))
		# time.sleep(0.5)


class CountWords(MapReduce):
	pass
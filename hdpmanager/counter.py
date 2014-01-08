import os
import sys
import collections
import atexit

FLUSH_INTERVAL = 1000
COMMA_REPLACE_CHARACTER = ';'
IS_TERMINAL = os.isatty(sys.stderr.fileno())

counters = collections.defaultdict(int)

def flush_file(counter=None, clear=True):
	if counter is None:
		for counter in counters.iterkeys():
			flush_file(counter)
		return

	if counters[counter] > 0:
		name = counter.replace(',', COMMA_REPLACE_CHARACTER)
		sys.stderr.write('reporter:counter:%s,%s\n' % (name, counters[counter]))
		counters[counter] = 0

def flush_terminal(counter=None, clear=True):
	for counter in counters.iterkeys():
		name = counter.replace(',', COMMA_REPLACE_CHARACTER)
		sys.stderr.write('counter:%s,%s\n' % (name, counters[counter]))
	if clear:
		sys.stderr.write('\033[%dA' % (len(counters) + 1))

def count(counter, amount=1):
	current_value = counters[counter]
	new_value = current_value + amount
	counters[counter] = new_value
	if new_value / FLUSH_INTERVAL > current_value / FLUSH_INTERVAL:
		if IS_TERMINAL:
			flush_terminal(counter, clear=False)
		else:
			flush_file(counter)

if IS_TERMINAL:
	atexit.register(flush_terminal, clear=False)
else:
	atexit.register(flush_file)


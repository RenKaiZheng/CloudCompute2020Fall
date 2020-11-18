#!/usr/bin/python
import sys

currentTime = None
currentCount= 0
time = None

for line in sys.stdin:
	line = line.strip()
	time, count = line.split('\t', 1)
	count = int(count)

	if time == currentTime:
		currentCount += count
	else:
		if currentTime:
			print(currentTime + '\t' + str(currentCount))
		currentCount = count
		currentTime = time

if time == currentTime:
	print(currentTime + '\t' + str(currentCount))
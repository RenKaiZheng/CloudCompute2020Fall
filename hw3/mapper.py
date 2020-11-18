#!/usr/bin/python
import sys

month2digit = {'Jan': '01', 'Feb':'02', 'Mar':'03', 'Apr':'04', 'May':'05', 'Jun':'06', 'Jul':'07', 'Aug':'08', 'Sep':'09', 'Oct':'10', 'Nov':'11', 'Dec':'12'}


for line in sys.stdin:
	line = line.strip()
	elementList = line.split(" ")
	accessTime = elementList[3]
	accessTime = accessTime[1:]
	timeUnits = accessTime.split(':')

	date, month, year = timeUnits[0].split('/')
	outString = year + '-' + month + '-' + date + ' T ' + timeUnits[-3] + ':00:00.000'

	print(outString + '\t' + '1')
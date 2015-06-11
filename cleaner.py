##
# Created by akhld on 11/6/15.
##

# This one trims the first and last chars of lines in the given file.

for line in open("data/csv/part-00000"):
	print line[1:-2]

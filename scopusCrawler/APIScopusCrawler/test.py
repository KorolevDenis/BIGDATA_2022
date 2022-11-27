# Open a file: file
file = open('apis.txt',mode='r')
 
# read all lines at once
# Open a file: file
all_of_it = file.read().replace("\r", "").split("\n")
print(all_of_it)
# close the file
file.close()

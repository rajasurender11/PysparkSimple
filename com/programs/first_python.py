
print("Hi")
name1 = "Surender"
name2 = 'raja'
id = 10
print(name1 + " " + name2 )
print(name2)
print(id+8)
print("Hi")

data = ['surender|CHN' , 'raja|CHN']

for itr in data:
   m = itr.split('|')
   print(m)
   print(name1)

if(id > 50):
    print(f"{id} is greater than 50")
else:
    print(f"{id} is lesser than 50")

print("ended")

import sys

print("User Current Version:-", sys.version)

name_city = 'surender|CHN'
split_arr = name_city.split("|")
print(split_arr[1])



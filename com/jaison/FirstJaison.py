id = 100
print("Hi")
print(id)
id = "200"
print(id + " Hello")
name1 = "Surender"
name2 = 'raja'
id = 10
print(name1 + " " + name2 )
print(name2)
print(id+8)
my_list = ["hadoop","azure"]
for i in my_list:
     print(i)
     print(i)
     print("for loop end")
print("end")
print("*******************")
num = 101

if num+10 >  101:
     print(num)
     print("inside if")
elif num == 102:
     print(num)
     print("inside elif")

else:
     print("not 100")

inputLoc = "c://data//data.csv"

numbers_range = list(range(1,6))
print(numbers_range)
sum = 0
#sum=1
#sum=3
for i in numbers_range:
     print("i" + str(i))
     sum = sum +i #0 +1  1+2
     print(" sum " + str(sum))


for i in numbers_range:

     if i%2 == 0:
          print( str(i) + " is a EVEN number")
     else:
          print( str(i) + " is a ODD number")


my_dict = {"id":1,
           "name":"surender",
           "city" :"chennai",
           "skills":["hadoop","spark"]
           "obj" : [("1")]
           }

print(my_dict)
print(my_dict.keys())
print(my_dict.values())
print(my_dict.items())

for i in my_dict.keys():
     print(i)
     print(my_dict[i])


my_dict["org"] = "CTS"
print(my_dict)
print(my_dict.get("org"))

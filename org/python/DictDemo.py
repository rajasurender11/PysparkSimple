
def dict_demo():
    my_dict = {"id":1,
               "name":"surender",
               "city" :"chennai",
               "skills":["hadoop","spark"]
               }

    for i,j in my_dict.items():
        print(i)
        print(j)

    my_dict["org"] = "CTS"
    print(my_dict)


if __name__ == "__main__":
    dict_demo()
    my_data_list = [1,2,3,1,1,2,3,4,5,6,7]
    my_empty_dict ={}
    for i in my_data_list:
        if i in my_empty_dict:
            my_empty_dict[i] += 1
        else:
            my_empty_dict[i] = 1
    print(my_empty_dict)
    print("*************************")
    for i,j in my_empty_dict.items():
        if j !=1:
            print(i)



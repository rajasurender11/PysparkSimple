

def tuple_demo():
    my_tuple = ("surender","raja")
    for i in my_tuple:
        print(i)

    new_list = list(my_tuple)
    new_list[0] ="ajay"
    my_tuple = tuple(new_list)
    print(my_tuple)









if __name__ == "__main__":
    tuple_demo()
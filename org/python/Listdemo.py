












if __name__ == "__main__":
    my_list = ["surender","raja","ajay"]
    f_name = my_list[0]
    for i in my_list:
        print(i)
    print("***************")
    range_list = list(range(0,3))
    print(range_list)
    for i in range_list:
        print(my_list[i])

    my_list.append("kumar")
    print(my_list)
    my_list.insert(1,"vikram")
    print(my_list)
    my_list.remove("surender")
    print(my_list)
    my_list.pop(3)
    print(my_list)
    my_list[0]= "new_data"
    print(my_list)

    my_data_list = [1,2,3,1,1,2,3,4,5,6,7]
    my_new_list = []

    for i in my_data_list:
        if i not in my_new_list:
            my_new_list.append(i)

    print(my_new_list)

    my_str = "surender"
    my_reverse_str = ""
    for i in my_str:
        my_reverse_str = i+my_reverse_str
    print(my_reverse_str)
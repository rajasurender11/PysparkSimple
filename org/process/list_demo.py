
print(__name__)
def process():
    my_list = ["hadoop","spark","sql"]
    print(my_list)
    print(my_list[0])
    print(my_list[1])
    print(my_list[2])
    print("*********************")
    for elem in my_list:
        print(elem)
    print(my_list)
    my_list.append("azure")
    print(my_list)
    my_list.remove("hadoop")
    print(my_list)
    my_list[0] = "spark_updated"
    print(my_list)
    my_range_list = list(range(4,9))
    print(my_range_list)

if __name__ == "__main__":
    process()
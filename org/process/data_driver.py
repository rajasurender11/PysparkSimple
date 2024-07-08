import json

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

process_name = "DATA_PROCESS"
input_json_loc = "C:/surender/Intellij_projects/python_projects/Python2/config/input.json"


spark = SparkSession \
    .builder \
    .appName("data_process") \
    .getOrCreate()

def read_data(input_loc:str,format_type:str,reading_args:dict)->DataFrame:
    reader = spark.read.format(format_type)
    if format_type == "csv":
        for key,value in reading_args.items():
            reader = reader.option(key,value)
        df = reader.load(input_loc)
        return df
    else:
        df = reader.load(input_loc)
        return df


def read_data_as_dataframe(arr):
    input_dict ={}
    for i in arr:
        if(i["type_of_input"] == "file"):
         input_loc = i["input_loc"]
         format_type = i["format_type"]
         reading_args = i["reading_args"] if "reading_args" in i else {}
         df = read_data(input_loc,format_type,reading_args)
         #df = spark.read.format(i["format_type"]).option("header", "true").load(i["input_loc"])
         input_dict[i["dataframe_name"]] = df
    return  input_dict

def read_config(loc):
 with open(input_json_loc,"r") as read_file:
     data = json.load(read_file)
     return data

def data_process(p_name):
    json_config = read_config(input_json_loc)
    print(json_config[p_name])
    ip_dict= read_data_as_dataframe(json_config[p_name])
    print(ip_dict.keys())
    ip_dict["records_df"].show()
    ip_dict["accounts_df"].show()
    ip_dict["emp_df"].show()


if __name__ == "__main__":
    data_process(process_name)


try:
    print("Hi")
except Exception  as e:
    raise e.with_traceback()
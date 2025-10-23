import json


with open("rooms.json") as json_file:
    data = json.load(json_file)
    counter = 0
    for room in data:
        if counter != 10:
            print(room)
            counter = counter + 1
            continue
        else:
            break
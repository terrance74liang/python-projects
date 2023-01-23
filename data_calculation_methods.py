import json
from pymongo import MongoClient, InsertOne, collection
from datetime import datetime
from matplotlib import pyplot as plt

with open('URLMongo.txt') as f:
    url = f.readline()

myclient = MongoClient(
    url)

mydb = myclient["TestDatabase"]
mycol = mydb["AequitasData"]

# data is the orderID, finds all instances of orderID


def FIND_ORDER(database, data):
    query = {'OrderID': str(data)}
    instances = database.find(query)
    states = list()
    for x in instances:
        states.append(
            [datetime.strptime(x.get('TimeStamp')[x.get('TimeStamp').index(' ')+1:-3], '%H:%M:%S.%f'), x.get('MessageType'), x.get('OrderPrice')])
    states.sort(key=lambda a: a[0])
    return states


# takes a list of time vs price, returns true if end and first order is specific
def ORDER_PASSTHROUGH(data):
    if data[0][1] == 'NewOrderRequest' and data[len(data)-1][1] == 'Trade':
        return True
    else:
        return False

# period selection


def PERIOD_SELECTION(data):
    timeframe = input('select time frame in seconds(max 180 and min 0.0001):')
    aggregated_data = list()
    initial_time = 0
    initial_count = 0
    maximum = 0
    minimum = 0
    open_price = 0
    for x in data:
        if initial_count == 0:
            initial_time = x[0]
            open_price = x[1]
        time_difference = x[0] - initial_time
        initial_count = initial_count + 1
        if x[1] > maximum:
            maximum = x[1]
        if x[1] < minimum:
            minimum = x[1]
        if time_difference > timeframe:
            # open,close,high,low
            aggregated_data.append([open_price, x[2], maximum, minimum])
            initial_time = 0
            initial_count = 0
            maximum = 0
            minimum = 0
    return aggregated_data


all_data = list()
points = list()

for x, y in zip(mycol.distinct('OrderID'), range(0, 201)):
    if y == 200:
        break
    all_data.append(FIND_ORDER(mycol, x))
    # if not ORDER_PASSTHROUGH(all_data[len(all_data)-1]):
    # all_data = all_data[:-1]

print(all_data)
# for x in all_data:
#     points.append([x[0], x[2]])

# print(points)

# print(PERIOD_SELECTION(all_data))

# for i in mycol.distinct('OrderID'):
#     if ORDER_PASSTHROUGH(FIND_ORDER(mycol, i)):
#         all_data.append(i)

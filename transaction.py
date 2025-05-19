#JSON for inserting into MongoDB
#Version 2, all 20000 documents
#2022-12-03 10:51

import json
import pymongo
from pymongo import MongoClient
from pprint import pprint

MONGO_HOST ='172.31.88.146' 
MONGO_PORT = 27017
MONGO_DB = 'db'
connection = MongoClient(MONGO_HOST, MONGO_PORT)
db = connection[MONGO_DB]
transaction = db['transaction']

v = open('vehicle.json')
ri = open('rider.json')
ca = open('customer_address.json')
cc = open('customer_contact.json')
c = open('customer.json')
re = open('restaurant.json')
f = open('fooditems.json')
p = open('totalprice.json')
data_v = json.load(v)
data_ri = json.load(ri)
data_ca = json.load(ca)
data_cc = json.load(cc)
data_c = json.load(c)
data_re = json.load(re)
data_f = json.load(f)
data_p = json.load(p)

t1 = open('transaction01.json')
t2 = open('transaction02.json')
t3 = open('transaction03.json')
t4 = open('transaction04.json')
t5 = open('transaction05.json')
t6 = open('transaction06.json')
t7 = open('transaction07.json')
t8 = open('transaction08.json')
t9 = open('transaction09.json')
t10 = open('transaction10.json')
t11 = open('transaction11.json')
t12 = open('transaction12.json')
t13 = open('transaction13.json')
t14 = open('transaction14.json')
t15 = open('transaction15.json')
t16 = open('transaction16.json')
t17 = open('transaction17.json')
t18 = open('transaction18.json')
t19 = open('transaction19.json')
t20 = open('transaction20.json')
data_t1 = json.load(t1)
data_t2 = json.load(t2)
data_t3 = json.load(t3)
data_t4 = json.load(t4)
data_t5 = json.load(t5)
data_t6 = json.load(t6)
data_t7 = json.load(t7)
data_t8 = json.load(t8)
data_t9 = json.load(t9)
data_t10 = json.load(t10)
data_t11 = json.load(t11)
data_t12 = json.load(t12)
data_t13 = json.load(t13)
data_t14 = json.load(t14)
data_t15 = json.load(t15)
data_t16 = json.load(t16)
data_t17 = json.load(t17)
data_t18 = json.load(t18)
data_t19 = json.load(t19)
data_t20 = json.load(t20)

data_t = []
for i in range(1000): data_t.append(data_t1[i])
for i in range(1000): data_t.append(data_t2[i])
for i in range(1000): data_t.append(data_t3[i])
for i in range(1000): data_t.append(data_t4[i])
for i in range(1000): data_t.append(data_t5[i])
for i in range(1000): data_t.append(data_t6[i])
for i in range(1000): data_t.append(data_t7[i])
for i in range(1000): data_t.append(data_t8[i])
for i in range(1000): data_t.append(data_t9[i])
for i in range(1000): data_t.append(data_t10[i])
for i in range(1000): data_t.append(data_t11[i])
for i in range(1000): data_t.append(data_t12[i])
for i in range(1000): data_t.append(data_t13[i])
for i in range(1000): data_t.append(data_t14[i])
for i in range(1000): data_t.append(data_t15[i])
for i in range(1000): data_t.append(data_t16[i])
for i in range(1000): data_t.append(data_t17[i])
for i in range(1000): data_t.append(data_t18[i])
for i in range(1000): data_t.append(data_t19[i])
for i in range(1000): data_t.append(data_t20[i])

for i in range(100):
    v_id = data_ri[i]['vehicle_id']
    data_ri[i]['vehicle'] = data_v[v_id-1]
for element in data_ri:
    del element['vehicle_id']

for i in range(1000):
    ca_id = data_c[i]['customer_address_id']
    cc_id = data_c[i]['customer_contact_id']
    data_c[i]['customer_address'] = data_ca[ca_id-1]
    data_c[i]['customer_contact'] = data_cc[cc_id-1]
for element in data_c:
    del element['customer_address_id']
    del element['customer_contact_id']
for element in data_ca:
    del element['unit_floor']
    del element['unit_room']

for i in range(20000):
    c_id = data_t[i]['customer_id']
    re_id = data_t[i]['restaurant_id']
    ri_id = data_t[i]['rider_id']
    data_t[i]['customer'] = data_c[c_id-1]
    data_t[i]['restaurant'] = data_re[re_id-1]
    data_t[i]['rider'] = data_ri[ri_id-1]
    data_t[i]['food_items'] = data_f[i]['food_items']
    data_t[i]['total_price'] = data_p[i]['total_price']
for element in data_t:
    del element['customer_id']
    del element['restaurant_id']
    del element['rider_id']

transaction.drop()

for i in range(20000):
    transaction.insert_one(data_t[i])

for t in transaction.find():
  pprint(t)
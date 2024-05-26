from kafka import KafkaConsumer
import pymongo
import json

from tkinter import *
from tkinter import ttk,messagebox

import threading

config = json.load(open("config_consumer.json"))

mango = pymongo.MongoClient(config['mongo_uri'])

db = mango['data']
weather_col = db['weather']
stock_col = db['stock']

lbox = None
lbox_stock = None

def weather_consumer_thread():
    global lbox
    consumer = KafkaConsumer(
        "weather",
        bootstrap_servers=[config['kafka_address']],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id="w_group"
    )
    for msg in consumer:
        print("WEATHER")
        m_val = msg.value
        weather_col.insert_one(m_val)
        lbox.insert(END, f"{m_val['city']}: {m_val['weather']} Temp: {m_val['temp']}°C ({m_val['temp_min']}°C - {m_val['temp_max']}°C)")
        print("Added to weather: ", m_val)

def stock_consumer_thread():
    global lbox_stock
    consumer_stock = KafkaConsumer(
        "stock",
        bootstrap_servers=[config['kafka_address']],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id="s_group"
    )
    for msg in consumer_stock:
        print("STOCK")
        m_val = msg.value
        stock_col.insert_one(m_val)
        lbox_stock.insert(END, f"{m_val['symbol']}: {m_val['value']} {m_val['currency']} ({m_val['exchange']})")
        print("Added to stock: ", m_val)

def tkinter_thread():
    global lbox
    global lbox_stock
    root = Tk()
    root.title("Consumer: Proyecto Final")
    frm = ttk.Frame(root, padding=10)
    frm.grid()
    
    lbox = Listbox(frm, selectmode='browse', width=50)
    lbox.grid(column=0, row=0)

    lbox_stock = Listbox(frm, selectmode='browse', width=50)
    lbox_stock.grid(column=1, row=0)

    root.mainloop()

threading.Thread(target=weather_consumer_thread,daemon=True).start()
threading.Thread(target=stock_consumer_thread,daemon=True).start()
threading.Thread(target=tkinter_thread,daemon=False).start()
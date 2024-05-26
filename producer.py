from kafka import KafkaProducer
import requests
import json
from tkinter import *
from tkinter import ttk,messagebox

config = json.load(open("config_producer.json"))

API_KEY = config['API_KEY_WEATHER']
API2_KEY = config['API_KEY_STOCKS']

city_list=[
    "Chihuahua",
    "Monterrey",
    "Ciudad de Mexico",
    "Zacatecas",
    "Aguascalientes",
    "Merida",
    "Veracruz"
]

stock_symbols = [
    "AAPL",
    "MSFT",
    "AMZN",
    "GOOGL",
    "TSLA",
    "NU",
]

producer = KafkaProducer(
    bootstrap_servers=[config['kafka_address']],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def process_response(resp,city):
    return {
        'city': city,
        'weather': resp['weather'][0]['main'],
        'temp': resp['main']['temp'],
        'temp_min': resp['main']['temp_min'],
        'temp_max': resp['main']['temp_max']
    }

def process_stock_response(resp):
    return {
        'symbol': resp['meta']['symbol'],
        'currency': resp['meta']['currency'],
        'exchange': resp['meta']['exchange'],
        'value': resp['values'][0]['close']
    }

def get_city_data(city, valid_callback=None, invalid_callback=None):
    print(city)
    API_URL = f"https://api.openweathermap.org/data/2.5/weather?q={city}&units=metric&appid={API_KEY}"
    req = requests.get(API_URL)

    if req.status_code != 200:
        if invalid_callback:
            invalid_callback()
        return
    processed_res = process_response(req.json(),city)
    print("before send")
    producer.send('weather', processed_res)
    producer.flush()
    print("after flush")
    
    if valid_callback:
        valid_callback(processed_res)

def get_stock_data(stock, valid_callback=None, invalid_callback=None):
    API_URL = f"https://api.twelvedata.com/time_series?symbol={stock}&interval=1min&apikey={API2_KEY}"
    req = requests.get(API_URL)

    if req.status_code != 200:
        if invalid_callback:
            invalid_callback()
        return
    
    processed_res = process_stock_response(req.json())
    producer.send('stock', processed_res)
    producer.flush()

    if valid_callback:
        valid_callback(processed_res)

def main():
    root = Tk()
    root.title("Producer: Proyecto Final")
    frm = ttk.Frame(root, padding=10)
    frm.grid()
    combo = ttk.Combobox(frm, values=city_list)
    combo.grid(column=0, row=0)

    ttk.Button(frm, text="Obtener clima", command=lambda: get_city_data(combo.get(),lambda p: messagebox.showinfo('Correcto', 'Se ingresó correctamente la información'), lambda: messagebox.showerror('Error', 'Ocurrió un error al obtener los datos.'))).grid(column=0, row=1)

    combo_stock = ttk.Combobox(frm, values=stock_symbols)
    combo_stock.grid(column=1, row=0)

    ttk.Button(frm, text="Obtener stock", command=lambda: get_stock_data(combo_stock.get(),lambda p: messagebox.showinfo('Correcto', 'Se ingresó correctamente la información'), lambda: messagebox.showerror('Error', 'Ocurrió un error al obtener los datos.'))).grid(column=1, row=1)

    root.mainloop()

if __name__ == "__main__":
    main()

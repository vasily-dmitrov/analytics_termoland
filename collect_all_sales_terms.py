import pandas as pd
from API_termoland import API_termolad
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from dotenv import load_dotenv
from termolands_params import termolands
import os
import csv

new = False
# ДАТА, С КОТОРОЙ ГРУЗИТЬ ВИЗИТЫ и нужно ли сохранять в csv
try:
    payments = pd.read_csv('payments.csv')
    payments['date'] = pd.to_datetime(payments.dt_payment).dt.date
    dt = max(payments['date']) + timedelta(days=1)
except:
    new = True
    dt = datetime(2025,6,1).date()
if dt == datetime.now().date():
    exit()
need_to_csv = True

#чтение конфига и создание авторизации
load_dotenv()

login = os.getenv("login")
password = os.getenv("password")
auth = HTTPBasicAuth(login, password)

products = pd.DataFrame()
bill = pd.DataFrame()
sales = pd.DataFrame()
payments = pd.DataFrame()
refunds = pd.DataFrame()

def get_sales(term, params, historical=False):
    termolands[term]['api'] = API_termolad(term, params['url'], params['clubid'], params['apikey'])
    print(term, datetime.now())
    wtf = params['api'].get_sales_history(auth, params['dt_open'] if historical else dt)
    products_term = pd.DataFrame([i['products'][o]['product'] for i in wtf for o in range(len(i['products']))])
    bill_term = pd.DataFrame()
    sales_term = pd.DataFrame()
    payments_term = pd.DataFrame()
    for client in wtf:
        string = [{"sale_id": client['sale_id'],
                   'client_id': client['client_id'],
                   'dt': client['datetime'],
                   'total_discount': client['total_discount'],
                   'total_payment_amount': client['total_amount'],
                   'club_id': parameters['clubid']}]
        bill_term = pd.concat([bill_term, pd.DataFrame(string)])
        for prod in range(len(client['products'])):
            string = [{"sale_id": client['sale_id'],
                       'dt': client['datetime'],
                       'club_id': parameters['clubid'],
                       'product_id': client['products'][prod]['product']['product_id'],
                       'count': client['products'][prod]['count'],
                       'discount': client['products'][prod]['discount'],
                       'price': client['products'][prod]['price'],
                       'total_piece': client['products'][prod]['total']}]
            sales_term = pd.concat([sales_term, pd.DataFrame(string)])

        for pay in range(len(client['payments'])):
            string = [{"sale_id": client['sale_id'],
                       'club_id': parameters['clubid'],
                       'payment_id': client['payments'][pay]['payment_id'],
                       'dt_payment': client['payments'][pay]['datetime'],
                       'payment_amount': client['payments'][pay]['payment_amount'],
                       'type_payment': client['payments'][pay]['type']}]
            payments_term = pd.concat([payments_term, pd.DataFrame(string)])
    ref = params['api'].get_refunds_history(auth, params['dt_open'] if historical else dt)
    refund = pd.DataFrame()
    products_for_refund = pd.DataFrame()
    for row in ref:
        string = [{'refund_id': row['refund_id'],
                   "sale_id": row['sale_id'],
                   'client_id': row['client_id'],
                   'dt_refund': row['datetime'],
                   'total_discount': row['total_discount'],
                   'total_payment_amount': row['total_amount'],
                   'club_id': row['club_id']}]
        refund = pd.concat([refund, pd.DataFrame(string)])
        for prod in range(len(row['products'])):
            string = [{'refund_id': row['refund_id'],
                       "sale_id": row['sale_id'],
                       'club_id': row['club_id'],
                       'product_id': row['products'][prod]['product']['product_id'],
                       'count': row['products'][prod]['count'],
                       'discount': row['products'][prod]['discount'],
                       'price': row['products'][prod]['price'],
                       'total_piece': row['products'][prod]['total']}]

            products_for_refund = pd.concat([products_for_refund, pd.DataFrame(string)])
    refund = refund.merge(products_for_refund, on=['refund_id', 'sale_id', 'club_id'])

    return {'products': products_term,
            'sales': sales_term,
            'payments': payments_term,
            'bill': bill_term,
            'refunds': refund}

for name, parameters in termolands.items():
    if name == 'Termoland Физтех':
        total = get_sales(name, parameters, historical=False)
        products = pd.concat([products, total['products']])
        bill = pd.concat([bill, total['bill']])
        sales = pd.concat([sales, total['sales']])
        payments = pd.concat([payments, total['payments']])
        refunds = pd.concat([refunds, total['refunds']])

products = products.drop_duplicates()

if need_to_csv:
    if new:
        products.to_csv('products.csv', index=False)
        bill.to_csv('bills.csv', index=False)
        payments.to_csv('payments.csv', index=False)
        sales.to_csv('sales.csv', index=False)
        refunds.to_csv('refunds.csv', index=False)
    else:
        with open('products.csv', 'a', newline='', encoding='utf-8') as prdcts:
            # Создание объекта для записи
            csv_writer = csv.writer(prdcts)
            # Добавление данных
            csv_writer.writerows(products.values.tolist())
        # Открытие файла в режиме добавления
        with open('bills.csv', 'a', newline='') as bills:
            # Создание объекта для записи
            csv_writer = csv.writer(bills)
            # Добавление данных
            csv_writer.writerows(bill.values.tolist())

        with open('sales.csv', 'a', newline='') as sls:
            # Создание объекта для записи
            csv_writer = csv.writer(sls)
            # Добавление данных
            csv_writer.writerows(sales.values.tolist())

        with open('payments.csv', 'a', newline='') as pymnts:
            # Создание объекта для записи
            csv_writer = csv.writer(pymnts)
            # Добавление данных
            csv_writer.writerows(payments.values.tolist())

        with open('refunds.csv', 'a', newline='') as rfnds:
            # Создание объекта для записи
            csv_writer = csv.writer(rfnds)
            # Добавление данных
            csv_writer.writerows(refunds.values.tolist())
a=0
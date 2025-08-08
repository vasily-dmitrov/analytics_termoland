import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from bd_connect import db
import os
from dotenv import load_dotenv


########################################################################################################################
#библиотеки гугл таблиц
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

load_dotenv()  # автоматически ищет .env в текущей папке
# Пути и настройки гугл таблиц
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"]


creds = ServiceAccountCredentials.from_json_keyfile_name(os.getenv('json'), scope)
client = gspread.authorize(creds)

# Откройте таблицу по названию или по ключу
spreadsheet = client.open_by_key(os.getenv('googlesheet'))
term = spreadsheet.worksheet("term")  # имя листа
sub = spreadsheet.worksheet("sub")
total = spreadsheet.worksheet("total")
v2 = spreadsheet.worksheet("v2")
########################################################################################################################




# считывание исторических данных из csv, приведение типов данных к нужным
#TODO: залить все данные в ДБ и тянуть оттуда
bill = pd.read_csv('bills.csv')
products = pd.read_csv('products.csv')
products['division'][products.division.str.contains('пептид', case=False)] = 'Пептиды'
payments = pd.read_csv('payments.csv')
sales = pd.read_csv('sales.csv')

refunds = pd.read_csv('refunds.csv')
refunds = refunds[['sale_id', 'club_id', 'product_id', 'client_id', 'dt_refund', 'refund_id', 'total_payment_amount']]
refunds['date'] = pd.to_datetime(refunds.dt_refund).dt.date
refunds = refunds.merge(products, on='product_id', how='left')
refunds['category'] = refunds.division
products = products.drop_duplicates()

# склеивание продаж с чеками
sales = sales.merge(bill, on=['club_id', 'sale_id'], how='left', suffixes=('_sales', '_bill'))

# перечисление субарендаторов физтеха в дикте
#TODO:считывать субарендаторов
sub_fiz = pd.DataFrame([{'category': 'Кафе', 'name' : 'ИП Корнева', 'is_sub': True, 'revenue_share': 20},
       {'category': 'Сафари бар', 'name': 'ИП Халтурин','is_sub': True, 'revenue_share': 30},
        {'category': 'Сафари бар', 'name': 'ИП Аташов','is_sub': True, 'revenue_share': 30},
        {'category': 'СПА', 'name': 'ИП Балобанова', 'is_sub':True, 'revenue_share': 30},
         {'category': 'СПА', 'name': 'СПА ИП Королькова', 'is_sub':True, 'revenue_share': 30},
         {'category': 'Фиш-пиллинг', 'name': 'ИП Корольков С.Н.', 'is_sub':True, 'revenue_share': 25},
          {'category': 'Фото салон', 'name': 'ООО "ТАЙМ"', 'is_sub': True, 'revenue_share': 30},
           {'category': 'Кислород. коктейли', 'name': 'ИП Познанская', 'is_sub':True, 'revenue_share': 30},
            {'category': 'Магазин', 'name':'ИП Шаров', 'is_sub': True, 'revenue_share': 5},
             {'category': 'Парения', 'name': 'ИП Юсипов', 'is_sub':True, 'revenue_share': 25},
              {'category': 'Бьюти Ролл', 'name': 'ИП Ермакова','is_sub': True, 'revenue_share': 20},
               {'category': 'Серьезная игра', 'name': 'ООО Серьёзная игра', 'is_sub': True, 'revenue_share': 20}])
fizteh = sales[sales.club_id == 'a14df2bd-045d-11ef-adca-2ece78709720'].merge(products, on='product_id')
subs = pd.pivot_table(fizteh[['agent_name', 'agent_inn', 'division']][fizteh.agent_inn.notna()], index='agent_name', values='division', aggfunc='count').reset_index()
fizteh['is_sub'] = False
sub_fiz['sub_name'] = None
fizteh['%'] = 100
fizteh['sum_pay'] = fizteh.total_piece

fizteh['date'] = pd.to_datetime(fizteh.dt_sales).dt.date




list_index = []
total = pd.DataFrame()
for s in sub_fiz.name:
    print(s)
    sub_fiz['club'] = 'a14df2bd-045d-11ef-adca-2ece78709720'
    p = fizteh[fizteh.division.str.contains(s)]
    p['category'] = sub_fiz['category'][sub_fiz.name == s].iloc[0]
    refunds['category'][refunds.division.str.contains(s)] = sub_fiz['category'][sub_fiz.name == s].iloc[0]
    sub_fiz['sub_name'][sub_fiz.name == s] = p['division'].iloc[0]
    p['is_sub'] = True
    p['%'] = sub_fiz['revenue_share'][sub_fiz.name == s].iloc[0]
    total = pd.concat([total, p])
    list_index = list_index + list(fizteh[fizteh.division.str.contains(s)].index)
total = pd.concat([total, fizteh[~fizteh.index.isin(list_index)]])
total = total.drop_duplicates()


total['category'][total.is_sub == False] = total['division'][total.is_sub == False]

total['category'][total.division == 'Бар'] = 'Кафе'
total['is_sub'][total.category == 'Кафе'] = True

pivot_refunds = pd.pivot_table(refunds, index=['category', 'date'], values='total_payment_amount', aggfunc='sum').reset_index()

#total = total[~((total.payment_amount < total.sum_pay) & (total.type_payment == 'cash'))]
pivot = pd.pivot_table(total[(total.is_sub == False)],
                       index=['date', 'category'],
                       values='sum_pay',
                       aggfunc='sum').reset_index()

pivot['dt'] = pd.to_datetime(pivot['date'])
pivot['month_name'] = pd.to_datetime(pivot['dt']).dt.strftime('%B')
total['month_name'] = pd.to_datetime(total['dt_sales']).dt.strftime('%B')
pivot = pivot.merge(pd.pivot_table(total[(total.is_sub == False)],
                                   index=['category', 'month_name'],
                                   values='sum_pay',
                                   aggfunc='sum').reset_index(),
                    on=['category', 'month_name'],
                    suffixes=('', '_month'))
pivot = pivot.merge(pivot_refunds, on=['category', 'date'], how='left').fillna(0)
pivot['with_refunds'] = pivot['sum_pay'] - pivot.total_payment_amount
pivot['month_with_refunds'] = pivot['sum_pay_month'] - pivot.total_payment_amount
pivot = pivot[['date', 'category', 'with_refunds', 'month_with_refunds', 'sum_pay', 'sum_pay_month', 'total_payment_amount', 'month_name', 'dt']]

pivot_sub = pd.pivot_table(total[(total.is_sub == True) ],
                       index=['date', 'category'],
                       values='sum_pay',
                       aggfunc='sum').reset_index()

pivot_sub['dt'] = pd.to_datetime(pivot_sub['date'])
pivot_sub['month_name'] = pd.to_datetime(pivot_sub['dt']).dt.strftime('%B')

pivot_sub_month =  pd.pivot_table(total[(total.is_sub == True)],
                       index=['category', 'month_name'],
                       values='sum_pay',
                       aggfunc='sum').reset_index()

pivot_sub = pivot_sub.merge(pivot_sub_month, on=['category', 'month_name'], suffixes=('', '_month'))
pivot_sub = pivot_sub.merge(pivot_refunds, on=['category', 'date'], how='left').fillna(0)
pivot_sub = pivot_sub.merge(sub_fiz[['category', 'revenue_share']], on='category', how='left').fillna(100).drop_duplicates()
pivot_sub['sum_pay'] = pivot_sub['sum_pay'] - pivot_sub.total_payment_amount
pivot_sub['sum_pay_month'] = pivot_sub['sum_pay_month'] - pivot_sub.total_payment_amount
pivot_sub['sub_revenue'] = pivot_sub.sum_pay * (pivot_sub['revenue_share']/100)
pivot_sub['sub_revenue_month'] = pivot_sub.sum_pay_month * (pivot_sub['revenue_share'] / 100)
need_cols = ['date',
             'category',
             'sub_revenue',
             'sum_pay',
             'sub_revenue_month',
             'sum_pay_month',
             'total_payment_amount',
             'month_name',
             'dt']
pivot_sub = pivot_sub[need_cols]

# Очистить листы
term.clear()
sub.clear()
# загрузка на листы в гугл таблице
set_with_dataframe(term, pivot)
set_with_dataframe(sub, pivot_sub)

# v2
pivot['week'] = pivot['dt'].dt.to_period('W-SUN')
pivot_sub['week'] = pivot_sub['dt'].dt.to_period('W-SUN')



v2_report = pd.pivot_table(pivot, index='week', values='with_refunds', aggfunc='sum').reset_index()
v2_report = v2_report.merge(pd.pivot_table(pivot_sub, index='week', values='sub_revenue', aggfunc='sum').reset_index(), on='week', how='left')

# query for clients
print('clients')
clients = pd.DataFrame(db.query("select * from total_clients"))
clients_text = "SELECT COLUMN_NAME  FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME = 'total_clients'"
clients.columns = [i[0] for i in db.query(clients_text)]
clients['dt'] = pd.to_datetime(clients['first_contact'])
print('clients done')

# query visits
print('visits')
visits_text = """select id_club, id_client, dt_exit, dt_entry, duration, nomenklature_name, nomenklature_id
                 from total_visits tv 
                join total_clubs tc on tc.id = tv.id_club
                where tv.dt_entry >= date('2024-08-01')
                and tc.name like '%изтех%' """
visits = pd.DataFrame(db.query(visits_text))
visits_cols = "SELECT COLUMN_NAME  FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME = 'total_visits'"
visits.columns = [i[0] for i in db.query(visits_cols)]
visits = visits.merge(clients, left_on='id_client', right_on='client_id', how='left').dropna()
visits['entry_date'] = pd.to_datetime(visits.dt_entry).dt.date
visits['is_first'] = visits['entry_date'] == visits['first_contact']
visits['week'] = pd.to_datetime(visits['dt_entry']).dt.to_period('W-SUN')
visits['type_visit'] = np.where(visits.nomenklature_name.str.contains('полный день', case=False), 'full',
                                 np.where(visits.nomenklature_name.str.contains('абонемент', case=False), 'season', 'hours')
                                 )

print('visits_done')

v2_time = pd.pivot_table(visits, index='week', values='duration', aggfunc=lambda x:x.sum()//60)
v2_time = v2_time.merge(pd.pivot_table(visits,
                                       index='week',
                                       columns='type_visit',
                                       values='duration',
                                       aggfunc=lambda x:x.sum()//60), on='week', how='left')

v2_clients = pd.pivot_table(visits, index='week', values='client_id', aggfunc='count').reset_index()
v2_clients = v2_clients.merge(pd.pivot_table(visits[visits.is_first], index='week', values='client_id', aggfunc='count').reset_index(), on='week', suffixes=('', '_first'))
v2_clients['cli_cumsum'] = v2_clients['client_id_first'].cumsum()
v2_report = v2_report.merge(v2_clients[['week', 'client_id','cli_cumsum']], on='week', how='left')
v2_report['revenue_with_%'] = v2_report.with_refunds + v2_report.sub_revenue
v2_report['clients_growth'] = v2_report.cli_cumsum - v2_report.cli_cumsum.shift(1)
v2_report['avg_bill'] = v2_report['revenue_with_%'] // v2_report.client_id
v2_report = v2_report.merge(v2_time, on='week', how='left')

v2_sub = pd.pivot_table(pivot_sub, index=['week', 'category'], values='sum_pay', aggfunc='sum').reset_index()
v2_sub = v2_sub.merge(pd.pivot_table(pivot_sub, index='week', values='sum_pay', aggfunc='sum').reset_index(),
                      on='week', how='left',suffixes=('_categ', ''))
v2_sub['%_from_total'] = (v2_sub['sum_pay_categ'] / v2_sub['sum_pay'])*100
v2_sub = pd.pivot(v2_sub, index='week', columns='category', values=['%_from_total', 'sum_pay', 'sum_pay_categ']).reset_index()
v2_sub.columns = ['_'.join(i) for i in v2_sub.columns]
v2_report = v2_report.merge(v2_sub, left_on='week',right_on='week_', how='left')

drop_col = ['week_', 'sum_pay_Бьюти Ролл',  'sum_pay_Кислород. коктейли', 'sum_pay_Магазин', 'sum_pay_Парения',
            'sum_pay_СПА', 'sum_pay_Сафари бар', 'sum_pay_Серьезная игра', 'sum_pay_Фиш-пиллинг', 'sum_pay_Фото салон',
             'with_refunds', 'sub_revenue']

rename_col = {'sum_pay_Кафе': 'ВЫРУЧКА АРЕНДАТОРОВ',
              'week': 'неделя',
              'client_id': 'количество посещений',
              'cli_cumsum': 'клиентов в базе',
              'revenue_with_%': 'выручка+%',
              'clients_growth': "прирост клиентов",
              'avg_bill': "ср.чек",
              'duration': "часов в комплексе",
              'full': "полный день",
              'hours': "почасовка",
              'season': "абонементы",
              '%_from_total_Бьюти Ролл': "% от выручки Бьюти Ролл",
              '%_from_total_Кафе': "% от выручки Кафе",
              '%_from_total_Кислород. коктейли': "% от выручки Кислород. коктейли",
              '%_from_total_Магазин': "% от выручки Магазин",
              '%_from_total_Парения': "% от выручки Парения",
              '%_from_total_СПА': "% от выручки СПА",
              '%_from_total_Сафари бар': "% от выручки Сафари бар",
              '%_from_total_Серьезная игра': "% от выручки Серьёзная игра",
              '%_from_total_Фиш-пиллинг': "% от выручки Фиш-пиллинг",
              '%_from_total_Фото салон': "% от выручки Фото салон",
              'sum_pay_categ_Бьюти Ролл': "выручка Бьюти Ролл",
              'sum_pay_categ_Кафе': "выручка Кафе",
              'sum_pay_categ_Кислород. коктейли': "выручка Кислород. коктейли",
              'sum_pay_categ_Магазин': "выручка Магазин",
              'sum_pay_categ_Парения': "выручка Парения",
              'sum_pay_categ_СПА': "выручка СПА",
              'sum_pay_categ_Сафари бар': "выручка Сафари бар",
              'sum_pay_categ_Серьезная игра': "выручка Серьезная игра",
              'sum_pay_categ_Фиш-пиллинг': "выручка Фиш-пиллинг",
              'sum_pay_categ_Фото салон': "выручка Фото салон"}

v2_report = v2_report.drop(columns=drop_col)
v2_report = v2_report.rename(columns=rename_col)
v2_report['РАСХОДЫ'] = 0
v2_report['прибыль'] = 0
v2_report['маржинальность'] = 0
v2_report = v2_report[['неделя', 'выручка+%', 'РАСХОДЫ', 'прибыль', 'маржинальность', "ср.чек", 'количество посещений',
                       'клиентов в базе', "прирост клиентов", "часов в комплексе", "полный день", "почасовка",
                       "абонементы", 'ВЫРУЧКА АРЕНДАТОРОВ', "выручка Кафе", "% от выручки Кафе", "выручка Бьюти Ролл",
                       "% от выручки Бьюти Ролл", "выручка Кислород. коктейли", "% от выручки Кислород. коктейли",
                       "выручка Магазин", "% от выручки Магазин", "выручка Парения", "% от выручки Парения",
                       "выручка СПА", "% от выручки СПА", "выручка Сафари бар", "% от выручки Сафари бар",
                       "выручка Серьезная игра", "% от выручки Серьёзная игра", "выручка Фиш-пиллинг",
                       "% от выручки Фиш-пиллинг", "выручка Фото салон", "% от выручки Фото салон",]]
v2_report = v2_report.fillna(0)
v2_report = v2_report.T
v2_report = v2_report.reset_index()


#total.clear()
v2.clear()

# Загрузить DataFrame
#set_with_dataframe (worksheet3, total)
set_with_dataframe(v2, v2_report)

a=0
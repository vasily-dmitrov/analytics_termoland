import pandas as pd
from datetime import datetime
import warnings


warnings.filterwarnings('ignore')



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
#sales.csv = sales.csv.merge(payments, on=['sale_id', 'club_id'], how='left')
sales = sales.merge(bill, on=['club_id', 'sale_id'], how='left', suffixes=('_sales', '_bill'))


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
pivot = pivot.merge(pd.pivot_table(total[(total.is_sub == False)],
                                   index='category',
                                   values='sum_pay',
                                   aggfunc='sum').reset_index(),
                    on='category',
                    suffixes=['', '_month'])
pivot = pivot.merge(pivot_refunds, on=['category', 'date'], how='left').fillna(0)
pivot['with_refunds'] = pivot['sum_pay'] - pivot.total_payment_amount
pivot['month_with_refunds'] = pivot['sum_pay_month'] - pivot.total_payment_amount
pivot = pivot[['date', 'category', 'with_refunds', 'month_with_refunds', 'sum_pay', 'sum_pay_month', 'total_payment_amount']]
pivot_sub = pd.pivot_table(total[(total.is_sub == True) ],
                       index=['date', 'category'],
                       values='sum_pay',
                       aggfunc='sum').reset_index()



pivot_sub_month =  pd.pivot_table(total[(total.is_sub == True)],
                       index='category',
                       values='sum_pay',
                       aggfunc='sum').reset_index()

pivot_sub = pivot_sub.merge(pivot_sub_month, on='category', suffixes=('', '_month'))
pivot_sub = pivot_sub.merge(pivot_refunds, on=['category', 'date'], how='left').fillna(0)
pivot_sub = pivot_sub.merge(sub_fiz[['category', 'revenue_share']], on='category', how='left').fillna(100).drop_duplicates()
pivot_sub['sum_pay'] = pivot_sub['sum_pay'] - pivot_sub.total_payment_amount
pivot_sub['sum_pay_month'] = pivot_sub['sum_pay_month'] - pivot_sub.total_payment_amount
pivot_sub['sub_revenue'] = pivot_sub.sum_pay * (pivot_sub['revenue_share']/100)
pivot_sub['sub_revenue_month'] = pivot_sub.sum_pay_month * (pivot_sub['revenue_share']/100)
pivot_sub = pivot_sub[['date', 'category', 'sub_revenue', 'sum_pay', 'sub_revenue_month', 'sum_pay_month', 'total_payment_amount']]
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials



# Пути и настройки
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name('terms-465409-25c8ae426aee.json', scope)
client = gspread.authorize(creds)

# Откройте таблицу по названию или по ключу
spreadsheet = client.open_by_key('1qbZBq2skoVxLE6aJzw_VBnzzrqK_oB6bTScvkIq8fB0')  # или client.open_by_key('/1qbZBq2skoVxLE6aJzw_VBnzzrqK_oB6bTScvkIq8fB0/')
worksheet1 = spreadsheet.worksheet("term")  # имя листа
worksheet2 = spreadsheet.worksheet("sub")
worksheet3 = spreadsheet.worksheet("total")
# Очистить лист (по желанию)
worksheet1.clear()
worksheet2.clear()
worksheet3.clear()

# Загрузить DataFrame
set_with_dataframe(worksheet1, pivot)
set_with_dataframe(worksheet2, pivot_sub)
set_with_dataframe(worksheet3, total)
a=0
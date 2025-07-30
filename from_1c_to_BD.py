from API_termoland import API_termolad
from bd_connect import db
import pandas as pd
from datetime import datetime, timedelta
from termolands_params import termolands
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

start = datetime.now()
print(start)
### этап загрузки визитов
#   1. определение последней даты загрузки и если она совпадает со вчерашним днём, то загрузка отменяется
#   2. выгрузка визитов с последней даты и предобработка: удаление кривых данных, преобразование типов данных
#   3. определдить новых клиентов за выгруженный период
##################

#чтение конфига и создание авторизации
load_dotenv()

login = os.getenv("login")
password = os.getenv("password")
auth = HTTPBasicAuth(login, password)


clients = pd.DataFrame(db.query("select * from total_clients"))
clients.columns = [i[0] for i in db.query("SELECT COLUMN_NAME  FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME = 'total_clients'")]


new_clients = pd.DataFrame()

def prepare_your_ass(df):
    '''
    Prepare df with visits information for load to DB
    :param df:
    :return:
    '''
    df.phone = df.phone.astype(str)
    df['visit_dt_date'] = pd.to_datetime(df.visit_date, dayfirst=True).dt.date

    # хочу удалить все тестовые данные
    for tel in df.phone[df.surname.str.contains('тест', case=False, na=False)].unique():
        df = df[~(df.phone == tel)]
    for tel in df.phone[df.surname.str.contains('test', case=False, na=False)].unique():
        df = df[~(df.phone == tel)]
    # хочу удалить все наны в фамилиях и именах
    df = df[~df.surname.isna()]
    df = df[~df.name.isna()]

    departure = pd.concat([df[df.duration_min > 1000], df[df.duration_min <= 0]])
    df = df[~df.visit_id.isin(departure.visit_id)]
    f = '%d.%m.%Y %H:%M:%S'

    departure['date_of_departure'] = departure.apply(lambda x: x.date_of_arrival[:10] if x.time_close != '00:00'
    else datetime.strftime(datetime.strptime(x.date_of_arrival, f).date() + timedelta(days=1), f[:8]), axis=1)
    departure['date_of_departure'] = departure['date_of_departure'] + ' ' + departure.time_close + ':00'
    df = pd.concat([df, departure])
    for col in ['date_of_departure', 'date_of_arrival']:
        df['exit' if 'depart' in col else 'entry'] = pd.to_datetime(df[col], format='%d.%m.%Y %H:%M:%S',
                                                                    errors='coerce')
    df = df[df.exit.notna()]
    df = df[df.entry.notna()]
    df['duration'] = df.apply(lambda x: int((x.exit - x.entry).total_seconds() / 60), axis=1)
    # уникальные клиенты с их датой первого входа в клуб
    df['email'] = df.email.fillna('')
    df['phone'] = df.phone.fillna('')
    df['birthday'] = df.birthday.fillna('')

    clnts = \
        pd.pivot_table(df, index=['client_id', 'surname', 'name', 'patronymic', 'birthday', 'email', 'sex', 'phone'],
                       values='visit_dt_date', aggfunc='min').reset_index()

    return df, clnts




visits_df = pd.DataFrame()
for name, parameters in termolands.items():
    print(name)
    termolands[name]['api'] = API_termolad(name, parameters['url'], parameters['clubid'], parameters['apikey'])
    last_visits_dt = db.query(f"select date(MAX(dt_entry)) from total_visits tv where id_club = '{parameters['clubid']}'")[0][0]
    if last_visits_dt == datetime.now().date() - timedelta(days=1):
        continue
    elif last_visits_dt == None:
        visit = pd.DataFrame(termolands[name]['api'].get_visit_history(auth, parameters['dt_open']))
    else:
        visit = pd.DataFrame(termolands[name]['api'].get_visit_history(auth, last_visits_dt + timedelta(days=1)))
    visit['dt_open'] = parameters['dt_open']
    visit['time_open'] = parameters['time_open']
    visit['time_close'] = parameters['time_close']
    visit['region'] = parameters['region']
    visit['club_name'] = name

    opa = prepare_your_ass(visit)
    new_clients = pd.concat([new_clients, opa[1]])
    visits_df = pd.concat([visits_df, opa[0]])
    print(datetime.now() - start)

for_definition_new = ['client_id', 'surname', 'phone']
new_clients = new_clients.drop_duplicates()
new_clients = new_clients[for_definition_new].merge(clients[['client_id', 'surname', 'phone', 'name']],
                                            on=for_definition_new, how='left')

new_clients = new_clients[new_clients['name'].isna()].drop(columns=['name'])


# Формируем SQL-запрос для клиентов
cols = ', '.join(f'`{col}`' for col in new_clients.columns)
placeholders = ', '.join(['%s'] * len(new_clients.columns))
sql_string = f"INSERT INTO total_clients ({cols}) VALUES ({placeholders})"
# Преобразуем DataFrame в список кортежей
cli_for_db = new_clients[new_clients.columns].values.tolist()
db.query(sql_string, 'insert', cli_for_db)


# Формируем SQL-запрос для визитов
cols = ', '.join(f'`{col}`' for col in visits_df.columns)
placeholders = ', '.join(['%s'] * len(visits_df.columns))
sql_string = f"INSERT INTO total_visits ({cols}) VALUES ({placeholders})"
# Преобразуем DataFrame в список кортежей
vis_for_db = visits_df[visits_df.columns].values.tolist()
db.query(sql_string, 'insert', vis_for_db)
a=0
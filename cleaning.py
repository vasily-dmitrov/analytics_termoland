import pandas as pd
import warnings
from datetime import datetime, timedelta

from dateutil.utils import today

warnings.filterwarnings('ignore')


total_clubs = pd.DataFrame()
clients = pd.DataFrame()
total_visits = pd.DataFrame()
products = pd.DataFrame()
data = pd.read_csv('visits.csv')


for land in data['club_name'].unique():

    start = datetime.now()
    print('start', start, land)
    df = data[data.club_name == land]
    df.phone = df.phone.astype(str)
    df['visit_dt_date'] = pd.to_datetime(df.visit_date,  dayfirst=True).dt.date

    #хочу удалить все тестовые данные
    for tel in df.phone[df.surname.str.contains('тест', case=False, na=False)].unique():
        df = df[~(df.phone == tel)]
    for tel in df.phone[df.surname.str.contains('test', case=False, na=False)].unique():
        df = df[~(df.phone == tel)]
    # хочу удалить все наны в фамилиях и именах
    df = df[~df.surname.isna()]
    df = df[~df.name.isna()]

    # поправить все даты выбытия

    if land == 'Fitland Белорусская':
        df.date_of_arrival[df.visit_id == 'b79174f9-e9f3-11ee-bbc2-bac9a8f39dd2'] = '24.03.2024 20:00:00'
        df.date_of_arrival[df.visit_id == '923763e3-6a28-11ef-bbc8-bac9a8f39dd2'] = '03.09.2024 22:12:00'
        df.date_of_arrival[df.visit_id == '06486eeb-a409-11ef-bbca-bac9a8f39dd2'] = '16.11.2024 13:50:00'
        df.date_of_arrival[df.visit_id == '06486eeb-a409-11ef-bbca-bac9a8f39dd2'] = '06.01.2025 19:00:00'
        df.date_of_arrival[df.visit_id == '82b2219d-345d-11ef-bbc7-bac9a8f39dd2'] = '27.06.2024 11:10:00'
    if land == 'Termoland РИО':
        df.date_of_arrival[df.visit_id == 'de7bed6b-9a00-11ef-b818-ea10e92f991e'] = '03.11.2024 19:33:44'
    if land == 'Termoland Физтех':
        df.date_of_arrival[df.visit_id == 'c965e84e-b3e2-11ef-b817-f4b4a4e2b81a'] = '06.12.2024 18:00:53'
    if land == 'Termoland Пенза':
        df.date_of_arrival[df.visit_id == 'bb298359-4391-11f0-b81d-b23715a9290c'] = '07.06.2025 20:24:07'


    departure = pd.concat([df[df.duration_min > 1000], df[df.duration_min <= 0]])
    df = df[~df.visit_id.isin(departure.visit_id)]
    f = '%d.%m.%Y %H:%M:%S'

    departure['date_of_departure'] = departure.apply(lambda x: x.date_of_arrival[:10] if x.time_close != '00:00'
        else datetime.strftime(datetime.strptime(x.date_of_arrival, f).date() + timedelta(days=1), f[:8]), axis=1)
    departure['date_of_departure'] = departure['date_of_departure'] + ' ' + departure.time_close + ':00'
    df = pd.concat([df, departure])
    for col in ['date_of_departure', 'date_of_arrival']:
        df['exit' if 'depart' in col else 'entry'] = pd.to_datetime(df[col], format='%d.%m.%Y %H:%M:%S', errors='coerce')
    err = df[df.exit.isna()]
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
    clients = pd.concat([clients, clnts])

    total_clubs = pd.concat([total_clubs,
                             df[['club_id', 'club_name', 'dt_open', 'region', 'time_open', 'time_close']].head(1)])



    total_visits = pd.concat([total_visits,
                              df[['club_id', 'client_id', 'exit', 'entry', 'duration',
                                  'nomenklature_name', 'nomenklature_id']]])
    print('end', datetime.now(), datetime.now() - start)

total_visits = total_visits.rename(columns={'club_id': 'id_club',
                                            'client_id': 'id_client',
                                            'exit': 'dt_exit',
                                            'entry': 'dt_entry'})

total_clubs = total_clubs.rename(columns={'club_id': 'id', 'club_name': 'name'})

clients = clients.rename(columns={'clientd_id': 'id_client',
                                  'patronymic': 'middle_name',
                                  'birthday': 'dt_birth',
                                  'visit_dt_date': 'first_contact'})

clients['phone'] = clients.phone.astype(str)
clients.to_csv('total_clients.csv', index=False)
total_visits = total_visits[total_visits.dt_entry <= datetime.strftime(today(), format='%d.%m.%Y %H:%M:%S')]
total_visits.to_csv('total_visits.csv', index=False)

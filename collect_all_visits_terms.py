import pandas as pd
from API_termoland import API_termolad
from requests.auth import HTTPBasicAuth
from datetime import datetime
from dotenv import load_dotenv
from termolands_params import termolands
import os

# ДАТА, С КОТОРОЙ ГРУЗИТЬ ВИЗИТЫ и нужно ли сохранять в csv
dt = datetime(2025,6,30)
historical = True
need_to_csv = True
term_unit = None

#чтение конфига и создание авторизации
load_dotenv()

login = os.getenv("login")
password = os.getenv("password")
auth = HTTPBasicAuth(login, password)

def get_visits(term, params):
    start = datetime.now()
    print('start', term, start)
    termolands[term]['api'] = API_termolad(term, params['url'], params['clubid'], params['apikey'])
    df = pd.DataFrame(params['api'].get_visit_history(auth, params['dt_open'].date() if historical else dt))
    df['dt_open'] = params['dt_open']
    df['time_open'] = params['time_open']
    df['time_close'] = params['time_close']
    df['region'] = params['region']
    df['club_name'] = term
    print('end', datetime.now() - start)
    return df

visits_df = pd.DataFrame()
if term_unit == None:
    for name, parameters in termolands.items():
        visits_df = pd.concat([visits_df, get_visits(name, parameters)])
else:
    visits_df = pd.concat([visits_df, get_visits(term_unit, termolands[term_unit])])
if need_to_csv:
    visits_df.to_csv(f'visits_from_{dt.date()}.csv', index=False)
a=0

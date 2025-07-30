from datetime import datetime, timedelta, date
import requests as rq

class API_termolad:
    def __init__(self, name, url, club_id, apikey):
        self.apikey = apikey
        self.url = url
        self.club_id = club_id
        self.name  = name
        self.trouble_dates = []

    def get_visit_history(self, authen, dt_from, dt_end=datetime.now().date() - timedelta(days=1)):
        total = []
        if not isinstance(dt_from, date):
            dt_from = dt_from.date()
        while dt_from <= dt_end:
            print(dt_from)
            query = {
                "Method": "post_visits_history",
                "ClubId": self.club_id,
                "Parameters": {
                    "StartDate": dt_from.strftime('%Y-%m-%d') + ' 00:00 +03:00',
                    "EndDate": (dt_from + timedelta(days=1)).strftime('%Y-%m-%d') + ' 00:00 +03:00'
                },
                "Request_id": "1b54468c-f08c-4a7d-953f-a30274be9b89"
            }
            resp = rq.post(url=self.url+'v1/', json=query, auth=authen)
            if resp.status_code == 200:
                response = resp.json()
                total = total + response['Parameters']
            else:
                self.trouble_dates.append(dt_from)
            dt_from = dt_from + timedelta(days=1)
        return total

    def get_sales_history(self, authen, dt_from, dt_end=datetime.now().date() - timedelta(days=1)):
        total = []
        if not isinstance(dt_from, date):
            dt_from = dt_from.date()
        while dt_from <= dt_end:
            print(dt_from)
            headers = {
                "clubid": self.club_id,
                "apikey": self.apikey
            }
            method = "purchase_history_new"
            parameters = {
                "start_date": dt_from.strftime('%Y-%m-%d') + ' 00:00 +03:00',
                "end_date": (dt_from + timedelta(days=1)).strftime('%Y-%m-%d') + ' 00:00 +03:00'
            }
            resp = rq.get(url=self.url+'v3/'+method, params=parameters,headers=headers, auth=authen)
            if resp.status_code == 200:
                response = resp.json()
                if len(response['data']) != 0:
                    total = total + response['data']
                else:
                    self.trouble_dates.append(dt_from)
            else:
                self.trouble_dates.append(dt_from)
            dt_from = dt_from + timedelta(days=1)
        return total

    def get_refunds_history(self, authen, dt_from, dt_end=datetime.now().date() - timedelta(days=1)):
        total = []
        if not isinstance(dt_from, date):
            dt_from = dt_from.date()
        while dt_from <= dt_end:
            print(dt_from)
            headers = {
                "clubid": self.club_id,
                "apikey": self.apikey
            }
            method = "refunds_history_new"
            parameters = {
                "start_date": dt_from.strftime('%Y-%m-%d') + ' 00:00 +03:00',
                "end_date": (dt_from + timedelta(days=1)).strftime('%Y-%m-%d') + ' 00:00 +03:00'
            }
            resp = rq.get(url=self.url+'v3/'+method, params=parameters,headers=headers, auth=authen)
            if resp.status_code == 200:
                response = resp.json()
                if len(response['data']) != 0:
                    total = total + response['data']
                else:
                    self.trouble_dates.append(dt_from)
            else:
                self.trouble_dates.append(dt_from)
            dt_from = dt_from + timedelta(days=1)
        return total
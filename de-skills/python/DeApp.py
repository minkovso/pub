import requests
import os
import time
from datetime import datetime

while True:
    c_date = datetime.today()


    def get_ids(p):
        url = 'https://api.hh.ru/vacancies?text="Data+Engineer"&date_from=%s&page=%d' % (c_date.strftime('%Y-%m-%d'), p)
        r = requests.get(url)
        if r.status_code != 200:
            return list()
        else:
            return [i['id'] for i in r.json()["items"]] + get_ids(p+1)


    with open('/home/spark/stream/tmp/de_%s.json' % c_date.strftime("%Y-%m-%d_%H-%M-%S"), 'w') as f:
        for id_ in get_ids(0):
            f.write(requests.get('https://api.hh.ru/vacancies/%s' % id_).text + '\n')

    os.rename('/home/spark/stream/tmp/de_%s.json' % c_date.strftime("%Y-%m-%d_%H-%M-%S"),
              '/home/spark/stream/de_%s.json' % c_date.strftime("%Y-%m-%d_%H-%M-%S"))
    time.sleep(180)
    os.remove('/home/spark/stream/de_%s.json' % c_date.strftime("%Y-%m-%d_%H-%M-%S"))

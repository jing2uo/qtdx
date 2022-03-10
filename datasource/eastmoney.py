import requests
from datetime import datetime
from utils import loads_jsonp

fs = {
    'sh': 'm:1 t:2,m:1 t:23',
    'sz': 'm:0 t:6,m:0 t:80',
    'bj': 'm:0 t:81 s:2048',
    'gem': 'm:0 t:80',
    'star': 'm:1 t:23',
    'sh_hk': 'b:BK0707',
    'sz_hk': 'b:BK0804'
}


def get_stock_list(fs):
    l = []
    params = {
        'pn': '1',
        'pz': '20000',
        'fields': 'f12,f14,f26'
    }

    params['fs'] = fs

    r = requests.get(
        'https://33.push2.eastmoney.com/api/qt/clist/get', params=params)

    j = loads_jsonp(r.text)['data']['diff']
    for i in range(len(j)):
        stock = {
            'code': j[str(i)]['f12'],
            'name':  j[str(i)]['f14']
        }
        d = j[str(i)]['f26']
        if d != 0:
            stock['listed_date'] = datetime.strptime(
                str(j[str(i)]['f26']), '%Y%m%d').isoformat()
        else:
            stock['listed_date'] = ''

        l.append(stock)
    return l


def get_all_stocks():
    l = []
    sh_list = get_stock_list(fs['sh'])
    for i in sh_list:
        i['exchange'] = 'sh'

    star_list = get_stock_list(fs['star'])
    star_temp = [i['code'] for i in star_list]
    for i in sh_list:
        if i['code'] in star_temp:
            i['market'] = 'sh_star'
        else:
            i['market'] = 'main'

    sz_list = get_stock_list(fs['sz'])
    for i in sz_list:
        i['exchange'] = 'sz'

    gem_list = get_stock_list(fs['gem'])
    gem_temp = [i['code'] for i in gem_list]
    for i in sz_list:
        if i['code'] in gem_temp:
            i['market'] = 'sz_gem'
        else:
            i['market'] = 'main'

    bj_list = get_stock_list(fs['bj'])
    for i in bj_list:
        i['exchange'] = 'bj'
        i['market'] = 'main'
    l += sz_list
    l += sh_list
    l += bj_list

    shhk = [i['code'] for i in get_stock_list(fs['sh_hk'])]
    szhk = [i['code'] for i in get_stock_list(fs['sz_hk'])]
    for i in l:
        if i['code'] in (shhk + szhk):
            i['if_hksc'] = True
        else:
            i['if_hksc'] = False
    return l

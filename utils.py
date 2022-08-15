import pandas as pd
import grequests
import requests
import time
import os
from datetime import datetime, timedelta
import pytz

# Получить истинные данные о доступности для всех продуктов
def get_true_data_all():
    cookies = {
        'metabase.DEVICE': '154ee79b-96f7-4af8-a82a-0ecbe3f608c8',
        '_ym_uid': '1655094110346037389',
        '_ym_d': '1655094110',
        '_fw_crm_v': 'adc348b5-c953-443a-bb89-0755c124ce89',
        '__ddg1_': 'xeGIzTrgFAS6pXJb5hM9',
        'ajs_user_id': '32708',
        '__stripe_mid': '3bf3ca97-3eef-4dd4-968c-624b4bb5b986e852db',
        '_gcl_au': '1.1.2127845954.1659520533',
        '_gaexp': 'GAX1.2.VNxeiAz4Ts2HQRh10ilPGw.19293.0',
        'ajs_anonymous_id': '1ee568-c345-da1f-3f6a-a7ab2d7a56e5',
        '_ga_F7SJXGRF0F': 'GS1.1.1659592255.13.0.1659592255.60',
        'metabase.SESSION': 'e2078f80-164b-41be-baa1-b2b874da556f',
        'csrftoken': 'DIHpHsyu5qm0b9q38KY6d3xFQJ0jMTuDD0PhUohWlsfNo31GUSRbyHstEss7UMlf',
        'sessionid': '3px5a0nammdcer6gqd4k8l2wveak007g',
        '_gid': 'GA1.2.94658197.1659942948',
        '_ym_isad': '2',
        '_ga': 'GA1.2.1725221649.1655094112',
        '_ga_K8FNCP0QXZ': 'GS1.1.1659942947.1.1.1659943447.0',
    }
    headers = {
        'authority': 'meta.wegotrip.com',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Yandex";v="92"',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 YaBrowser/21.8.0.1967 (beta) Yowser/2.5 Safari/537.36',
        'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
        'accept': '*/*',
        'origin': 'https://meta.wegotrip.com',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://meta.wegotrip.com/question',
        'accept-language': 'ru,en;q=0.9',
    }
    data = {
    'query': '{"type":"query","database":3,"query":{"source-table":"card__449"},"middleware":{"js-int-to-string?":true,"add-default-userland-constraints?":true}}',
    'visualization_settings': '{"column_settings":{},"table.pivot":false,"table.pivot_column":"available","table.cell_column":"capacity","table.columns":[{"name":"id","fieldRef":["field",1360,null],"enabled":true},{"name":"start","fieldRef":["field",1358,{"temporal-unit":"default"}],"enabled":true},{"name":"end","fieldRef":["field",1356,{"temporal-unit":"default"}],"enabled":true},{"name":"capacity","fieldRef":["field",1362,null],"enabled":true},{"name":"product_id","fieldRef":["field",1357,null],"enabled":true},{"name":"available","fieldRef":["field",1363,null],"enabled":true},{"name":"autoupdate","fieldRef":["field",1361,null],"enabled":true},{"name":"real_capacity","fieldRef":["field",1359,null],"enabled":true},{"name":"tripster_id","fieldRef":["field",1662,null],"enabled":true},{"name":"name","fieldRef":["field",1390,null],"enabled":true},{"name":"bookingType","fieldRef":["field",1398,null],"enabled":true},{"name":"wgt_product_id","fieldRef":["field",1369,null],"enabled":true},{"name":"cutoff","fieldRef":["field",1396,null],"enabled":true},{"name":"name_2","fieldRef":["field",1504,null],"enabled":true},{"name":"offset","fieldRef":["field",1506,null],"enabled":true}],"table.column_formatting":[]}',
}
    response = requests.post('https://meta.wegotrip.com/api/dataset/json', cookies=cookies, headers=headers, data=data)
    df = pd.json_normalize(response.json())
    df.columns = ['product_id_micro', 'start', 'end', 'booking_type', 'cuttoff','real_capacity', 'id', 'timezone_name', 'offset', 'autoupdate', 'product_id', 'capacity', 'tripster_id', 'available', 'product_name']
    df['offset'] = df['timezone_name'].apply(lambda x: datetime.now(pytz.timezone(x)).utcoffset().total_seconds()/3600)
    df['start'] = [time + timedelta(hours=offset) for time, offset in zip(pd.DatetimeIndex(df['start']), df['offset'])]
    df['start_date'] = pd.to_datetime(df['start']).dt.date
    df['start_time'] = pd.to_datetime(df['start']).dt.time
    df['available'] = df['capacity'].apply(lambda x: False if x == 0 else True)
    df = df.sort_values('start')
    df = df.drop(['start', 'capacity'], axis=1)
    return df

# Получить истинные данные о доступности для конкретного продукта по id
def get_true_data_one_product(product_id):
    cookies = {
        'metabase.DEVICE': '154ee79b-96f7-4af8-a82a-0ecbe3f608c8',
        '_ym_uid': '1655094110346037389',
        '_ym_d': '1655094110',
        '_fw_crm_v': 'adc348b5-c953-443a-bb89-0755c124ce89',
        '__ddg1_': 'xeGIzTrgFAS6pXJb5hM9',
        'ajs_user_id': '32708',
        '__stripe_mid': '3bf3ca97-3eef-4dd4-968c-624b4bb5b986e852db',
        '_gcl_au': '1.1.2127845954.1659520533',
        '_gaexp': 'GAX1.2.VNxeiAz4Ts2HQRh10ilPGw.19293.0',
        'ajs_anonymous_id': '1ee568-c345-da1f-3f6a-a7ab2d7a56e5',
        '_ga_F7SJXGRF0F': 'GS1.1.1659592255.13.0.1659592255.60',
        'metabase.SESSION': 'e2078f80-164b-41be-baa1-b2b874da556f',
        'csrftoken': 'DIHpHsyu5qm0b9q38KY6d3xFQJ0jMTuDD0PhUohWlsfNo31GUSRbyHstEss7UMlf',
        'sessionid': '3px5a0nammdcer6gqd4k8l2wveak007g',
        '_ga': 'GA1.2.1725221649.1655094112',
        '_ga_K8FNCP0QXZ': 'GS1.1.1660192945.5.1.1660194768.0',
    }
    headers = {
        'authority': 'meta.wegotrip.com',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Yandex";v="92"',
        'accept': 'application/json',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 YaBrowser/21.8.0.1967 (beta) Yowser/2.5 Safari/537.36',
        'origin': 'https://meta.wegotrip.com',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://meta.wegotrip.com/question',
        'accept-language': 'ru,en;q=0.9',
    }
    json_data = {
        'query': {
            'source-table': 'card__449',
            'filter': [
                '=',
                [
                    'field',
                    1369,
                    None,
                ],
                product_id,
            ],
        },
        'database': 3,
        'type': 'query',
        'parameters': [],
    }
    response = requests.post('https://meta.wegotrip.com/api/dataset', cookies=cookies, headers=headers, json=json_data)
    columns_name_easy = ['id', 'start', 'end', 'capacity', 'product_id_micro', 'available', 'autoupdate', 'real_capacity', 'tripster_id', 'product_name', 'booking_type', 'product_id', 'cuttoff', 'timezone_name', 'offset']
    df = pd.DataFrame(response.json()['data']['rows'], columns=columns_name_easy)
    df['offset'] = df['timezone_name'].apply(lambda x: datetime.now(pytz.timezone(x)).utcoffset().total_seconds()/3600)
    df['start'] = [time + timedelta(hours=offset) for time, offset in zip(pd.DatetimeIndex(df['start']), df['offset'])]
    df['start_date'] = pd.to_datetime(df['start']).dt.date
    df['start_time'] = pd.to_datetime(df['start']).dt.time
    df['available'] = df['capacity'].apply(lambda x: False if x == 0 else True)
    df = df.sort_values('start')
    df = df.drop(['start', 'capacity'], axis=1)
    return df

# Получить коды всех продуктов на всех маркетплейсах
def get_product_codes_all():
    cookies_codes = {
        'metabase.DEVICE': '154ee79b-96f7-4af8-a82a-0ecbe3f608c8',
        '_ym_uid': '1655094110346037389',
        '_ym_d': '1655094110',
        '_fw_crm_v': 'adc348b5-c953-443a-bb89-0755c124ce89',
        '__ddg1_': 'xeGIzTrgFAS6pXJb5hM9',
        'ajs_user_id': '32708',
        '__stripe_mid': '3bf3ca97-3eef-4dd4-968c-624b4bb5b986e852db',
        '_gcl_au': '1.1.2127845954.1659520533',
        '_gaexp': 'GAX1.2.VNxeiAz4Ts2HQRh10ilPGw.19293.0',
        'ajs_anonymous_id': '1ee568-c345-da1f-3f6a-a7ab2d7a56e5',
        '_ga_F7SJXGRF0F': 'GS1.1.1659592255.13.0.1659592255.60',
        'metabase.SESSION': 'e2078f80-164b-41be-baa1-b2b874da556f',
        'csrftoken': 'DIHpHsyu5qm0b9q38KY6d3xFQJ0jMTuDD0PhUohWlsfNo31GUSRbyHstEss7UMlf',
        'sessionid': '3px5a0nammdcer6gqd4k8l2wveak007g',
        '_gid': 'GA1.2.94658197.1659942948',
        '_ga': 'GA1.2.1725221649.1655094112',
        '_ga_K8FNCP0QXZ': 'GS1.1.1659942947.1.1.1659943447.0',
    }
    headers_codes = {
        'authority': 'meta.wegotrip.com',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Yandex";v="92"',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 YaBrowser/21.8.0.1967 (beta) Yowser/2.5 Safari/537.36',
        'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
        'accept': '*/*',
        'origin': 'https://meta.wegotrip.com',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://meta.wegotrip.com/model/442-tablica-turov-i-ih-kodov-na-mp',
        'accept-language': 'ru,en;q=0.9',
    }
    data = {
        'parameters': '',
    }
    response = requests.post('https://meta.wegotrip.com/api/card/442/query/json', cookies=cookies_codes, headers=headers_codes, data=data)
    df = pd.json_normalize(response.json())
    df.columns = ['id', 'marketplace_id', 'product_id', 'marketplace','product_name', 'product_code', 'ticket_id', 'is_published']
    return df

# Получить коды конкретного продукта по id на всех маркетплейсах
def get_product_codes_one_product(product_id):
    cookies = {
    'metabase.DEVICE': '154ee79b-96f7-4af8-a82a-0ecbe3f608c8',
    '_ym_uid': '1655094110346037389',
    '_ym_d': '1655094110',
    '_fw_crm_v': 'adc348b5-c953-443a-bb89-0755c124ce89',
    '__ddg1_': 'xeGIzTrgFAS6pXJb5hM9',
    'ajs_user_id': '32708',
    '__stripe_mid': '3bf3ca97-3eef-4dd4-968c-624b4bb5b986e852db',
    '_gcl_au': '1.1.2127845954.1659520533',
    '_gaexp': 'GAX1.2.VNxeiAz4Ts2HQRh10ilPGw.19293.0',
    'ajs_anonymous_id': '1ee568-c345-da1f-3f6a-a7ab2d7a56e5',
    '_ga_F7SJXGRF0F': 'GS1.1.1659592255.13.0.1659592255.60',
    'metabase.SESSION': 'e2078f80-164b-41be-baa1-b2b874da556f',
    'csrftoken': 'DIHpHsyu5qm0b9q38KY6d3xFQJ0jMTuDD0PhUohWlsfNo31GUSRbyHstEss7UMlf',
    'sessionid': '3px5a0nammdcer6gqd4k8l2wveak007g',
    '_ga': 'GA1.2.1725221649.1655094112',
    '_ga_K8FNCP0QXZ': 'GS1.1.1660297130.6.1.1660297956.0',
    }
    headers = {
        'authority': 'meta.wegotrip.com',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Yandex";v="92"',
        'accept': 'application/json',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 YaBrowser/21.8.0.1967 (beta) Yowser/2.5 Safari/537.36',
        'origin': 'https://meta.wegotrip.com',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://meta.wegotrip.com/question',
        'accept-language': 'ru,en;q=0.9',
    }
    json_data = {
        'query': {
            'source-table': 'card__442',
            'filter': [
                '=',
                [
                    'field',
                    1643,
                    None,
                ],
                product_id,
            ],
        },
        'database': 2,
        'type': 'query',
        'parameters': [],
    }
    response = requests.post('https://meta.wegotrip.com/api/dataset', cookies=cookies, headers=headers, json=json_data)
    columns_name_easy = ['id', 'product_code', 'marketplace_id', 'product_id', 'is_published', 'product_name', 'ticket_id', 'marketplace']

    return pd.DataFrame(response.json()['data']['rows'], columns=columns_name_easy)

# Собрать данные с виатора для одного кода продукта
def collect_data_viator_one_product_code(product_code, product_id, product_name):
    df_availability = pd.DataFrame(columns=['marketplace', 'product_id', 'product_name', 'product_code', 'date', 'time', 'is_available'])
    dates = [datetime.now() + timedelta(i) for i in range(31)]
    cookies_viator = {
        'x-viator-tapersistentcookie': 'c827ed15-b943-4e9a-9c44-3f0b1540a3df',
        'x-viator-tapersistentcookie-xs': 'c827ed15-b943-4e9a-9c44-3f0b1540a3df',
        '_gcl_au': '1.1.511014316.1654666327',
        '_ga': 'GA1.2.1821282722.1655094375',
        '_ga_NSNYDWC6SG': 'GS1.1.1656918397.2.1.1656918651.0',
        '_cc': 'AdggGWPJunsmVzLmmuBcviUS',
        'g_state': '{"i_p":1659681022759,"i_l":4}',
        'LAST_TOUCH_SEM_MCID': '42385',
        'SEM_PARAMS': '%7B%7D',
        'SEM_MCID': '42385',
        'EXTERNAL_SESSION_ID': '',
        'XSRF-TOKEN': '65421e11-bb33-405a-a28c-cb562e10d8f2',
        'ORION_SESSION': 'Ia%2FLKpiOsTVTSs3h2%2BZxhQ%3D%3D%7CrOtIBdTQvG7MZd1UhygblQ5DnYlQaedyPr8hB6BZjMQQyXPWjTnwCsHZQjYH4vNVGxfkgN8nJj9wlchL85%2FDB9x9KfZmjYgwzIy58gX3r6x3VSTJ3sxfs6d%2BEMgZFPiFce%2BA3lc8u%2B%2Fwb2m3EN2tjFbWWXkYOtP9MCSIKHp2ZdzYd%2FEr%2BTZ%2FYipfJTUvRGZdYPNre49dDcGUu1anQwilSeebXRUl0fHW29clCyn7Yjl5wx0l3yAq2vnGcZryKfd08v5ClM3BZzwTHWeTzrJPnbbqYnAHIO1PYZKGLtXU6zuN2CtJNv00MfwrCRdt8g2JluWyZrrxi%2Fp76dWjWBmyXRztkonnbfEXApjlVMHJhZ3yhTON1lXO%2BXT5RFDxrxlkmLL%2BL09p8bqKwTc%2B2rdmAL%2F6FI5FfvmbZJvO%2BI4g7OeWo6x8PKf7R7jeZPhH7rrQ9PNwaYiS%2FBBUYj3ui3aMaGogctj%2BOuHWTAsf8mEplftCrku5v7kxVxMVdhMWZcLaGs3qII%2BU4TlMWSzybzYqB0YiRtW7cGUK%2FO0K2bGzIv2MLJWJKZ27t3L2Lp3HTayU8iKawTHL1aC%2BWDmNdVdHv4irhxgXCHecalsPz0IDDNtk7FgIholPFkpTdGLYInSFNU1S3O3gWbEhB6afmT6UlDoBGC%2B1B1WEhG4qbQQESv9KRIYjggoOr0E3bfoqLi6FBnQnl9U2TGp5jp8tB3jZTIWEUQVWXlVka5OCVvBQ%2Fw29TkfmoOxBdS2UcIiK26BAvezVG%2F%2FwXR8ntsqWRjV3Wu9buPFCzWSah8duxlFxjXdsdeyOb3ZR4stETgqerBB2LEhSauImdALiKQ72cgCTiv67xRmEs79EF80%2BLcWT5ur2N61%2B5hXnKmrEa9f3elWf1zn4ZdhuRb8Xr%2BS9df%2BBe8GCfoNLSboLHoV%2FSoJbzcstSR%2BeYft3lUxcncYpnXuMmTHVOyEu%2Fnlroalwy6dzE3UtBAocWB79HAvJcLZJUQV5Y3FgN5sl1Ht0U5H561jH%2FPFCc5DLuH1he4CrPTumdH8xIdJJXOlhhFt7b9JopV7esH8aQWRi82LRI8vcjEXf3CusbWfcPjFNosi9ew%2B7D5HFNfB7jQ3MFku2oRLm10lC4UldPYlER7I1dhRgbCdxmPssMEJBxBB3HR2Bf2Q1I%2BvKnd3OJ3GVXuesWO1JWYaZB2Z6EVXfu0Z9Ct5UjWl9Q%2Fjchlqn3SbC%2BZiRp5fvk%2BcQAGuGFkfgRe%2B72LudYJBeVcBvBMvZ%2BHUsLxB49bTcyGb8GIEDMP0ldkTHnYvfxzeVPmHjwj89U3K9h5RHfqL4VdUps6ct4uaQvT0p0xTJgacOd6DQgiuJGDwZiU7OxL9yb9kdRSadaIPk1MYAldrC8bAc8MFMU89lJC9ZWqNC3vCBQYsgbZ%2BbeEdv5eLsbcC4G8LiLwlmviTrNUuO%2Fe67lOYxZqD2JMOCur1uvp7av7JBKcffQkjA7kBTooNKfl0%2FnYHcNcYlbnji3THrtmaWJ%2F1pFcjQruzCkEVWvjh%2F2Ckqq%2B34f%2FPQbjWpH2LtY1lZEF38YSO0Eby%2FOjb06uj2XAOjzOKARfTisdy8KnMdEYgY5qPokYMYo6o0LUPyTZURgWARdwMubfFJwD7Ljx7MZd9yy3wdvf45GCELgtmgZWmENwz3o24P1A5n4nTz2yHS%2Fy2XmPSfUV%2BywDmTlbdQCrSeo48QnOi%2BlF6SKgN9z2ZkxZhSOKb30FaF0%2BMXxWAtRqzYuwVfcWSu4Z4u1yQ6wM1GJpdYXtQ%2FlTeXx0eb6zVxEzawFLZPO1qXfNQc3UWcMZ50qUVDnf3qlKh07oJv36qhmv6MOjhhOGRAO%2BYkL6G3AGh3E3CDaIxmX2n41oChxnGcaq8MB%2B%2FhUFld4qBXF4%2FZhMMCl%2BjIJuXSMazoEZhNKH%2BodvWlENesVz0XDc0bfiK4Fi9JYCqykE3v%2Bbc7Yg81mfkowV0miCOAKrfrchiHpBfJcz%2BXzEmENTiNE1VyYyfDln%2B2AK7NZWfNmtpC62tTp%2Fyf57nPQIdGKWx9Rjjver4fXtb2%2BNK4Mbsmu3P27KaeMOYH4WnakCH3KlSKCJnqDXLpgAuZ1jMh5pReojISX%2BhBkE%2BES6VTDhrH4l6y6E0mLsqu0xOp9Him7Kmc9ikgcigfp9UhU%2Bm%2F4Xp6A%2BclMUILIjGlUNi%2FSyACkWyK%2BfEXoFP%2Fs6Z%2BDUmpmrbbRFfKc5o1O21icoyBTvjVPqsLF7iE%2FZcXNYgAneN1TlQyBjezrTy2WxrHpR%2FiitrdvPXr1CAR6zsYTfaPFVJsVSIagYzcK%2FRKEXCkXiPByCX6s09fJYZPD1GO3Zw7n1R7GgqYzD2k3PGh4O93Ip3tp8SBrAnVlyS1lYQSZsPF%2FsQH1HWaTo6b8VTgLQ1h%2BdI6gmQrs%2FOb5R223u2Wi%2F5PJjj2dpt6otM1pXqIfZWjavtHnr7dnTCbyvLapq22truSJBAkuWAIplh5Kc2ifISB%2FxNnbRFUZwvOpdXOi5oZnIqKTQa1lYzXezg27JSOnESpWHNtWdZ9kK0IwcLMhA8dcrCy2JB0a1QvLo613i5GuAZynMpA4TzVmr6ReaM1bfykQhI%3D%7CMTct2IbgOuk%3D%3AzVpZGJJG0%2FdYFfllSuqwSCVEuZcurDaWSpGTzLaHQBs%3D',
        'ORION_SESSION_REQ': '17DADF09%3AD440_0A280CD5%3A01BB_62D66F83_34D1081%3A798A%7C17244227%3AEE99_0A280CD5%3A01BB_62D66F7D_34CFF35%3A798A%7C17DADF09%3AD440_0A280CD5%3A01BB_62D66F83_34D1081%3A798A',
        'REFERER_PAGE_REQUEST_ID': '17DADF09:D440_0A280CD5:01BB_62D66F83_34D1081:798A',
    }
    headers_viator = {
        'authority': 'www.viator.com',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Yandex";v="92"',
        'x-xsrf-token': '65421e11-bb33-405a-a28c-cb562e10d8f2',
        'traceparent': '00-64ef87bc4f05bd8c4ec10470ac44e819-6098929b6bc0a354-00',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 YaBrowser/21.8.0.1967 (beta) Yowser/2.5 Safari/537.36',
        'accept': 'application/json, text/plain, */*',
        'x-requested-with': 'XMLHttpRequest',
        'origin': 'https://www.viator.com',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'accept-language': 'ru,en;q=0.9',
    }
    all_requests_viator = [grequests.post('https://www.viator.com/orion/ajax/product-availability', cookies=cookies_viator, headers=headers_viator, json={'productCode':product_code,'searchDate': str(date.date()),'ageBands': {'ADULT': '2'}}) for date in dates]
    grequests_mapped = grequests.map(all_requests_viator)
    for response, date in zip(grequests_mapped, dates):
        if response.status_code != 200:
            return response.content
        for i in response.json()['productAvailability']['tourGrades']:
            if i['availability'] == 'AVAILABLE':
                if i['startTimes']:
                    for j in i['startTimes']:
                        is_available = True if j['availabilityResult'] == 'AVAILABLE' else False
                        df_availability.loc[len(df_availability)] = ['viator', product_id, product_name, product_code, date.date(), j['startTime'], is_available]
                else:
                    df_availability.loc[len(df_availability)] = ['viator', product_id, product_name, product_code, date.date(), None, True]
            else:
                df_availability.loc[len(df_availability)] = ['viator', product_id, product_name, product_code, date.date(), None, False]

    df_availability['time'] =  pd.to_datetime(df_availability['time']).dt.time
    df_availability['date'] =  pd.to_datetime(df_availability['date']).dt.date
    return df_availability

# Собрать данные с мьюзмента для одного кода продукта
def collect_data_musement_one_product_code(product_code, product_id, product_name):
    df_availability = pd.DataFrame(columns=['marketplace', 'product_id', 'product_name', 'product_code', 'date', 'time', 'is_available'])
    dates = [datetime.now() + timedelta(i) for i in range(31)]
    all_requests_musement = [grequests.get('https://api.musement.com/api/v3/activities/' + product_code + '/dates/' + str(date.date())) for date in dates]
    grequests_mapped = grequests.map(all_requests_musement)
    for response, date in zip(grequests_mapped, dates):
        if response.status_code != 200:
            if '1402' in response.text or '1410' in response.text or '1440' in response.text:
                df_availability.loc[len(df_availability)] = ['musement', product_id, product_name, product_code, date.date(), None, False]
            else:
                return response.content
        for i in response.json():
            if type(i) == dict: # если есть доступное время
                for j in i['groups']:
                    for k in j['slots']:
                        if k['time'] == '00:00':
                            df_availability.loc[len(df_availability)] = ['musement', product_id, product_name, product_code, date.date(), None, True]
                        else:
                            df_availability.loc[len(df_availability)] = ['musement', product_id, product_name, product_code, date.date(), k['time'], True]
            else:
                df_availability.loc[len(df_availability)] = ['musement', product_id, product_name, product_code, date.date(), None, False]
    df_availability['time'] =  pd.to_datetime(df_availability['time']).dt.time
    df_availability['date'] =  pd.to_datetime(df_availability['date']).dt.date
    return df_availability

# Собрать данные с спутинка для одного кода продукта
def collect_data_sputnik8_one_product_code(product_code, product_id, product_name, ticket_id):
    df_availability = pd.DataFrame(columns=['marketplace', 'product_id', 'product_name', 'product_code', 'date', 'time', 'is_available'])
    dates = [datetime.now() + timedelta(i) for i in range(31)]
    response = requests.get('https://api.sputnik8.com/v1/products/'+str(product_code)+'?api_key=9bc84ec26f47bf3005dc55434b4b796a&username=partners+tpo50@sputnik8.com')
    if response.status_code != 200:
        return response.content
    if ticket_id:
        try:
            for i in r.json()['last_events']:
                df_availability.loc[len(df_availability)] = ['sputnik8', product_id, product_name, product_code, i['date'], i['time'], True]
        except:
            pass
    else:
        for i in r.json()['last_events']:
            df_availability.loc[len(df_availability)] = ['sputnik8', product_id, product_name, product_code, i['date'], None, True]
            break
    df_availability['time'] =  pd.to_datetime(df_availability['time']).dt.time
    df_availability['date'] =  pd.to_datetime(df_availability['date']).dt.date
    return df_availability


# Соединить истинные данные для одного продукта ПО ТАЙМСЛОТАМ с собранными и вернуть ошибки, если есть
def match_data_one_product_timeslots(true_data, list_df_availability):
    only_dataframes_res = [result for result in list_df_availability if isinstance(result, pd.DataFrame)]
    temp = [true_data[['start_date', 'start_time', 'available']]]
    name_product_codes = []
    errors = [result for result in list_df_availability if isinstance(result, pd.DataFrame) == False]
    for df_res in only_dataframes_res:
        if df_res.empty != True:
            name = df_res['marketplace'].unique()[0]
            product_code = df_res['product_code'].unique()[0]
            name_product_codes.append((name, product_code))
            temp.append(true_data[['start_date', 'start_time', 'available']].merge(df_res[['date', 'time', 'is_available']], indicator = True, how='outer', left_on=['start_date', 'start_time', 'available'], right_on=['date', 'time', 'is_available']).add_prefix(f'{name}_{product_code}_'))
        #errors.append('Пустой датафрейм')
    df = pd.concat(temp, axis=1)
    only_true_time = df[~df['start_date'].isnull()] # только то что есть в истинных значениях
    for name, product_code in name_product_codes:
        only_true_time[f'{name}_{product_code}_is_available'] = only_true_time[f'{name}_{product_code}_is_available'].apply(lambda x: x if x == x else False)
    columns_availability = only_true_time.columns[(only_true_time.columns.str.contains('is_available'))]
    only_true_time = only_true_time[['start_date', 'start_time', 'available'] + list(columns_availability)]

    doesnt_exists_in_true_list = []
    doesnt_exists_in_true = df[df['start_date'].isnull()]
    for name, product_code in name_product_codes:
        new_df = pd.DataFrame()
        new_df['start_date'] = doesnt_exists_in_true[f'{name}_{product_code}_date']
        new_df['start_time'] = doesnt_exists_in_true[f'{name}_{product_code}_time']
        new_df['available'] = doesnt_exists_in_true['available'].fillna(False)
        new_df[f'{name}_{product_code}_is_available'] = doesnt_exists_in_true[f'{name}_{product_code}_is_available']
        new_df = new_df[new_df['start_date'].notna()]
        new_df = new_df[(~new_df['start_date'].isin(only_true_time['start_date'])) & (new_df[f'{name}_{product_code}_is_available'] == True)]
        doesnt_exists_in_true_list.append(new_df)

    if len(doesnt_exists_in_true_list) > 0:
        exclude = doesnt_exists_in_true_list[0]  # то чего нет в истинных значениях
        for i in doesnt_exists_in_true_list[1:]:
            exclude = exclude.merge(i, how='outer')
        result = only_true_time.merge(exclude, how='outer')
    else:
        result = only_true_time

    # Вывести только то, что не совпадает
    for name, product_code in name_product_codes:
        result = result[result[f'{name}_{product_code}_is_available'] != result['available']]
    result = result[result['start_date'] != result['start_date'].values[-1]]
    return result, errors

# Соединить истинные данные для одного продукта ПО ТАЙМСЛОТАМ с собранными и вернуть ошибки, если есть
def match_data_one_product_date_only(true_data, list_df_availability):
    only_dataframes_res = [result for result in list_df_availability if isinstance(result, pd.DataFrame)]
    temp = [true_data[['start_date', 'start_time', 'available']]]
    name_product_codes = []
    errors = [result for result in list_df_availability if isinstance(result, pd.DataFrame) == False]
    for df_res in only_dataframes_res:
        if df_res.empty != True:
            name = df_res['marketplace'].unique()[0]
            product_code = df_res['product_code'].unique()[0]
            name_product_codes.append((name, product_code))
            temp.append(true_data[['start_date', 'available']].merge(df_res[['date', 'is_available']], indicator = True, how='outer', left_on=['start_date', 'available'], right_on=['date', 'is_available']).add_prefix(f'{name}_{product_code}_'))
        #errors.append('Пустой датафрейм')
    df = pd.concat(temp, axis=1)

    only_true_time = df[~df['start_date'].isnull()] # только то что есть в истинных значениях
    for name, product_code in name_product_codes:
        only_true_time[f'{name}_{product_code}_is_available'] = only_true_time[f'{name}_{product_code}_is_available'].apply(lambda x: x if x == x else False)
    columns_availability = only_true_time.columns[(only_true_time.columns.str.contains('is_available'))]
    only_true_time = only_true_time[['start_date', 'start_time', 'available'] + list(columns_availability)]

    doesnt_exists_in_true_list = []
    doesnt_exists_in_true = df[df['start_date'].isnull()]
    for name, product_code in name_product_codes:
        new_df = pd.DataFrame()
        new_df['start_date'] = doesnt_exists_in_true[f'{name}_{product_code}_date']
        #new_df['start_time'] = doesnt_exists_in_true[f'{name}_{product_code}_time']
        new_df['available'] = doesnt_exists_in_true['available'].fillna(False)
        new_df[f'{name}_{product_code}_is_available'] = doesnt_exists_in_true[f'{name}_{product_code}_is_available']
        new_df = new_df[new_df['start_date'].notna()]
        new_df = new_df[(~new_df['start_date'].isin(only_true_time['start_date'])) & (new_df[f'{name}_{product_code}_is_available'] == True)]
        doesnt_exists_in_true_list.append(new_df)

    if len(doesnt_exists_in_true_list) > 0:
        exclude = doesnt_exists_in_true_list[0]  # то чего нет в истинных значениях
        for i in doesnt_exists_in_true_list[1:]:
            exclude = exclude.merge(i, how='outer')
        result = only_true_time.merge(exclude, how='outer')
    else:
        result = only_true_time

    # Вывести только то, что не совпадает
    for name, product_code in name_product_codes:
        result = result[result[f'{name}_{product_code}_is_available'] != result['available']]
    result = result[result['start_date'] != result['start_date'].values[-1]]
    return result, errors

# Получить полностью готовый результат для одного продукта
def get_result_one_product(product_id):
    #get_true_data_all().to_csv('all.csv')
    true_data_df = get_true_data_one_product(product_id) # истинные данные
    product_codes_df = get_product_codes_one_product(product_id) # данные кодов продукта для маркетплейсов
    product_codes_df = product_codes_df[product_codes_df['marketplace'].isin(['Viator', 'Sputnik8', 'Musement'])] # Пока ограничиваемся тремя маркетплейсами

    list_df_availability = [] # список с датафреймами с доступностью для каждого кода продукта или с ошибками

    # для каждого кода продукта
    for index, row in  product_codes_df.iterrows():
        if row['marketplace'] == 'Viator':
            list_df_availability.append(collect_data_viator_one_product_code(row['product_code'], row['product_id'], row['product_name']))
        if row['marketplace'] == 'Musement':
            list_df_availability.append(collect_data_musement_one_product_code(row['product_code'], row['product_id'], row['product_name']))
        if row['marketplace'] == 'Sputnik8':
            list_df_availability.append(collect_data_sputnik8_one_product_code(row['product_code'], row['product_id'], row['product_name'], row['ticket_id']))

    if true_data_df['booking_type'].unique()[0] == 'DATE_AND_TIME':
        result, errors = match_data_one_product_timeslots(true_data_df, list_df_availability)
    else:
        result, errors = match_data_one_product_date_only(true_data_df, list_df_availability)
    return (result, errors)

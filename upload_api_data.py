import os
import json
from datetime import datetime, date, timedelta
import requests
from urllib.parse import urljoin
import csv


from hdfs import InsecureClient # library docs https://hdfscli.readthedocs.io/en/latest/index.html


def _get_date_list(start_date: str, end_date: str):
    # Format: 'yyyy-mm-dd' "

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    return [start + timedelta(days=x) for x in range(0, (end - start).days)]


def upload_api_data():

    client = InsecureClient(f'http://127.0.0.1:50070/', user='user')

    api = {
        'creds': {
            'username': 'rd_dreams',
            'password': 'djT6LasE',
        },
        'url': "https://robot-dreams-de-api.herokuapp.com",
        'headers_auth':
            {
                'content-type': "application/json"
            },
        'headers_get':
            {
                'authorization': "",
                'content-type': "application/json"
            },
    }

    url_auth = urljoin(api['url'], "/auth")

    api_jwt = requests.post(
                        url_auth,
                        headers=api["headers_auth"],
                        data=json.dumps(api["creds"]
                    )
                ).json().get("access_token")

    api["headers_get"]["authorization"] = "JWT {}".format(api_jwt)

    url_get = api['url'] + "/out_of_stock"

    _headers = {
        'authorization': 'JWT {}'.format(api_jwt),
        'content-type': "application/json"
    }

    dates = _get_date_list(
        '2021-12-10',
        '2021-12-14'
    )

    for date in dates:

        loading_date = {
            'date': date.strftime("%Y-%m-%d")
        }
        response = requests.get(
                        url_get,
                        headers=_headers,
                        data=json.dumps(loading_date),
                        timeout=5
                    )

        response.raise_for_status()

        result = response.json()

        # dir_name = os.path.join(
        #                 '/Users/imac/Projects/data_engineering/upload_hdfs/api_data/' +
        #                 date.strftime("%Y-%m-%d")
        #             )
        # os.makedirs(dir_name, exist_ok=True)

        dir_name = os.path.join('/bronze/from_api/' + loading_date['date'])
        client.makedirs(dir_name)

        file_name = os.path.join(dir_name, loading_date['date']+'.json')

        with client.write(file_name, encoding='utf-8') as f:
            json.dump(result, f)

        print(f"Wrote data to file {file_name}")


if __name__ == '__main__':
    upload_api_data()


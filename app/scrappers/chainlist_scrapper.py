import requests
import pandas as pd
from bs4 import BeautifulSoup

url = 'https://chainlist.org/'
response = requests.get(url)

soup = BeautifulSoup(response.text, "html.parser")


def prepare_chainlist():
    print(response)

    chainlist = soup.find('div', {'class': lambda value: value and value.startswith("grid gap-5 grid-cols-1")})
    chainlist_to_df = []

    for child in chainlist:
        chainlist_to_df.append([
            child.a.span.text,
            child.table.tbody.tr.findChildren('td')[0].text,
            child.table.tbody.tr.findChildren('td')[1].text
        ])

    df = pd.DataFrame(chainlist_to_df, columns=['chain_name', 'chain_id', 'chain_symbol'])
    df = df.set_index('chain_id')
    df.to_csv('chainlist.csv', sep='\t')
    print(df)

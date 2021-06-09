"""
Scrapes player position-level percentile data as per fbref scouting pages
(e.g. https://fbref.com/en/players/e46012d4/scout/365_euro/Kevin-De-Bruyne-Scouting-Report)
Saves json file to fpl_bilderberg/data/fbref/scouting_reports.json with following format
{
...
player_id:
    "name": player name,
    "positions": list of player positions,
    "mins": # of minutes played by player,
    "team": team they play for,
    "nation": player nationality,
    "league": league they play in,
    "age": player's age,
    "profile": link to fbref profile page,
    "data":{
        "Goals": {
            "Per 90": goals per 90,
            "Percentile": {
                "player position #1, e.g. CB": goal percentile among CBs,
                "player position #2, e.g. FB": goal percentile among FBs
                ...
            }
        },
        "Assists": {
            "Per 90": assists per 90,
            "Percentile": {
                "player position #1, e.g. CB": assist percentile among CBs,
                "player position #2, e.g. FB": assist percentile among FBs
                ...
            }
        },
        ...
    },
...
}
"""

import os
import json

import pandas as pd
import requests
from bs4 import BeautifulSoup
from progress.bar import IncrementalBar


def get_scouting_report(idx, name):
    """
    :param idx: fbref id of player
    :param name: name of player as appears in last part of their profile url
    :return: dict containing percentile data, relevant positions, and their id
    """
    # Get player'spositions from page source
    url = f'https://fbref.com/en/players/{idx}/scout/365_euro/{name}-Scouting-Report'
    req = requests. get(url, 'html.parser')
    pos_list = ['GK', 'CB', 'FB', 'MF', 'AM', 'FW']
    soup = BeautifulSoup(req.text, 'html.parser')
    pos = [p for p in pos_list if soup.find_all('div', {"id": f"div_scout_full_{p}"})]
    # Get tables containing percentile data
    dfs = pd.read_html(req.text)
    percentile_dfs = []
    for df in dfs:
        if isinstance(df.columns, pd.MultiIndex):   # Filter allows only percentile tables
            df.columns = df.columns.droplevel()
            df = df.drop_duplicates().dropna()
            df = df.set_index('Statistic')
            df = df[(df['Percentile'].str.isdigit()) & (df.index != 'Passes Blocked')]
            df.index.name = None
            df = df.T.to_json()
            percentile_dfs.append(df)
    # Create complete percentile data json from each position table
    data = {}
    for stat in (df := json.loads(percentile_dfs[0])):
        data[stat] = df[stat]
        data[stat]['Percentile'] = {}
        for i, p in enumerate(pos):
            data[stat]['Percentile'][p] = json.loads(percentile_dfs[i])[stat]['Percentile']
    return {
        'data': data,
        'positions': pos,
        'id': idx
    }


if __name__ == "__main__":
    # Get hrefs to player pages in big 5 leagues
    big_5_url = 'https://fbref.com/en/comps/Big5/stats/players/Big-5-European-Leagues-Stats'
    req = requests. get(big_5_url, 'html.parser')
    soup = BeautifulSoup(req.text, 'html.parser')
    table = soup.find('table')
    links = [link['href'] for link in table.findAll('a')]
    hrefs = links[0:len(links):5]

    # Create df containing key info for each player and add hrefs to df
    df = pd.read_html(req.text)[0]
    df.columns = df.columns.droplevel()
    df = df.drop(df[df['Rk'] == 'Rk'].index)
    df['href'] = hrefs
    df = df.drop_duplicates(subset=['Player', 'Age']).set_index('Rk')
    df.index.name = None

    reports = {}
    for idx in IncrementalBar('Collecting Scouting Reports').iter(df.index):
        try:
            href = df.loc[idx, 'href']
            r = get_scouting_report(*href.split('/')[-2:])
            # Add player level + percentile data to reports dict
            reports[r['id']] = {
                'name': df.loc[idx, 'Player'],
                'positions': r['positions'],
                'mins': df.loc[idx, 'Min'],
                'team': df.loc[idx, 'Squad'],
                'nation': df.loc[idx, 'Nation'].split(' ')[-1],
                'league': ' '.join(df.loc[idx, 'Comp'].split(' ')[1:]),
                'age': df.loc[idx, 'Age'],
                'profile': 'https://fbref.com' + df.loc[idx, 'href'],
                'data': r['data']
            }
        except ValueError:  # Filters out players who don't have enough minutes for percentile data
            pass

    with open('scouting_reports.json', 'w') as outfile:
        json.dump(reports, outfile)

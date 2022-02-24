import os, requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import pymongo
 

def getTable(soupInstance, id):
    '''
    Parse table by ID using BeautifulSoup
  
    Extended description of function.
  
    Parameters:
    soupInstance (BeautifulSoup): instance of bs4 request
  
    Returns:
    list: list containing rows of table
    '''

    table = soupInstance.find('table', attrs={'id':id})
    table_body = table.find('tbody')
    rows = table_body.find_all('tr')

    data = []
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append(cols)
        # data.append([ele for ele in cols if ele]) # Get rid of empty

    return data


def getPortfolioAllocation(soupInstance):
    '''
    Parse 'portfolioAllocation' table
    
    Parameters:
    soupInstance (BeautifulSoup): instance of bs4 request
  
    Returns:
    pandas.DataFrame: list containing rows of table
    '''
    table = getTable(soupInstance, 'portfolioAllocation')

    return pd.DataFrame(table, columns=['Weight','Ticker','ETF Name','Investment Themes'])


def getComponentsReturnsTable(soupInstance):
    '''
    Parse 'componentsReturnsTable' table
    
    Parameters:
    soupInstance (BeautifulSoup): instance of bs4 request
  
    Returns:
    pandas.DataFrame: list containing rows of table
    '''
    table = getTable(soupInstance, 'componentsReturnsTable')

    data = []
    for row in table:
        if 'Components' in row:
            break
        else:
            data.append(row)

    return pd.DataFrame(data, columns=['Name',datetime.now().strftime('%b %Y'),'1M','6M','1Y','5Y','10Y','25Y'])


def getHistoricalReturnsTable(soupInstance):
    '''
    Parse 'historicalReturnsTable' table
    
    Parameters:
    soupInstance (BeautifulSoup): instance of bs4 request
  
    Returns:
    pandas.DataFrame: list containing rows of table
    '''
    table = getTable(soupInstance, 'historicalReturnsTable')

    data = []
    for row in table:
        if 'Components' in row:
            break
        else:
            data.append([ele.split('\n')[0].replace('\t','') for ele in row])

    return pd.DataFrame(data, columns=['Period','Return','Return (Inflation Adjusted)','Standard Deviation','Max Drawdown','Months (Pos - Neg)'])


def getPortfolioRatingSummary(soupInstance):
    '''
    Parse 'portfolioRatingSummary' table
    
    Parameters:
    soupInstance (BeautifulSoup): instance of bs4 request
  
    Returns:
    pandas.DataFrame: list containing rows of table
    '''
    table = getTable(soupInstance, 'portfolioRatingSummary')

    data = []
    for row in table:
        if 'Rating assigned considering all the Low Risk Portfolios' in row:
            break
        else:
            cols = []
            for ele in [ele.split('\n')[0].replace('\t','') for ele in row]:
                cols.extend([subele.strip().replace('+','') for subele in ele.split(':')])

            data.append(cols)

    df = pd.DataFrame(data, columns=['Category','Grade','Low Risk Portfolios Grade', 'Low Risk Portfolios Rating', 'All Portfolios Grade', 'All Portfolios Rating'])

    return df[df.Category.str.len() > 0]


if __name__ == "__main__":

    # CREATE CONNECTION TO DATABASE
    client = pymongo.MongoClient(f"mongodb+srv://{os.environ['USERNAME']}:{os.environ['PASSWORD']}@{os.environ['HOST']}")
    db = client.lazyportfolioetf

    # SEND REQUEST FOR HOMEPAGE HTML
    url = 'http://www.lazyportfolioetf.com/allocation/'
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'html.parser')
 
    # GET ALL UNIQUE PORTFOLIO URLS FROM SITE HOMEPAGE
    urls = set([])
    for link in soup.find_all('a'):
    
        url = link.get('href')        

        if 'http://www.lazyportfolioetf.com/allocation/' in url and url != 'http://www.lazyportfolioetf.com/allocation/':
            urls.add(url)

    # SCRAPE DATA FROM ALL UNIQUE PORTFOLIO URLS
    # TODO: FINISH DB INSERT FOR ALL FUNCTIONS
    for i, url in enumerate(sorted(urls)):
        if i == 1:
            reqs = requests.get(url)
            soup = BeautifulSoup(reqs.text, 'html.parser')

            df = getPortfolioRatingSummary(soup)

            print(df.to_dict())
            db.PortfolioRatingSummary.insert_one({'record':df.to_dict('r')})
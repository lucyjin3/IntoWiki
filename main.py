# %% load database

from mwsql import Dump
import pandas as pd
import networkx as nx
import time
import sys


# %% Helper Function to look for stuffs
def getPageId(df: pd.DataFrame, name: str) -> int:
    try:
        return (df.loc[df['title'].str.lower() == name.lower()].head(1)).iloc[0]['pageId']
    except IndexError:
        return None


def getPageTitle(df: pd.DataFrame, pageId: int) -> str:
    try:
        return df.loc[df['pageId'] == pageId].iloc[0]['title']
    except IndexError:
        return None


def getSCC(scc: set, pageId: int) -> set:
    for cs in scc:
        if pageId in cs:
            return cs
    return None


if __name__ == '__main__':
    # %% Data Preparation
    pagelinkDump = Dump.from_file('simplewiki-latest-pagelinks.sql.gz')
    pageDump = Dump.from_file('simplewiki-latest-page.sql.gz')

    pageLinkRows = pagelinkDump.rows(convert_dtypes=True)
    pageRows = pageDump.rows(convert_dtypes=True)
    data = []
    while True:
        try:
            data.append(next(pageLinkRows))
        except StopIteration:
            break

    pagelink_df = pd.DataFrame(data, columns=['fromPageId', 'toNamespace', 'toTitle', 'fromNamespace'])
    pagelink_df['fromPageId'] = pd.to_numeric(pagelink_df['fromPageId'])
    pagelink_df['toNamespace'] = pd.to_numeric(pagelink_df['toNamespace'])
    pagelink_df['fromNamespace'] = pd.to_numeric(pagelink_df['fromNamespace'])

    data.clear()
    while True:
        try:
            data.append(next(pageRows)[0:3])
        except StopIteration:
            break

    page_df = pd.DataFrame(data, columns=['pageId', 'namespace', 'title'])
    page_df['pageId'] = pd.to_numeric(page_df['pageId'])
    page_df['namespace'] = pd.to_numeric(page_df['namespace'])

    # %% Join pagelink with page id

    plftdf = pd.merge(pagelink_df, page_df, left_on=['fromPageId', 'fromNamespace'], right_on=['pageId', 'namespace'],
                      sort=True)
    plftdf = plftdf[['fromPageId', 'fromNamespace', 'title', 'toNamespace', 'toTitle']]
    plftdf.rename(columns={'title': 'fromTitle'}, inplace=True)
    data = pd.merge(plftdf, page_df, left_on=['toNamespace', 'toTitle'], right_on=['namespace', 'title'])
    data = data[['fromPageId', 'fromNamespace', 'fromTitle', 'pageId', 'toNamespace', 'toTitle']]
    data.rename(columns={'pageId': 'toPageId'}, inplace=True)
    G = nx.from_pandas_edgelist(data, 'fromPageId', 'toPageId', create_using=nx.DiGraph)

    # %% Algorithm
    tstart = time.time()
    tscc = nx.strongly_connected_components(G)  # tarjan algorithm
    ttime = time.time() - tstart

    kstart = time.time()
    kscc = nx.kosaraju_strongly_connected_components(G)
    ktime = time.time() - kstart


    # %% Console Interaction
    print("the execution time of kosaraju's algorithm is ", ktime)
    print("the execution time of tarjan's algorithm is ", ttime)

    print("\nThis function will return a list of all titles that are strongly connected components")
    print("type a term you want to search")
    print("type break or done when you want to exit this program")
    word: str = ""

    while True:
        word = input().rstrip()
        if word.lower() == "done" or word.lower() == "break":
            sys.exit(0)

        pageId = getPageId(page_df, word)
        if pageId is None:
            print("There is no article that has a title ", word, ".\nPick a new search term")
            continue

        scc = getSCC(kscc, pageId)
        if scc is None:
            print('The article exists in simplewiki, but the article does not contain pagelink')
            print('Pick a new search term')
            continue

        sccTitle = [getPageTitle(page_df, pid) for pid in scc]
        print('The strongly connected components of article ', word, ' is:')
        print(sccTitle, "\nSearch a new term, or type break or done to exit the program")

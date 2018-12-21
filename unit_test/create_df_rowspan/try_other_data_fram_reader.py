import pandas as pd
from bs4 import BeautifulSoup
from lxml.html import parse
from pandas.io.parsers import TextParser

def _unpack(row, kind='td'):
   elts = row.findall('.//%s' % kind)
   return [val.text_content() for val in elts]

def parse_options_data(table):
  rows = table.findall('.//tr')
  header = _unpack(rows[0], kind='th')
  data = [_unpack(r) for r in rows[1:]]
  return TextParser(data, names=header).get_chunk()

def read_html_other1(info):
    soup = BeautifulSoup(info)
    tables = soup.find_all('table')
    # tables = doc.findall('.//table')
    table = parse_options_data(tables[0])
    return table

if __name__ == '__main__':
    parsed = parse(urlopen(
        'http://www.bmfbovespa.com.br/en-us/intros/Limits-and-Haircuts-for-accepting-stocks-as-collateral.aspx?idioma=en-us'))
    doc = parsed.getroot()
    tables = doc.findall('.//table')
    table = parse_options_data(tables[0])
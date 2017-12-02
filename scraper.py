# coding: utf-8

import os
import dataset
import requests
from collections import OrderedDict
from tempfile import NamedTemporaryFile
from lxml import html
from openpyxl import load_workbook

URL = 'http://date.gov.md/ckan/en/dataset/11736-date-din-registrul-de-stat-al-unitatilor-de-drept-privind-intreprinderile-inregistrate-in-repu'  # noqa


def sheet_rows(book, name):
    sheet = book.get_sheet_by_name(name)
    headers = None
    for row in sheet.rows:
        row = [c.value for c in row]
        if headers is None:
            headers = []
            for header in row:
                if '(' in header:
                    header, _ = header.split('(')
                if '/' in header:
                    header, _ = header.split('/')
                header = header.replace(' ', '_')
                header = header.replace('.', '_')
                header = header.strip('_')
                headers.append(header)
            continue
        yield OrderedDict(zip(headers, row))


def subfield(row, field):
    value = row.pop(field, None)
    if value is None:
        return
    for item in value.split(', '):
        item = item.strip()
        if len(item):
            yield item


def load_file(file_name):
    book = load_workbook(file_name, read_only=True, data_only=True)
    db = dataset.connect('sqlite:///data.sqlite')

    unlicensed = {}
    for row in sheet_rows(book, 'Clasificare nelicentiate'):
        unlicensed[str(row.get('ID'))] = row

    licensed = {}
    for row in sheet_rows(book, 'Clasificare licentiate'):
        licensed[str(row.get('ID'))] = row

    table = db.get_table('data', primary_id=False)
    table.drop()
    unlicensed_table = db.get_table('unlicensed', primary_id=False)
    unlicensed_table.drop()
    licensed_table = db.get_table('licensed', primary_id=False)
    licensed_table.drop()
    directors_table = db.get_table('directors', primary_id=False)
    directors_table.drop()
    founders_table = db.get_table('founders', primary_id=False)
    founders_table.drop()
    for index, row in enumerate(sheet_rows(book, 'RSUD'), 1):
        row['id'] = index
        date = row.pop(u'Data_înregistrării')
        if date is not None:
            row[u'Data_înregistrării'] = date.date().isoformat()

        for item in subfield(row, 'Genuri_de_activitate_nelicentiate'):
            ctx = unlicensed.get(item)
            if ctx is None:
                continue
            ctx = dict(ctx)
            ctx['company_id'] = index
            unlicensed_table.insert(ctx)

        for item in subfield(row, 'Genuri_de_activitate_licentiate'):
            ctx = licensed.get(item)
            if ctx is None:
                continue
            ctx = dict(ctx)
            ctx['company_id'] = index
            licensed_table.insert(ctx)

        for item in subfield(row, 'Lista_fondatorilor'):
            founders_table.insert({
                'company_id': index,
                'name': item
            })

        for item in subfield(row, u'Lista_conducătorilor'):
            directors_table.insert({
                'company_id': index,
                'name': item
            })

        # print row
        table.insert(row)
        # print '> ', row.get(u'Denumirea_completă')


def fetch_latest():
    res = requests.get(URL)
    doc = html.fromstring(res.content)

    data_url = None
    for res in doc.findall('.//li[@class="resource-item"]'):
        for link in res.findall('.//a'):
            link = link.get('href')
            if link and link.lower().endswith('.xlsx'):
                data_url = link

    file_name = os.path.basename(data_url)
    with open(file_name, 'w') as fh:
        print "Downloading:", data_url
        res = requests.get(data_url, stream=True)
        for chunk in res.iter_content(8000):
            fh.write(chunk)

    load_file(file_name)


if __name__ == '__main__':
    fetch_latest()
    # load_file('/Users/fl/Downloads/company_20.11.17.xlsx')

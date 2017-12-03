# coding: utf-8

import os
import dataset
import requests
from unicodecsv import DictWriter
from collections import OrderedDict
# from tempfile import NamedTemporaryFile
from lxml import html
from openpyxl import load_workbook
from morphium import Archive


archive = Archive(bucket='archive.pudo.org', prefix='md-companies')
db = dataset.connect('sqlite:///data.sqlite')
companies_table = db.get_table('companies', primary_id=False)
unlicensed_table = db.get_table('unlicensed', primary_id=False)
licensed_table = db.get_table('licensed', primary_id=False)
directors_table = db.get_table('directors', primary_id=False)
founders_table = db.get_table('founders', primary_id=False)

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
    if isinstance(value, long):
        yield unicode(long)
        return
    for item in value.split(', '):
        item = item.strip()
        if len(item):
            yield item


def insert_row(index, row, unlicensed, licensed):
    row['id'] = index
    idno = row.get(u'IDNO')
    name = row.get(u'Denumirea_completă')
    if name is None:
        return
    date = row.pop(u'Data_înregistrării')
    if date is not None:
        row[u'Data_înregistrării'] = date.date().isoformat()

    base = {
        'Company_IDNO': idno,
        'Company_ID': index,
        'Company_Name': name
    }

    for item in subfield(row, 'Genuri_de_activitate_nelicentiate'):
        ctx = unlicensed.get(item)
        if ctx is None:
            continue
        ctx = dict(ctx)
        ctx.update(base)
        unlicensed_table.insert(ctx)

    for item in subfield(row, 'Genuri_de_activitate_licentiate'):
        ctx = licensed.get(item)
        if ctx is None:
            continue
        ctx = dict(ctx)
        ctx.update(base)
        licensed_table.insert(ctx)

    for item in subfield(row, 'Lista_fondatorilor'):
        data = base.copy()
        data['Founder'] = item
        founders_table.insert(data)

    for item in subfield(row, u'Lista_conducătorilor'):
        data = base.copy()
        data['Director'] = item
        directors_table.insert(data)

    companies_table.insert(row)
    # print index, row.get("IDNO"), name


def dump_csv(table, name):
    with open(name, 'w') as fh:
        writer = DictWriter(fh, fieldnames=table.columns)
        writer.writeheader()
        for row in table:
            writer.writerow(row)


def load_file(file_name):
    book = load_workbook(file_name, read_only=True, data_only=True)

    unlicensed = {}
    for row in sheet_rows(book, 'Clasificare nelicentiate'):
        unlicensed[str(row.get('ID'))] = row

    licensed = {}
    for row in sheet_rows(book, 'Clasificare licentiate'):
        licensed[str(row.get('ID'))] = row

    companies_table.drop()
    unlicensed_table.drop()
    licensed_table.drop()
    directors_table.drop()
    founders_table.drop()

    for index, row in enumerate(sheet_rows(book, 'RSUD'), 1):
        with db:
            insert_row(index, row, unlicensed, licensed)

    # send the results to an S3 bucket:
    meta = db.get_table('data')
    meta.drop()
    tables = ('licensed', 'unlicensed', 'founders', 'directors', 'companies')
    for table in tables:
        file_name = '%s.csv' % table
        print "Dump CSV:", file_name
        dump_csv(db[table], file_name)
        url = archive.upload_file(file_name)
        meta.upsert({
            'name': table,
            'file_name': file_name,
            'url': url
        }, ['name'])
        os.unlink(file_name)


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
    os.unlink(file_name)


if __name__ == '__main__':
    fetch_latest()
    # load_file('/Users/fl/Downloads/company_20.11.17.xlsx')

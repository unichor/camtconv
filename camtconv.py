#!/usr/bin/env python

from typing import List, Iterator

import csv
import tempfile
import zipfile
import re
import os
import locale
locale.setlocale(locale.LC_NUMERIC, 'de_DE.utf8')

import fintech
fintech.register()
from fintech.sepa import CAMTDocument

account_patterns = (
        (re.compile('\\b(PWE|Probenwochenende|Proben-WE|Ochsenhausen|vegetarisch)\\b', re.I), 'Probenwochenende'),
        (re.compile('PWE\\b'), 'Probenwochenende'),
        (re.compile('\\b(Chorreise|Reisebeitrag|Chorreisebeitrag)\\b', re.I), 'Chorreise'),
)

class Transaction:
    def __init__(self, date, subject, amount) -> None:
        self.date = date
        self.subject = subject
        self.amount = amount
        self.account = self._guess_account()

    @staticmethod
    def from_camt_transaction(camt) -> 'Transaction':
        subject = []
        if camt.name:
            subject.append(camt.name)
        if len(camt.purpose) > 0:
            subject.append(camt.purpose[0])
        subject = '\n'.join(subject)
        amount = camt.amount.value
        assert camt.amount.currency == 'EUR'
        return Transaction(camt.date, subject, amount)

    def _guess_account(self):
        for pattern, account in account_patterns:
            if pattern.search(self.subject):
                return account
        return 'Tagesgeschäft'

    def __str__(self):
        return f'Transaction[{self.date}, {self.subject}, {self.amount}, {self.account}]'


def from_camt_doc(xml: str) -> Iterator[Transaction]:
    doc = CAMTDocument(xml)
    for t in doc:
        yield Transaction.from_camt_transaction(t)

def from_camt_xmlfile(xmlfile: str) -> Iterator[Transaction]:
    with open(xmlfile) as fd:
        xml = fd.read()
        yield from from_camt_doc(xml)

def from_camt_zipfile(zipname: str) -> Iterator[Transaction]:
    zf = zipfile.ZipFile(zipname)
    tempdir = tempfile.mkdtemp(prefix='camtconv')
    zf.extractall(path=tempdir)
    filenames = os.listdir(tempdir)
    filenames.sort()
    for xml in filenames:
        xmlfile = os.path.join(tempdir, xml)
        yield from from_camt_xmlfile(xmlfile)
        os.unlink(xmlfile)
    os.rmdir(tempdir)

def is_zipfile(arg: str) -> bool:
    return arg.endswith('.zip') or arg.endswith('.ZIP')

def from_any_files(args: List[str]) -> Iterator[Transaction]:
    for arg in args:
        if is_zipfile(arg):
            yield from from_camt_zipfile(arg)
        else:
            raise ValueError("Don't know how to handle " + arg)

def to_csv(transactions: Iterator[Transaction], csvname: str) -> None:
    with open(csvname, 'w', newline='') as csvfile:
        fieldnames = ['Datum', 'Betreff', 'Betrag', 'Buchungsnummer', 'Semester', 'Kategorie', 'Unterkonto']
        out = csv.writer(csvfile, dialect=csv.unix_dialect)
        out.writerow(fieldnames)
        for t in transactions:
            date = t.date.strftime("%d.%m.%Y")
            amount = f"{t.amount:n}"
            out.writerow([date, t.subject, amount, '', '', '', t.account])

def main():
    import sys
    if len(sys.argv) == 2 and is_zipfile(sys.argv[1]):
        zipfile = sys.argv[1]
        inputs = [zipfile]
        output = zipfile[:-4] + '.csv'
    elif len(sys.argv) >= 3:
        inputs = sys.argv[1:-1]
        output = sys.argv[-1]
    else:
        raise Exception(f"Usage: {sys.argv[0]} zipfile(s) outfile")

    transactions = from_any_files(inputs)
    to_csv(transactions, output)

if __name__ == '__main__':
    main()


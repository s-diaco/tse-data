# %%
import storage
import pandas as pd
from io import StringIO


with open('temp2.txt', 'r', encoding='utf-8') as f:
    file_cont = f.read()
    rows = pd.read_csv(StringIO(file_cont.replace('\\n', '\n')),
                       header=None, sep=',', lineterminator='\n')
    print(rows.head(4))
    arr = rows.to_numpy()
# %%


def parse_instruments(struct=False, arr=False, struct_key='InsCode', itd=False):
    instruments = None
    strg = storage.Storage()
    file_cont = strg.get_item('tse.instruments'+('', '.intraday')[itd])
    if file_cont:
        rows = pd.read_csv(StringIO(file_cont))
    else:
        rows = []
        for row in rows:
            if arr:
                instruments = []
                if itd:
                    if struct:
                        instruments.push(instrument_itd(row))
                    else:
                        instruments.push(row)
                else:
                    if struct:
                        instruments.push(instrument(row))
                    else:
                        instruments.push(row)
            else:
                instruments = {}
                if itd:
                    if struct:
                        key = instrument_itd(row)[struct_key]
                        instruments[key] = instrument_itd(row)
                    else:
                        key = row.split(',', 1)[0]
                        instruments[key] = row
                else:
                    if struct:
                        key = instrument(row)[struct_key]
                        instruments[key] = instrument(row)
                    else:
                        key = row.split(',', 1)[0]
                        instruments[key] = row
    return instruments


def parse_shares(struct=False, arr=True):
    shares = None
    rows = storage.get_item('tse.shares')
    if rows:
        rows.split('\n')
        for row in rows:
            if arr:
                shares = []
                if struct:
                    shares.push(share(row))
                else:
                    shares.push(row)
            else:
                shares = {}
                if struct:
                    key = share(row).InsCode
                    if not shares.has_key(key):
                        shares[key] = []
                    shares[key].push(share(row))
                else:
                    key = row.split(',', 2)[1]
                    shares[key].push(row)
    return shares


def clean_fa(text):
    text = text.replace('\u200B', '')  # zero-width space
    # text = text.replace('\u200C', ' ') # zero-width non-joiner
    text = text.replace('\u200D', '')  # zero-width joiner
    text = text.replace('\uFEFF', '')  # zero-width no-break space
    text = text.replace('ك', 'ک')  # kaf
    text = text.replace('ي', 'ی')  # ye
    return text

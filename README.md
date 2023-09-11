[![Tests](https://github.com/s-diaco/tse-data/actions/workflows/python-app.yml/badge.svg)](https://github.com/s-diaco/tse-data/actions/workflows/python-app.yml)

A python package that helps to access stock data from the Tehran Stock Exchange (TSETMC).

## Usage:

### 1- Command line:

```bash
dtse update ["ذوب"]
dtse update [ذوب,"فولاد", "خساپا", "شپنا"]
dtse update ["شاخص کل6"]
dtse update ["شاخص کل فرابورس6", "شاخص کل (هم وزن)6"]
dtse reset
```

### 2- python:

import dtse

```python
# dtse.get_tse_prices(symbols: list[str], **kwconf)
dtse.get_tse_prices(symbols=["همراه"])
```

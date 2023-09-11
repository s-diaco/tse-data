[![Tests](https://github.com/s-diaco/tse-data/actions/workflows/python-app.yml/badge.svg)](https://github.com/s-diaco/tse-data/actions/workflows/python-app.yml)

A python package that helps to access stock data from the Tehran Stock Exchange (TSETMC).

## Usage:

### 1- Command line:

```bash
dtse update ["ذوب"]
dtse update ["همراه", "ذوب", "فولاد", "شیراز", "وخارزم"]
dtse update ["شاخص کل6"]
dtse reset
```

### 2- python:

```python
import dtse

# dtse.get_tse_prices(symbols: list[str], **kwconf)
dtse.get_tse_prices(symbols=["همراه"], {"adjust_prices": 2})
```

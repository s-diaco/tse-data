# dtse: A Fast and Async API Client for TSE

[![Tests](https://github.com/s-diaco/tse-data/actions/workflows/python-app.yml/badge.svg)](https://github.com/s-diaco/tse-data/actions/workflows/python-app.yml)
![Python Version from PEP 621 TOML](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2Fs-diaco%2Ftse-data%2Fmain%2Fpyproject.toml)
![GitHub](https://img.shields.io/github/license/s-diaco/tse-data)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/s-diaco/tse-data)
[![Coverage Status](https://coveralls.io/repos/github/s-diaco/tse-data/badge.svg?branch=main)](https://coveralls.io/github/s-diaco/tse-data?branch=main)

This is a python package that helps to access stock data from the Tehran Stock Exchange without any HTML parsing.

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
dtse.get_tse_prices(
        symbols=["همراه"], 
        adjust_prices=2,
        cache_to_db=False,
        write_csv=False
)
```

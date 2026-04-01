# allhomes_py

[![codecov](https://codecov.io/gh/OWNER/REPO/branch/main/graph/badge.svg)](https://codecov.io/gh/mevers/allhomes_py)

`allhomes_py` is a Python package that exposes `get_past_sales_data()` for retrieving historical Allhomes past sales data for ACT and NSW suburbs.

## Usage

```python
from allhomes_py import get_past_sales_data

sales = get_past_sales_data("Balmain, NSW", year=2020)
print(sales)
```

## Installation

```bash
pip install git+https://github.com/mevers/allhomes_py.git
```

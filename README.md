# allhomes_py

[![codecov](https://codecov.io/gh/mevers/allhomes_py/graph/badge.svg?token=6GSIL8PPGD)](https://codecov.io/gh/mevers/allhomes_py)

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

## Contributing

Please report bugs and request features on [GitHub](https://github.com/mevers/allhomes_py/issues). Pull requests are welcome!

## Disclaimer

This package is not affiliated with or endorsed by [allhomes.com.au](https://www.allhomes.com.au/). Functions may break if the website changes its data format. Historical data may also be updated or removed by Allhomes. All data provided are subject to the [Domain General Terms and Conditions](https://www.domain.com.au/group/domain-general-terms-and-conditions/).

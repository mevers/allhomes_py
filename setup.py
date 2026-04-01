from setuptools import find_packages, setup

setup(
    name="allhomes_py",
    version="0.1.0",
    description="Python package for extracting historical Allhomes past sales data for ACT/NSW suburbs.",
    author="allhomes_py maintainer",
    python_requires=">=3.9",
    install_requires=[
        "polars>=0.18.0",
        "requests>=2.28.0",
    ],
    packages=find_packages(include=["allhomes_py", "allhomes_py.*"]),
)

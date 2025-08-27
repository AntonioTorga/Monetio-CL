from setuptools import setup, find_packages

setup(
    name="monetio_cl",
    version="0.1",
    py_modules=["monetio_cl"],
    packages=find_packages(),
    install_requires=[
        "typer[all]",
        "dask",
        "xarray",
        "pyarrow",
        "httpx",
    ],
    entry_points={
        "console_scripts": [
            "Monetio-Cl=monetio_cl:app",
        ],
    },
)

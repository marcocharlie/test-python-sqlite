# test-python-sqlite
A repository to test the Python SQLite module.

## Requirements

You can run the application in a Python 3.7 environment.

### Python env

Install requirements via PIP

```bash
virtualenv .venv
pip install -r requirements.txt
```

Execute the application

```bash
python main.py
```

## Usage

The application offers:
- a `main` file in order to download the dataset from [here](https://catalog.data.gov/dataset/air-quality-measures-on-the-national-environmental-health-tracking-network) and insert records to a local SQLite database
- a `utils` file housing various methods in order to write data to a SQLite database and querying on it
- a Jupyter Notebook in order to perform specific queries on the created database
- an `html` file showing the results from the notebook

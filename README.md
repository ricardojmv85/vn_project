# vn_project

Development of a process for reading, transforming and inserting raw data in S3 following partition conditions.

## Requierements

* S3 bucket
* Setup your awscli with credentials or configure ENV variables for aws authentication
* CSV data from https://www.kaggle.com/wordsforthewise/lending-club

## Installation

Use the package manager pip to install the listed libraries in the requierements file.

#### [Optional] Setup a new environment

```bash
pip install -r requirements.txt
```

## Usage

Download raw data from the Kaggle repository, then place the raw csv inside the src directory. 

In order to run the process place a terminal in the project's root directory

```bash
python src\etl.py filename.csv
```
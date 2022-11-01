# Lestrade - Database Performance Evaluation
This project looks for evaluate multiple database in order to analyze performance according to several parameters. The database already available to be evaluated are Cassandra, MongoDB and Redis.

## 1. Setup
To facilitate setup and installation, this project is using a dependency manager, called Poetry. With this tool, there only some code executions needed to run the code.

### 1.1. Install Poetry
Installing Poetry is simple and can be done following the [official documentation](https://python-poetry.org/docs/).

### 1.2. Enter the shell
Once installed, Poetry can create a virtual enviroment to encapsulate libraries and dependency installation.

```
poetry shell
```

### 1.3. Install dependencies
Just execute the following instruction:

```
poetry install
```

## 2. Test scenarios
The parameters accepted to test is the following:

| Parameter        | Description                    |
| ---------------- | ------------------------------ |
| id               | Test identification            |
| users            | Number of concurrent users     |
| duration         | Time of execution (in seconds) |
| replication_sync | WEAK/STRONG                    |
| operation        | READ/WRITE                     |
| database         | Name of database               |

The test scenarios are put in scenarios directory. It is a CSV file with each one of these parameters and each line corresponds to a new test case.

## 3. Execution
When the database is configured and so the test scenarios, you can start script execute this instruction:

```
python main.py --scenarios=scenarios/test_case_name.csv --save
```

Argument description:
- scenarios: path to test scenario file
- save: flag to indicate if this execution should generate a result file.

At the end of the execution, the result files should be saved at results directory.
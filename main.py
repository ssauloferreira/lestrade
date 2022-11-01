"""
@author: sauloferreira

args:
1. test-type: description of the tests for identification purposes (stress, software_aging or analysis)
2. scenario: path to the scenario

example:
    python run.py --test-type=software_aging --scenario=scenarios/software_aging/scenario_high.csv
"""
from src.helpers.processors import PostProcessor
from progress.bar import Bar
import argparse
import datetime
import os
import pandas as pd
import time

parser = argparse.ArgumentParser(
    description="Cassandra Locust Test", formatter_class=argparse.ArgumentDefaultsHelpFormatter
)

parser.add_argument("--scenario", help="Path to the CSV scenario file")
parser.add_argument("--save", help="Save the results in a CSV file", action="store_true")

args = parser.parse_args()
config = vars(args)
print(config)

scenarios = pd.read_csv(config["scenario"], encoding="ISO-8859-1")

dir_datetime = datetime.datetime.now().strftime("%d_%m_%Y_%H_%M_%S_%f")
result_dir = f"results/{dir_datetime}"

if config["save"]:
    os.mkdir(result_dir)
    os.system(f"cp {config['scenario']} {result_dir}/executed_scenario.csv")

len = scenarios.shape[0]
bar = Bar("progress", max=len)


for i in range(len):
    if config["save"]:
        os.mkdir(f"{result_dir}/test_{scenarios['id'][i]}")

    command = (
        f"locust --users={scenarios['users'][i]} --run-time={scenarios['duration'][i]}s "
        + f" --replication-sync={scenarios['replication_sync'][i]} "
        + f"--test-name={scenarios['id'][i]}"
    )

    if config["save"]:
        command += (
            f" --html={result_dir}/test_{scenarios['id'][i]}/result.html "
            + f"--csv={result_dir}/test_{scenarios['id'][i]}/result "
        )
    
    command += " " + scenarios["database"][i] + "User" + scenarios["operation"][i].capitalize()

    print(command)
    print(" id", i, "command\n\n", command, "\n\n")
    bar.next()
    os.system(command)
    # time.sleep(600)

if config["save"]:
    post_processor = PostProcessor()
    merged_results = post_processor.merge_test_results(result_dir)
    merged_results.to_csv(f"{result_dir}/merged_results.csv")

bar.finish()

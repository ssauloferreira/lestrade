from src.helpers import helper
from dotenv import dotenv_values
import pandas as pd


class PostProcessor:
    def __init__(self):
        config = dotenv_values("./config/.env")
        self.ignore_keys = config["IGNORE_KEYS"].split(",")

    def merge_test_results(self, result_dir : str) -> pd.DataFrame:
        scenarios = pd.read_csv(f"{result_dir}/executed_scenario.csv")
        values_to_merge = {"id": []}

        for i, item in enumerate(scenarios["id"]):
            df = pd.read_csv(f"{result_dir}/test_{item}/result_stats.csv")
            values_to_merge["id"].append(item)

            for j, key in enumerate(df):
                formatted_key = helper.format_header(key)
                if formatted_key not in self.ignore_keys:
                    key_values = values_to_merge.get(formatted_key, [])
                    key_values.append(df[key][0])
                    values_to_merge[formatted_key] = key_values

        df_to_merge = pd.DataFrame(values_to_merge)
        return scenarios.merge(df_to_merge, how="left", on="id")

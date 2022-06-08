from typing import Any

from pandas import DataFrame


def df_rows_to_list_of_data(df: DataFrame, columns: dict[str, str]) -> list[dict[str, Any]]:
    return df.rename(columns=columns)[columns.values()].to_dict("records")

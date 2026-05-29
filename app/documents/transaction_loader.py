import json
from time import time
import pandas as pd

from app.streaming.producer import publish_transaction


def process_document(file_path: str, file_format: str):

    transactions = []

    if file_format == "json":

        with open(file_path, "r", encoding="utf-8") as f:
            transactions = json.load(f)

    elif file_format == "csv":

        df = pd.read_csv(file_path)
        transactions = df.to_dict(orient="records")

    elif file_format == "xlsx":

        df = pd.read_excel(file_path)
        transactions = df.to_dict(orient="records")

    else:
        raise ValueError(
            f"Unsupported format: {file_format}"
        )

    for tx in transactions:
        publish_transaction(tx)
        time.sleep(0.1)

    return len(transactions)
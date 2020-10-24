#!/opt/anaconda/envs/bd9/bin/python3

import numpy as np
import pandas as pd
import pickle
import sys

from sklearn.linear_model import LogisticRegression

# read the model, deserialize and unpickle it.

model = pickle.loads(
    base64.b64decode(
        open("lab07.model").read().encode('utf-8')
    )
)

rows = []  # here we keep input data to Dataframe constructor


def parse_wrapped_array(input_val: str):
    changed_value = input_val.replace(
        "WrappedArray(", ""
    ).replace(")", "")

    return changed_value.replace(" ", "").split(",")


# iterate over standard input
for line in sys.stdin:
    # parse line into a dict: {"column1": value1, ...}

    changed_value: str = line.replace("[", "").replace("]", "")

    values_list = changed_value.split(",")

    line_dict = {
        'uid': values_list[0],
        'domains': parse_wrapped_array(values_list[1])
    }
    rows.append(line_dict)

# initialize a dataframe from the list
df = pd.DataFrame(rows)

# run inference
pred = model.predict(df)

# Output to stdin, so that rdd.pipe() can return the strings to pipedRdd.
print(pred)

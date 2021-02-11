import tensorflow as tf
import numpy as np
import pandas as pd
import json


def load_encoded_queries():
    with open('encoded_queries.json', 'r') as f:
        data_dict = json.load(f)

    return data_dict


def extract_values(all_data):
    data_keys = encoded_data.keys()
    labels = ["Tables", "Joins", "Predicate String", "Predicate Numeric", "Cardinality"]
    df = pd.DataFrame()

    for label in labels:
        all_values = [all_data[k][label] for k in data_keys]
        if label != 'Cardinality':
            padded_input = tf.keras.preprocessing.sequence.pad_sequences(all_values, padding='post', dtype='float64')
            final_input = np.array(padded_input).tolist()
            all_values = final_input
        df[label] = all_values
    df.reset_index(drop=True, inplace=True)

    return df


def save_to_file(df):
    df.to_csv('regressor_dataset.csv', index=False)


def split_train_test(df):
    df_copy = df.copy()
    train_set = df_copy[:70]
    test_set = df_copy.drop(train_set.index)

    # train_set_labels = train_set.pop('Cardinality')
    # test_set_labels = test_set.pop('Cardinality')
    # print(train_set_labels)
    return train_set, test_set


if __name__ == "__main__":
    encoded_data = load_encoded_queries()
    data_df = extract_values(encoded_data)
    save_to_file(data_df)
    train_dataset, test_dataset = split_train_test(data_df)

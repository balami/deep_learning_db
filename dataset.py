import json
import tensorflow as tf
import numpy as np


class Dataset:
    def __init__(self):
        self.MAX_CARDINALITY = 6.56944320390689  # 3710592
        self.MIN_CARDINALITY = 0.30102999566398  # 2
        self.load_dataset()
        self.test_train_split()
        self.get_inputs()

    @staticmethod
    def load_dataset(self):
        with open('encoded_queries.json', 'r') as f:
            self.data_dict = json.load(f)

    @staticmethod
    def test_train_split(self):
        # TODO pass option to split or random as function parameters itself
        split_random = False
        overfit_split = False

        keys = list(self.data_dict.keys())
        self.train_dict = {}
        self.test_dict = {}
        if split_random:
            for key in keys:
                # TODO read the query numbers from file
                if key not in ["28c.sql", "17b.sql", "3a.sql", "19c.sql", "26c.sql",
                               "15d.sql", "16d.sql", "16c.sql", "20c.sql", "15a.sql",
                               "3c.sql", "20b.sql", "31b.sql", "25c.sql", "30b.sql",
                               "8d.sql", "20a.sql", "19b.sql", "19d.sql", "17f.sql",
                               "28a.sql", "6c.sql", "2b.sql", "7b.sql", "4a.sql",
                               "9b.sql", "13d.sql", "27c.sql", "6d.sql", "7c.sql",
                               "6f.sql", "33c.sql", "17c.sql", "23c.sql", "29b.sql", "1c.sql", "17d.sql"]:
                    self.train_dict[key] = self.data_dict[key]
                else:
                    self.test_dict[key] = self.data_dict[key]

        elif overfit_split:
            self.train_dict = self.data_dict
            self.test_dict = self.data_dict

        else:
            self.train_dict = dict(list(self.data_dict.items())[:70])
            self.test_dict = dict(list(self.data_dict.items())[70:])

    @staticmethod
    def get_inputs(self, query_batch):
        # query_batch contains dictionary keys example 1.sql
        q_names = query_batch
        target = [self.data_dict[k]['Cardinality'] for k in query_batch]

        # TODO refactor redundant codes
        all_tables = [self.data_dict[k]['Tables'] for k in query_batch]
        tables_input = tf.keras.preprocessing.sequence.pad_sequences(all_tables, padding='post', dtype='float64')
        self.tables_input = np.array(tables_input).tolist()

        all_joins = [self.data_dict[k]['Joins'] for k in query_batch]
        joins_input = tf.keras.preprocessing.sequence.pad_sequences(all_joins, padding='post', dtype='float64')
        self.joins_input = np.array(joins_input).tolist()

        all_ands = [self.data_dict[k]['Predicate String'] for k in query_batch]
        and_input = tf.keras.preprocessing.sequence.pad_sequences(all_ands, padding='post', dtype='float64')
        self.and_input = np.array(and_input).tolist()

        all_norm_ands = [self.data_dict[k]['Predicate Numeric'] for k in query_batch]
        normalized_and_input = tf.keras.preprocessing.sequence.pad_sequences(all_norm_ands, padding='post',
                                                                             dtype='float64')
        self.normalized_and_input = np.array(normalized_and_input).tolist()

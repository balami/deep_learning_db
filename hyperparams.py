import tensorflow as tf


class Setup:
    def __init__(self, units=128):
        self.batch_size = 32
        self.table_vlength = units
        self.join_vlength = units
        self.and_vlength = units
        self.normalized_and_vlength = units
        self.or_vlength = units
        self.nested_or_vlength = units

    @staticmethod
    def hp_initializer(self, **kwargs):
        if "GlorotUniform" == kwargs:
            self.initializer = tf.initializers.GlorotUniform()
        else:
            self.initializer = tf.contrib.layers.variance_scaling_initializer(
                factor=2.0,
                mode='FAN_IN',
                uniform=False,
                seed=42,
                dtype=tf.dtypes.float32)

    @staticmethod
    def hp_dropouts(self, external_dropout, internal_dropout):
        self.external_dropout = external_dropout
        self.internal_dropout = internal_dropout

    @staticmethod
    def hp_activation(self, **kwargs):
        if "relu" in kwargs:
            self.activation_fn = tf.nn.relu
        elif "leaky" in kwargs:
            self.activation_fn = tf.nn.leaky_relu
        elif "swish" in kwargs:
            self.activation_fn = tf.nn.swish

    @staticmethod
    def hp_learning_rate(self, rate):
        # rate could be 0.01, 0.001, 0.0001
        self.learning_rate = rate

    @staticmethod
    def hp_optimizer(self, **kwargs):
        if "AdamOptimizer" in kwargs:
            self.train_optimizer = tf.train.AdamOptimizer(self.learning_rate)
        elif "RMSPropOptimizer" in kwargs:
            self.train_optimizer = tf.train.RMSPropOptimizer(self.learning_rate)
        elif "MomentumOptimizer" in kwargs:
            if "use_nesterov" in kwargs:
                self.train_optimizer = tf.train.MomentumOptimizer(self.learning_rate, 0.9, name='Momentum',
                                                                  use_nesterov=True)
            else:
                self.train_optimizer = tf.train.MomentumOptimizer(self.learning_rate, 0.9, name='Momentum',
                                                                  use_nesterov=False)

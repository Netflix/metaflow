from metaflow import FlowSpec, step, batch, current, tensorflow_parallel, Parameter
import tensorflow as tf
import json
import os


class TensorflowParallelTest(FlowSpec):
    """
    Test flow to test @tensorflow_parallel.
    """

    num_parallel = Parameter(
        "num_parallel", help="Number of nodes in cluster", default=3
    )

    @step
    def start(self):
        self.next(self.parallel_step, num_parallel=self.num_parallel)

    @tensorflow_parallel
    @step
    def parallel_step(self):
        """
        Run a simple tensorflow parallel program
        """
        train()

        self.node_index = current.parallel.node_index
        self.num_nodes = current.parallel.num_nodes

        self.next(self.multinode_end)

    @step
    def multinode_end(self, inputs):
        """
        Check the validity of the parallel execution.
        """
        j = 0
        for input in inputs:
            assert input.node_index == j
            assert input.num_nodes == self.num_parallel
            j += 1
        assert j == self.num_parallel
        self.next(self.end)

    @step
    def end(self):
        pass


def train():
    tf_config = json.loads(os.environ["TF_CONFIG"])
    num_workers = len(tf_config["cluster"]["worker"])

    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    per_worker_batch_size = 4
    global_batch_size = per_worker_batch_size * num_workers
    multi_worker_dataset = dummy_dataset(global_batch_size)

    with strategy.scope():
        multi_worker_model = create_keras_model()
    multi_worker_model.fit(multi_worker_dataset, epochs=1, steps_per_epoch=4)


def create_keras_model():
    model = tf.keras.Sequential(
        [tf.keras.layers.InputLayer(input_shape=(1, 1)), tf.keras.layers.Dense(1)]
    )
    model.compile(
        loss=tf.keras.losses.MeanAbsoluteError(),
        optimizer=tf.keras.optimizers.SGD(learning_rate=0.001),
        metrics=["accuracy"],
    )
    return model


def dummy_dataset(batch_size):
    import numpy as np

    (x_train, y_train) = np.random.rand(100, 1), np.random.rand(100, 1) + 0.5
    # The `x` arrays are in uint8 and have values in the [0, 255] range.
    # You need to convert them to float32 with values in the [0, 1] range.
    x_train = x_train / np.float32(255)
    y_train = y_train.astype(np.int64)
    train_dataset = (
        tf.data.Dataset.from_tensor_slices((x_train, y_train))
        .shuffle(60000)
        .repeat()
        .batch(batch_size)
    )
    return train_dataset


if __name__ == "__main__":
    TensorflowParallelTest()

from metaflow import FlowSpec, Parameter, step, pytorch_distributed, resources

from models.train import train_model
from models.evaluate import evaluate_model


class HelloPyTorch(FlowSpec):
    """
    A Feed Forward Neural Network trained with PyTorch
    """

    # Hyper-parameters
    input_data_path = Parameter(
        "input_data_path",
        help="MNIST dataset path, local or S3",
        default="./mnist_data",
    )
    model_path = Parameter("model_path", default="/tmp/mnist_cnn.pth")
    batch_size = Parameter("batch_size", default=1000)
    test_batch_size = Parameter("test_batch_size", default=1000)
    epochs = Parameter("epochs", default=1)
    optimizer = Parameter("optimizer", default="sgd")
    lr = Parameter("learning_rate", default=0.01)
    momentum = Parameter("momentum", default=0.5)
    seed = Parameter("seed", default=42)
    world_size = Parameter("world_size", default=1)
    train_accuracy_threshold = Parameter("train_accuracy_threshold", default=0.5)
    test_accuracy_threshold = Parameter("test_accuracy_threshold", default=0.5)

    @step
    def start(self):
        """
        Initialize the world_size ranks for PyTorch trainers
        """
        self.ranks = list(range(self.world_size))
        print(f"ranks: {self.ranks}")
        self.next(self.train, foreach="ranks")

    @resources(cpu=1, cpu_limit=2, gpu="1", memory="2G", memory_limit="5G")
    @pytorch_distributed
    @step
    def train(self):
        """
        PyTorch train step
        """
        self.rank = self.input
        print("self.rank", self.rank)

        self.model_state_dict = train_model(
            input_data_path=self.input_data_path,
            model_path=self.model_path,
            batch_size=self.batch_size,
            test_batch_size=self.test_batch_size,
            epochs=self.epochs,
            optimizer=self.optimizer,
            lr=self.lr,
            momentum=self.momentum,
            seed=self.seed,
            world_size=self.world_size,
            rank=self.rank,
        )

        self.next(self.evaluate)

    @resources(cpu=1, cpu_limit=2, gpu=1, memory="1G", memory_limit="5G")
    @step
    def evaluate(self, inputs):
        train_input = next((x for x in inputs if x.rank == 0), None)
        print("train_input", train_input)

        self.model_state_dict = train_input.model_state_dict

        self.evaluate_results = evaluate_model(
            model_state_dict=self.model_state_dict,
            input_data_path=self.input_data_path,
            batch_size=self.batch_size,
            test_batch_size=self.test_batch_size,
            train_accuracy_threshold=self.train_accuracy_threshold,
            test_accuracy_threshold=self.test_accuracy_threshold,
        )

        self.next(self.end)

    @resources(gpu=1)
    @step
    def end(self):
        """
        Done! Can now publish or validate the results.
        """
        print(f"evaluate_results: {self.evaluate_results}")


if __name__ == "__main__":
    HelloPyTorch()

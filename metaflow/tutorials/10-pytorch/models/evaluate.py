from collections import OrderedDict
import json
from typing import NamedTuple, Tuple

import torch
from models.cnn import Net
from models.train import validate
from torch.utils.data import DataLoader
from torchvision import transforms
from torchvision.datasets import MNIST


def evaluate_model(
    model_state_dict: OrderedDict,
    input_data_path: str,
    batch_size: int = 64,
    test_batch_size: int = 64,
    test_accuracy_threshold: float = 0.9,
    train_accuracy_threshold: float = 0.9,
) -> Tuple[float, float]:
    print(f"Starting evaluation..")

    use_cuda = torch.cuda.is_available()
    device = torch.device("cuda" if use_cuda else "cpu")

    model = Net().type("torch.FloatTensor").to(device)

    print(f"Loading trained model state dict.")
    model.load_state_dict(model_state_dict)

    print("Loading dataset.")
    train_loader = DataLoader(
        MNIST(
            input_data_path,
            train=True,
            download=True,
            transform=transforms.Compose(
                [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
            ),
        ),
        batch_size=batch_size,
    )
    train_accuracy = validate(model, device, train_loader)
    print(f"Training dataset accuracy = {train_accuracy}")

    test_loader = DataLoader(
        MNIST(
            input_data_path,
            train=False,
            transform=transforms.Compose(
                [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
            ),
        ),
        batch_size=test_batch_size,
    )
    test_accuracy = validate(model, device, test_loader)
    print(f"Test dataset accuracy = {test_accuracy}")

    if test_accuracy < test_accuracy_threshold:
        raise Exception(
            f"test_accuracy is {test_accuracy} threshold {test_accuracy_threshold}"
        )

    if train_accuracy < train_accuracy_threshold:
        raise Exception(
            f"train_accuracy is {train_accuracy} threshold: {train_accuracy_threshold}"
        )

    return (train_accuracy, test_accuracy)


if __name__ == "__main__":
    model_state_dict = torch.load("/opt/zillow/shared/anchit/playground/mnist_cnn.pth")
    evaluate_model(
        model_state_dict=model_state_dict,
        input_data_path="/opt/zillow/shared/anchit/playground/data/MNIST",
        batch_size=512,
        test_batch_size=512,
        test_accuracy_threshold=0.5,
        train_accuracy_threshold=0.5,
    )
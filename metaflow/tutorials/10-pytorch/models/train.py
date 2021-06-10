from __future__ import print_function

from collections import OrderedDict
import time
from math import ceil

import torch
import torch.distributed as dist
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from models.cnn import Net
from torchvision import datasets, transforms


def train(model, device, train_loader, optimizer, epoch):
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        data, target = data.type("torch.FloatTensor").to(device), target.to(device)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()
        if batch_idx % 10 == 0:
            print(
                "Train Epoch: {} [{}/{} ({:.0f}%)]\tloss={:.4f}".format(
                    epoch,
                    batch_idx * len(data),
                    len(train_loader.dataset),
                    100.0 * batch_idx / len(train_loader),
                    loss.item(),
                )
            )


def validate(model, device, val_loader):
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target in val_loader:
            data, target = data.type("torch.FloatTensor").to(device), target.to(device)
            output = model(data)
            # sum up batch loss
            test_loss += F.nll_loss(output, target, reduction="sum").item()
            # get the index of the max log-probability
            pred = output.max(1, keepdim=True)[1]
            correct += pred.eq(target.view_as(pred)).sum().item()

    test_loss /= len(val_loader.dataset)

    accuracy = float(correct) / len(val_loader.dataset)
    print("\naccuracy={:.4f}\n".format(accuracy))

    return accuracy


def is_distributed():
    return dist.is_available() and dist.is_initialized()


def train_model(
    input_data_path: str,
    model_path: str,
    batch_size: int = 64,
    test_batch_size: int = 64,
    epochs: int = 1,
    optimizer: str = "sgd",
    lr: float = 0.01,
    momentum: float = 0.5,
    seed: int = 42,
    world_size: int = 1,
    rank: int = 0,
    data_loading_workers: int = 1,
) -> OrderedDict:
    # check if GPU is available
    use_cuda = torch.cuda.is_available()
    if use_cuda:
        print("Using CUDA")
        pytorch_backend = "nccl"
    else:
        print("Using CPU")
        pytorch_backend = "gloo"

    torch.manual_seed(seed)

    # switch between GPU and CPU
    device = torch.device("cuda" if use_cuda else "cpu")

    # init distributed training if more than 1 nodes are available for distributed training
    if dist.is_available() and world_size > 1:
        print("Using distributed PyTorch with {} backend".format(pytorch_backend))
        dist.init_process_group(
            backend=pytorch_backend,
            init_method="file:///opt/metaflow_volume/sharedfile",
            world_size=world_size,
            rank=rank,
        )
        world_size = dist.get_world_size()
        rank = dist.get_rank()
    print(f"WORLD_SIZE = {world_size}")
    print(f"RANK = {rank}")

    # load MNIST dataset
    train_dataset = datasets.MNIST(
        input_data_path,
        train=True,
        download=True,
        transform=transforms.Compose(
            [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
        ),
    )
    print(f"Dataset size = {train_dataset.data.size()}")

    # create data sampler to divide a training batch into sub-batches
    if is_distributed():
        train_sampler = torch.utils.data.distributed.DistributedSampler(
            dataset=train_dataset, num_replicas=world_size, rank=rank
        )
    else:
        train_sampler = None

    batch_size = int(batch_size / float(world_size))
    workers = data_loading_workers if use_cuda else 1
    train_loader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=(train_sampler is None),
        num_workers=workers,
        pin_memory=use_cuda,
        sampler=train_sampler,
    )
    num_batches = ceil(len(train_loader.dataset) / float(batch_size))
    print(f"num_batches = {num_batches}")
    print(f"batch size = {batch_size}")
    print(f"Train dataset size for this node = {train_loader.sampler.num_samples}")

    val_loader = torch.utils.data.DataLoader(
        datasets.MNIST(
            input_data_path,
            train=False,
            transform=transforms.Compose(
                [transforms.ToTensor(), transforms.Normalize((0.1307,), (0.3081,))]
            ),
        ),
        batch_size=test_batch_size,
        shuffle=False,
        num_workers=workers,
        pin_memory=True,
    )

    # create model
    model = Net().type("torch.FloatTensor").to(device)

    # distribute model across nodes
    if is_distributed():
        Distributor = (
            nn.parallel.DistributedDataParallel
            if use_cuda
            else nn.parallel.DistributedDataParallelCPU
        )
        model = Distributor(model)

    # define the optimizer
    if optimizer == "sgd":
        optimizer = optim.SGD(model.parameters(), lr=lr, momentum=momentum)
    else:
        print("Optimizer not defined.")

    start = time.time()
    for epoch in range(1, epochs + 1):
        if is_distributed():
            print(f"Train sampler epoch = {epoch}")
            train_sampler.set_epoch(epoch)
        train(model, device, train_loader, optimizer, epoch)
        validate(model, device, val_loader)
    end = time.time()
    delta = end - start
    print(f"Model trained in {delta:.2f}s")

    print(f"Model trained successfully.")

    if world_size > 1:
        model_state_dict = model.module.state_dict()
    else:
        model_state_dict = model.state_dict()

    if rank == 0:
        print(f"Trainer{rank}: saving model state dictionary.")
        torch.save(model_state_dict, model_path)

    return model_state_dict


if __name__ == "__main__":
    model_state_dict = train_model(
        input_data_path="/Users/anchita/playground/data/MNIST",
        model_path="/Users/anchita/mnist_cnn.pth",
        batch_size=1024,
        test_batch_size=1024,
    )

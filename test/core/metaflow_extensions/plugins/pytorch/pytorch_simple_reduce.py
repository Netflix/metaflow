# Run very simple parallel pytorch program that reduces a vector.
import pytorch_lightning as pl
import os
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.distributed as dist
import torch.optim as optim

class LearnToSum(pl.LightningModule):
    def __init__(self):
        super().__init__()
        self.linear = nn.Linear(10, 1)

    def forward(self, x):
        x = self.linear(x)
        return x

    def training_step(self, batch, batch_idx):
        data, target = batch
        output = self(data)
        loss = F.l1_loss(output, target)
        return loss

    def on_epoch_end(self) -> None:
        # Do rank all reduce at epoch end to validate parallel execution
        rank = torch.tensor([dist.get_rank() + 1], dtype=torch.int32)
        dist.all_reduce(rank, op=dist.ReduceOp.SUM)
        self.rank_reduction = rank


    def configure_optimizers(self):
        return {"optimizer": optim.SGD(self.parameters(), lr=0.01)}


def train(num_local_processes):
    num_nodes = int(os.environ.get("NUM_NODES", "1"))
    trainer = pl.Trainer(
        gpus=None,
        num_processes=num_local_processes,
        accelerator="ddp",
        num_nodes=num_nodes,
        max_epochs=5,
        callbacks=[],
    )
    model = LearnToSum()
    inps = torch.rand(1000, 10)
    targets = torch.sum(inps, axis=1)
    dataset = torch.utils.data.TensorDataset(inps, targets)
    trainer.fit(model, torch.utils.data.DataLoader(dataset, batch_size=1))
    print("result")
    print(model(torch.arange(10, dtype=torch.float32)), "expect close to", sum(range(10)))
    assert model.rank_reduction == sum(range(1, 1 + num_local_processes * num_nodes))
    return model.rank_reduction


if __name__ == "__main__":
    train(3)


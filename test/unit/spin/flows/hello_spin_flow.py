from metaflow import FlowSpec, step
import random


class HelloSpinFlow(FlowSpec):

    @step
    def start(self):
        chunk_size = 1024 * 1024  # 1 MB
        total_size = 1024 * 1024 * 1000  # 1000 MB

        data = bytearray()
        for _ in range(total_size // chunk_size):
            data.extend(random.randbytes(chunk_size))

        self.a = data
        self.next(self.end)

    @step
    def end(self):
        print(f"Size of artifact a: {len(self.a)} bytes")
        print("HelloSpinFlow completed.")


if __name__ == "__main__":
    HelloSpinFlow()

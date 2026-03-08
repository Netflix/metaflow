# Running Metaflow on Windows using WSL

This guide explains how to install and run Metaflow on Windows using Windows Subsystem for Linux (WSL).

## 1. Install WSL

Open PowerShell and run:

wsl --install

Restart your computer after installation.

## 2. Install Ubuntu

Install Ubuntu from the Microsoft Store and launch it.

Create a Linux username and password when prompted.

## 3. Install Python and Git

Run the following commands:

sudo apt update
sudo apt install python3 python3-pip git

## 4. Create a Virtual Environment

python3 -m venv gsoc-env
source gsoc-env/bin/activate

## 5. Install Metaflow

pip install metaflow

## 6. Run a Simple Flow

Create a file called hello.py with the following content:

from metaflow import FlowSpec, step

class HelloFlow(FlowSpec):

    @step
    def start(self):
        print("Hello from Metaflow!")
        self.next(self.end)

    @step
    def end(self):
        print("Flow complete.")

if __name__ == "__main__":
    HelloFlow()

Then run:

python hello.py run

If everything works, Metaflow should execute the flow successfully and you should get an output similar to:
Hello from Metaflow!
Flow complete.

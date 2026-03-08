# Running Metaflow on Windows using WSL

This guide explains how to run Metaflow on Windows using Windows Subsystem for Linux (WSL).

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

Create a file called hello_flow.py and run:

python hello_flow.py run

If everything works, Metaflow should execute the flow successfully.

#!/bin/bash

# Check if Python is installed
if ! command -v python3 &>/dev/null; then
    echo "Python is not installed. Please install Python and try again."
    exit 1
fi

# Check if pip is installed
if ! command -v pip &>/dev/null; then
    echo "pip is not installed. Please install pip and try again."
    exit 1
fi

# Create a virtual environment (optional but recommended)
python3 -m venv myenv
source myenv/bin/activate

# Install or upgrade pip and setuptools
pip install --upgrade pip setuptools

# Install Faker library
pip install Faker

echo "Environment is set up with Python and Faker."
echo "You can activate the virtual environment by running: source myenv/bin/activate"

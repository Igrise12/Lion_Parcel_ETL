#!/bin/bash

# Create necessary directories
mkdir -p dags logs plugins init-scripts
mkdir -p logs/scheduler logs/webserver logs/worker

# Set proper permissions
chmod -R 777 logs
chmod -R 777 plugins

# Create empty __init__.py files
touch dags/__init__.py
touch plugins/__init__.py
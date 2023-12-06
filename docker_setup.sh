#!/bin/bash

# Stop and remove containers
docker-compose down

# Remove volumes (use the -v flag to delete volumes)
docker-compose down -v

# Remove orphaned volumes
docker volume ls -qf dangling=true | xargs -r docker volume rm

# Build and start containers
docker-compose up --build -d


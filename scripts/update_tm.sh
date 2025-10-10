#!/bin/bash

# Function to clone or update the repository
clone_or_update_repository() {
  if [ -d "$FOLDER_NAME" ]; then
    echo "The folder '$FOLDER_NAME' already exists. Updating..."
    cd "$FOLDER_NAME"
    git pull
    cd "scripts"
  else
    echo "Cloning the repository into '$FOLDER_NAME'..."
    git clone "$REPO_URL" "$FOLDER_NAME"
    cd "$FOLDER_NAME/scripts"
  fi
}

# Define the repository URL and folder name
REPO_URL="https://gitlab.com/ip-fabric/integrations/ipfabric-netbox-sync.git"
FOLDER_NAME="ipfabric-netbox-sync"

# Check if the folder exists and clone/update the repository
clone_or_update_repository

# Get the current working directory (PWD)
CURRENT_DIR=$(pwd)

echo "The current working directory is: $CURRENT_DIR"

# Activate the virtual environment
source /opt/netbox/venv/bin/activate

# Define the Python script
SCRIPT="
exec(open('$CURRENT_DIR/import_tm.py').read())
from ipfabric_netbox.models import IPFabricTransformMap, IPFabricTransformField, IPFabricRelationshipField
print(f'Transform Map Count: {IPFabricTransformMap.objects.all().count()}')
print(f'Transform Field Count: {IPFabricTransformField.objects.all().count()}')
print(f'Relationship Field Count: {IPFabricRelationshipField.objects.all().count()}')"

# Pass the script to nbshell
echo "$SCRIPT" | python3 /opt/netbox/netbox/manage.py nbshell

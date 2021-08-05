#!/bin/sh

# get path, repo_path
x="git rev-parse --show-toplevel"
repo_path=$(eval "$x")

if ! command -v nb-clean &> /dev/null # if is not installed, install library
then
  python3 -m pip install nb-clean
else # else make actions
  if [[ $1 == "activate" ]]
  then
    # activate filter
    nb-clean add-filter --preserve-cell-metadata --remove-empty-cells
    echo 'Clean outputs activated'
  elif [[ $1 == "deactivate" ]]
  then
    # deactivate filter
    nb-clean remove-filter
    echo 'Clean outputs deactivated'
  elif [[ $1 == "status" ]]
  then
    # check if filter is activated or not for current repo
    if grep -Fxq "[filter \"nb-clean\"]" ${repo_path}/.git/config
    then
      echo "Clean outputs filter is ON for current repo: $repo_path"
    else
      echo "Clean outputs filter is OFF for current repo: $repo_path"
    fi  
  else
    echo 'Missing argument: activate, deactivate or status'
    exit 1
  fi
fi
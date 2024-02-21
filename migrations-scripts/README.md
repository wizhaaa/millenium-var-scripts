# Purpose

This directory gives scripts to automate data migration process to a Postgres instance.

## Setting Up

To improve housekeeping, create a virtual environment in this directory to avoid package bloating and to ensure that all requirements are correctly in path.

If you're working in this directory. The following will create a virtual environment in the this directory; otherwise, replace . with whatever path you're working in.

`python3 -m venv .`

Activate your virtual environment. Again, if you're in the current directory you can use the following.

`./source/activate`

Install dependencies.

`pip3 install -r requirements.txt`

You will need to enter the following database information in `constants.py`

```
HOST := database address
DATABASE := database
USER := database user
PASSWORD := database user password
PORT := database port
```

You will need to place the following csv's in this directory as well.

`pnl.csv -> your pnl table data`

`positions.csv -> your positions table data`

`securities.csv -> your positions table data`

## A Quick Overview

Since the pnl, positions, and securities tables can be rather large, we include checkpointing and write enable flags that allow us to resume data execution in cases where scripts crash/fail mid-process. However, in rare cases where you want to reupload all data without checkpointing simply erase and write `0 0 0` into `checkpoints.txt`. These number correspond to the last security row index written, the last position row index written, and the last pnl position row index written.

Furthermore, you can toggle the following flags to skip and avoid loading these tables - especially the pnl table, which can take some time to load into memory.

```
SECURITY_WRITE_ENABLE : boolean
POSITIONS_WRITE_ENABLE : boolean
PNL_WRITE_ENABLE : boolean
```

## TODO

I will make a Makefile to make this easier to set up when I have time.

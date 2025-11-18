# Smartsheet Job Number Generator

## Overview
This is a Python CLI application that automatically generates and assigns job numbers to Smartsheet sheets based on department and work request numbers. The script discovers qualifying sheets automatically and maintains state between runs.

## Project Type
Python command-line application (no frontend)

## Key Features
- Automatically discovers Smartsheet sheets with required columns (Dept #, Work Request #, Job #)
- Assigns job numbers based on department and work request numbers
- Maintains state between runs using a Smartsheet state sheet
- Supports optional helper columns (Helper Dept # and Helper Job [#])
- Detects existing job number format and applies it to new assignments
- Handles duplicate work requests across multiple sheets

## Required Environment Variables
- `SMARTSHEET_API_TOKEN`: Your Smartsheet API access token

## Required Sheet Structure
Target sheets must contain columns named:
- Dept #
- Work Request #
- Job #

Optional columns:
- Helper Dept #
- Helper Job [#] (both must be present for helper functionality)

State sheet (ID: 6534534683119492) must have:
- "key" column
- "value" column
- "StateData" row for main job number mappings
- "HelperStateData" row for helper job number mappings

## How to Run
Execute the script using:
```bash
python generate_job_numbers.py
```

## Dependencies
- smartsheet-python-sdk

## Recent Changes
- 2025-11-18: Initial import and Replit environment setup

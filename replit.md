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
- Processes sheets from nested folders in the Linetec - Resiliency workspace
- Handles both "Resiliency Promax Database" and "Intake Promax" sheets

## Performance Optimizations (v3 - Latest)
- **JSON Caching System**: Stores discovered sheet metadata for instant loading on future runs
- **Parallel Processing**: Uses ThreadPoolExecutor with 5 workers for concurrent operations
- **Batch Operations**: Updates up to 500 rows in single API calls
- **Smart Rate Limiting**: Enhanced rate limiter with burst capacity and request queuing
- **Progress Tracking**: Real-time progress with percentage, ETA, and current sheet display
- **Column ID Caching**: Eliminates redundant API calls for state sheet operations
- **Self-Optimizing**: Gets faster with each run as cache builds up

### Performance Improvements
- First run: ~10-15 minutes (discovery and caching)
- Subsequent runs: ~3-5 minutes (using cached data)
- Old version: 35+ minutes every run
- Processes sheets from specific workspace folders only
- Automatic cache persistence between runs

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

### Original Version (slower, 2+ hours)
```bash
python generate_job_numbers.py
```

### Optimized Version (faster, ~15-20 minutes)
```bash
python generate_job_numbers_optimized.py
```

## Dependencies
- smartsheet-python-sdk

## Recent Changes
- 2025-11-18: Initial import and Replit environment setup
- 2025-11-18: Created optimized version with parallel processing and performance improvements

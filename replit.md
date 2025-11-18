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

## Performance Optimizations (v2)
- **Parallel Processing**: Uses ThreadPoolExecutor with 6 workers for concurrent sheet processing
- **Cached Metadata**: Sheet discovery caches column information to avoid redundant API calls
- **Selective Column Fetching**: Only fetches required columns instead of entire sheets
- **Rate Limiting**: Implements intelligent rate limiting (300 req/min) with retry logic
- **Batch Updates**: Processes updates in batches of 500 rows
- **Progress Tracking**: Real-time progress updates and ETA calculations
- **Performance Metrics**: Built-in timing instrumentation for bottleneck analysis

### Performance Improvements
- Reduced runtime from 2+ hours to ~15-20 minutes
- Single sheet fetch per discovery (eliminated double-fetching)
- Parallel processing of up to 6 sheets simultaneously
- Respects Smartsheet API limits automatically

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

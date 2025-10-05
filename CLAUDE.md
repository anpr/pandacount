# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a personal finance management tool called "pandacount" that processes bank statement CSV files and categorizes transactions. The main script is `panda.py` which imports bank statements, applies automatic categorization, and manages financial data in YAML format.

## Core Architecture

- **Main script**: `panda.py` - CLI tool built with Typer for importing and categorizing bank transactions
- **Data storage**: `pandacount.yml` - Main data file containing all categorized transactions
- **Backup files**: `pandacount-backup.yml`, `pandacount-backup2.yml` - Historical backups
- **Analysis notebook**: `panda_scratch.ipynb` - Jupyter notebook for financial analysis and reporting
- **Input data**: `kontoauszüge/` directory contains bank statement CSV files (ISO-8859-1 encoded)

## Key Components

### Transaction Processing Pipeline
1. **CSV Import** (`to_raw_df`): Parses bank CSV files with German date format and ISO-8859-1 encoding
2. **Account Mapping** (`get_account`): Maps IBAN numbers to account names (common, giro, gesa, extra, extra-common)
3. **Categorization** (`categorize_df`): Applies extensive rule-based categorization using party/purpose string matching
4. **Transfer Categorization** (`transfer_categorize`): Identifies internal transfers between accounts
5. **Final Category Assignment** (`add_cat`): Merges automatic and manual categories

### Data Structure
- **Core fields**: account, book_date, valuta_date, party, book_text, purpose, amount, balance
- **Categories**: Hierarchical using `::` separator (e.g., `kinder::kleidung`, `mobilitaet::auto`)
- **Accounts**: 5 different bank accounts mapped by IBAN
- **Transfer tracking**: Separate transfer_category field for internal movements

## Development Commands

### Essential Commands
```bash
# Install dependencies
poetry install

# Import new bank statements
./panda.py ing-import <csv_file1> <csv_file2> ...

# Re-categorize existing data
./panda.py categorize

# Code quality
black panda.py --line-length 100
flake8 panda.py
mypy panda.py

# Analysis
jupyter lab panda_scratch.ipynb
```

### Environment Setup
- Uses mise for Python version management (3.13.7)
- Virtual environment in `.venv/pandacount-3.13.7`
- Poetry for dependency management

## Categorization System

The categorization system uses extensive string matching rules in `categorize_df()`:
- **Income categories**: `einnahmen::gehalt::andreas`, `einnahmen::gehalt::gesa`, `einnahmen::dividende`
- **Housing**: `wohnen::rate`, `wohnen::strom`, `wohnen::putzen`, `wohnen::GEZ`
- **Children**: `kinder::kleidung`, `kinder::sport`, `kinder::reiten`, `kinder::sparen`
- **Mobility**: `mobilitaet::auto`, `mobilitaet::db`, `mobilitaet::fliegen`
- **Health**: `gesundheit::debeka`, `gesundheit::vorleistung`
- **And many more hierarchical categories**

## Important Notes

- Bank statements must be in German CSV format with specific column headers
- Date parsing uses `dayfirst=True` for German date format (DD.MM.YYYY)
- Amounts use German number format (dots as thousands separator, comma as decimal)
- The system expects specific IBAN numbers mapped to account names
- Manual category overrides are supported via `category_manual` field
- All data is stored in human-readable YAML format for transparency

## File Encoding

- CSV files: ISO-8859-1 encoding (German bank export standard)
- Python files: UTF-8 encoding
- YAML files: UTF-8 with unicode support enabled
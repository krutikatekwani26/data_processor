# Data Processor

This project is a data processing package that focuses on formatting and merging datasets with a modular design.

## High-Level Structure

```bash
data_processor/
│
├── core/
│   ├── __init__.py
│   ├── dataset.py              # Manages the Dataset class
│   ├── base_processor.py       # Abstract class for processors
│   ├── operation.py            # Abstract class for operations
│
├── operations/
│   ├── formatting/
│   │   ├── __init__.py
│   │   ├── apply_formatting.py  # Subclass of operation for formatting
│
├── merging/
│   ├── __init__.py             # Handles merging operations
│
├── processors/
│   ├── DataCleaningProcessor.py         # Subclass of base_processor
|   |-- DataValidationProcessor.py
│   ├── __init__.py
│
├── utils/
│   ├── __init__.py
│   ├── helper.py
|   |-- exceptions.py
|
|-- Tests/-
|   |-- util_tests.py
|   |-- processor_tests.py
|        

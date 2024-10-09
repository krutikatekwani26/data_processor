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
│   │   ├── apply_formatting.py  
│
├── merging/
│   ├── __init__.py   
|   |--apply_merge.py          
│
├── processors/
│   ├── DataCleaningProcessor.py         
|   |-- DataValidationProcessor.py
|   |-- MergeProcessor.py
│   ├── __init__.py
│
├── utils/
│   ├── __init__.py
│   ├── helper.py
|   |-- exception_handler.py
|
|-- Tests/-
|   |-- test_helper.py
|   |-- test_data_cleaning_processor.py
|   |-- test_data_validaton_processor.py
|   |--__init__.py
|
       

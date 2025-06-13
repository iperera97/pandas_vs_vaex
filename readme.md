# Student Dataset Analytics - Pandas vs Vaex Comparison

This project demonstrates generating a large synthetic student dataset (~1 GB) and performing data processing and analytics using two popular Python data processing engines: **Pandas** and **Vaex**. It compares their usage within Docker containers limited to 1 CPU core and 1 GB of RAM.

---

## Project Structure

- `main.py` - Main application script to load the dataset and run various analytics queries using the selected data engine (Pandas or Vaex).
- `generate_dataset.py` - Script to generate the synthetic student dataset in CSV format using multiprocessing.
- `df_engines/` - Module containing wrappers for Pandas and Vaex dataframes with common interface methods.
- `utils.py` - Utility functions, e.g., for profiling resource usage.
- `Dockerfile` - Defines the Docker image with Python 3.8 and required dependencies.
- `docker-compose.yml` - Docker Compose file defining two services:
  - `pandas-service` - Runs the app with Pandas backend.
  - `vaex-service` - Runs the app with Vaex backend.

---

## Dataset Generation

The synthetic dataset contains student records with fields including:

- `student_id`
- `name`
- `gender`
- `year`
- `major`
- `subject`
- `marks`
- `age`
- `attendance_percentage`
- `passed` (Yes/No)

The dataset size is approximately **1 GB**, generated in parallel chunks using Python multiprocessing.

To generate the dataset independently, run:

```bash
python generate_dataset.py

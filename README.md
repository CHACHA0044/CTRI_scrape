# CTRI Clinical Trials Scraper

This production-grade script automates the extraction of clinical trial data from the Clinical Trials Registry - India (CTRI). It is specifically configured to target cancer-related research across multiple search categories.

## Features

- **Auto-Recovery**: Automatically detects browser or driver crashes and restarts the session without losing current progress.
- **Resume Capability**: Supports resuming from a specific category or trial index to handle long-running tasks or network interruptions.
- **Data Persistence**: Saves progress every 25 trials to a CSV file to prevent data loss.
- **Optimized Performance**: Uses eager page load strategies and disables heavy browser elements like images for faster scraping (approximately 0.8s per trial).

## Technical Requirements

- **Python 3.x**
- **Chrome Browser**
- **Dependencies**:
  - `selenium`
  - `pandas`
  - `webdriver-manager`

### Install dependencies using:
```bash
pip install selenium pandas webdriver-manager
```

## Usage

Run the script:
```bash
python ctri_scraper_final.py
```

The script will prompt for a manual CAPTCHA entry on the CTRI search page. After solving the CAPTCHA and clicking search, return to the terminal and press **ENTER** to start the automated extraction.

## Output Files

- **`ctri_cancer_trials_FINAL.csv`**: The final dataset containing all successfully scraped trials.
- **`ctri_cancer_trials_PROGRESS.csv`**: A temporary file used to store progress and enable the resume feature.
- **`ctri_final.log`**: Detailed execution logs including timestamps, success messages, and any encountered errors.

## Data Fields Captured

The scraper extracts comprehensive details including:

- CTRI Number and Study Titles
- Recruitment Status and Brief Summary
- Health Conditions and Interventions
- Primary/Secondary Outcome Measures
- Sponsor and Collaborator Information
- Demographic Data (Age, Gender)
- Phase and Study Design
- Important Dates (Registration, Enrollment, Completion)
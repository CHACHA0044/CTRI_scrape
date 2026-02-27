# CTRI Clinical Trials Scraper (Cancer Research)
This scraper systematically extracts detailed clinical trial data from the Clinical Trials Registry – India (CTRI) using a PDF-based parsing strategy, capturing 60+ structured fields per trial. It provides a solid baseline for building a more refined and high-precision extraction pipeline...

## 🚀 Key Features

*   **PDF-Based Extraction**: Instead of scraping limited HTML, the bot downloads the official PDF for each trial and extracts as much as possible (including hidden fields like Ethics Committee approval, Funding Sources, and detailed Sample Sizes).
*   **Robust Multi-Format Parser**: Handles varied PDF layouts (standard vs. merged cells) found across 4,000+ trials.
*   **Intelligent De-duplication**: Automatically identifies duplicate trials across different search categories (e.g., "Lung Cancer" vs. "Breast Cancer") using unique CTRI Numbers.
*   **Checkpoint & Resume**:
    *   Saves progress every **10 trials** to `ctri_cancer_trials_PROGRESS.csv`.
    *   Auto-resumes from where it left off in case of interruptions.
*   **Crash Recovery**: Automatically detects browser crashes, popups, or network timeouts and restarts the session seamlessly.
*   **Rate Limiting**: Includes configurable delays (default 2s) to respect server load and avoid IP bans.

## 🛠️ Installation

### Prerequisites
*   **Python 3.8+**
*   **Google Chrome** (latest version)

### Steps
1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/ctri-scraper.git
    cd ctri-scraper
    ```

2.  **Install dependencies:**
    ```bash
    pip install selenium pandas pdfplumber webdriver-manager requests
    ```

## 💻 Usage

1.  **Run the scraper:**
    ```bash
    python ctri_scraper_final.py
    ```

2.  **Manual CAPTCHA Step**:
    *   The browser will open and navigate to the CTRI search page.
    *   **Enter the CAPTCHA manually** in the browser window and click "Search".
    *   Once the search results load, return to your terminal and press **ENTER**.

3.  **Sit back and relax**:
    *   The bot will iterate through all categories (Public Title, Scientific Title, etc.).
    *   It opens each trial -> Generates PDF -> Extracts Data -> Saves to CSV.
    *   Real-time progress is shown in the terminal with emoji indicators:
        *   ✅ **PDF**: Successful extraction
        *   🔄 **Duplicate**: Skipped duplicate trial
        *   💾 **Saved**: Progress saved to disk

## 📂 Output

*   **`ctri_cancer_trials_FINAL.csv`**: The master dataset containing all unique, fully scrapped trials.
*   **`ctri_cancer_trials_PROGRESS.csv`**: Intermediate file for resuming interruptions.
*   **`ctri_final.log`**: Detailed logs for debugging.

## 📊 Data Fields (60+ Columns)
The scraper captures granular details including:
*   **Core IDs**: `CTRI Number`, `Registration Date`, `Secondary IDs`
*   **Study Details**: `Public/Scientific Title`, `Study Design`, `Phase`, `Type of Trial`
*   **Sponsors**: `Primary Sponsor`, `Secondary Sponsor`, `Funding Source`
*   **Contacts**: `Principal Investigator`, `Contact Person` (Name, Email, Phone, Address)
*   **Design**: `Randomization Method`, `Blinding`, `Concealment`, `Intervention Details`
*   **Enrollment**: `Target Sample Size`, `Final Enrollment (Total/India)`, `Recruitment Status`
*   **Outcomes**: `Primary Outcome Measures`, `Secondary Outcome Measures`, `Timepoints`
*   **Ethics**: `Committee Name`, `Approval Status`

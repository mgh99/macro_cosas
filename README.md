# 📊 Macro Strategy Engine

An interactive macroeconomic analytics engine designed to support strategic decision-making for corporate leadership in the apparel and luxury industry.

Fetches structured macro, demographic, and tourism data from multiple public APIs, transforms it into analytical views (CSV & Excel), and optionally generates executive-level AI briefings using Mistral.

## 🚀 What This Project Does

The engine:

- Profiles
   - Europe
   - World

- 📥 Fetches structured data from:
   - Eurostat -> (Europe and World)
   - OECD -> (SDMX API) (Europe and World)
   - IMF -> (Europe and World)
   - Un Tourism -> (World)
   - United Nations -> (World)
   - WorldBank -> (World)

- 🧱 Organizes data into analytical frameworks:
   - Economics
   - Demographics
   - Tourism
   - Nuts (only World)

- 📊 Generates:
   - Clean long-format CSV datasets
   - Excel workbooks per framework
   - Single-sheet consolidated workbook
   - Tourism seasonality KPI
   - Top origin markets analysis
   - AI-generated executive briefings (optional)

- 🧠 Produces structured C-level narratives using Mistral AI (if enabled)

## 🛠 Installation on a New Computer
1️. Clone the repository

```bash
git clone https://github.com/mgh99/macro_cosas
cd macro_cosas
```

2️. Create virtual environment

```bash
python -m venv .venv
```

Activate:

**Windows**

```bash
.venv\Scripts\activate
```

**Mac/Linux**

```bash
source .venv/bin/activate
```

3. Install dependencies

```bash
pip install -r requirements.txt
```

4. Configure environment variables (for AI)

Create a `.env` file in the root folder:

```bash
MISTRAL_API_KEY=your_api_key_here
MISTRAL_MODEL=mistral-small-2506
```

If AI is disabled in the menu, the API key is not required.

## ▶️ How to Run

**Recommended (Interactive Menu)**

```bash
python cli_menu.py
```

This launches the guided menu interface.

**Developer Mode (Non-interactive)**

```bash
python run.py
```

Uses default parameters defined inside `run.py`.

## 🧭 Interactive Menu Options

When running `cli_menu.py`, the following steps appear:

**1. Excel Warning**

The tool reminds the user to close any previously generated Excel files.

Excel files must be closed before running, or file-write errors may occur.

**2. Country Selection 🌍**

You can enter:

- Full names: Spain
- Local names: España
- ISO2 codes: ES
- ISO3 codes: ESP
- Multiple countries separated by comma:

```bash
Spain, France, DEU
```

The engine automatically:
- Normalizes accents
- Converts to ISO2
- Removes duplicates
- Applies YAML-defined aliases (config/country_aliases.yaml)

**3a. Framework Selection Europe🧱**

Choose:

- Economics
- Demographics
- Tourism
- ALL frameworks
- Or a custom combination (e.g. 1,3)

**3b. Framework Selection World🧱**

Choose:

- Economics
- Demographics
- Tourism
- Nuts3
- ALL frameworks
- Or a custom combination (e.g. 1,3)

Note: If you select the option: Nuts3 or All frameworks the next question is:

🏙️  Enter NUTS3 regions/cities (comma separated, e.g. Madrid, Barcelona, ES300) or press ENTER to skip:

**4. AI Briefings 🤖**

Enable or disable AI executive reports.

If enabled:

- Generates structured executive narratives per country
- Stored in dedicated folders per framework

If disabled:

- No API calls are made
- Data files are still generated

**5. Output Type 📦**

Choose what to generate:

- Full package (CSV + Excel + single sheet + AI)
- CSV only
- Excel by indicator only
- Single-sheet Excel only
- Debug run (no files written)

**6. Output Directory 📁**

Choose:

- Default (./outputs)
- Desktop
- Custom path

Folders are automatically created if missing.

## ⚙️ Configuration Files

Located in `/config/profiles`:

- `frameworks.yaml` → Define/edit indicators
- `prompts.yaml` → AI narrative prompts
- `country_aliases.yaml` → Country normalization overrides

You normally only need to modify:

```bash
frameworks.yaml
```

No Python changes required to adjust indicators.

## 🔐 Notes on Stability

- OECD requests use gzip compression to reduce timeout risk.
- AI generation includes internal error handling.
- If AI API returns 500 error, re-running typically resolves it.

## 🎯 Recommended Usage Pattern

For business users:

```bash
1. Run cli_menu.py
2. Select countries
3. Select frameworks
4. Enable AI (optional)
5. Choose output location
6. Run
```

No Python editing required.

# Microsoft 365 Copilot Viva Insights Assisted Metrics Fix Toolkit

Restore accurate Microsoft 365 Copilot assisted and meeting-hours metrics in Viva Insights exports with an automated Python/PowerShell toolkit that implements [Microsoft's Viva Insights assisted metrics fix (2025)](https://learn.microsoft.com/microsoft-365/admin/activity-reports/microsoft-365-copilot-assisted-metrics-fix). The tool expects your input CSV to contain **August 2025 activity** (reference window) and **September–October 2025 activity** (target window) so it can apply the official multiplier correctly.

---

## Why this exists
Between September and October 2025, Viva Insights underreported Copilot-assisted meeting hours. Microsoft's official guidance is to apply a multiplier derived from August 2025 activity to reconcile those metrics. Doing this manually is error-prone, especially when working with large CSV exports or custom pipelines. This project automates the entire remediation and validation process so you can:

- **Apply** the official workaround in seconds.
- **Verify** the corrected file row-by-row to ensure all calculations follow the Microsoft formula.
- **Preserve** every other column in your export (including custom columns) untouched.

---

<a id="prerequisites"></a>
<details>
<summary><strong>⚙️ Prerequisites</strong></summary>

- Windows, macOS, or Linux (PowerShell launcher requires Windows PowerShell 5.1+ or PowerShell 7+).
- Viva Insights CSV export that already includes both the **August 2025 reference data** and the **September–October 2025 target data**. The tool produces a corrected CSV with the same schema and all rows intact, adjusting only the Copilot assisted meeting-hour and value metrics noted below.

> **Tip:** Both scripts automatically detect required modules and install them (including Python via `winget` when launched from PowerShell).

</details>

---

<a id="adjusted-metrics"></a>
<details>
<summary><strong>📊 Adjusted metrics</strong></summary>

During correction the toolkit recalculates **only** the metrics below; every other column (including custom fields) is copied through unchanged so the corrected CSV mirrors your input file:
- `Total Meeting hours summarized or recapped by Copilot`
- `Copilot assisted hours`
- `Copilot assisted value` (if present)

All other columns—standard or custom—are passed through exactly as they appear in the original CSV.

</details>

---

<a id="required-columns-schema-checklist"></a>
<details>
<summary><strong>🧾 Required columns (schema checklist)</strong></summary>

To run the fix and its automatic validation, your Viva Insights CSV must include:

- `Total Meeting hours summarized or recapped by Copilot`
- `Copilot assisted hours`
- `Intelligent recap actions taken`
- `Summarize meeting actions taken using Copilot in Teams`
- `MetricDate`

If the export also contains `Copilot assisted value`, the toolkit will recompute it; otherwise that metric is skipped. The scripts recognize common column aliases (like “Intelligent recap actions taken using Copilot”) and report which names were matched. When a required column is missing, the run pauses with a summary of absent fields so you can adjust the CSV or rerun with `--accept-partial`/`-AcceptPartial` if you intentionally want to proceed with a subset.

</details>

---

<a id="running-the-fix"></a>
<details>
<summary><strong>🚀 Running the fix vs. running validation</strong></summary>

The toolkit exposes the same options through both the Python script and the PowerShell launcher. Choose whichever workflow suits your environment; the behavior is identical. The PowerShell launcher still shells into the Python script (so `fix_viva_export.py` must remain alongside it), while running the Python script directly has no dependency on the PowerShell wrapper.

> **Important:** Running the fix without **-Test/--test** automatically executes validation against the pre-fix file and prints the full results in the summary. Reserve **-Test/--test** for validation-only runs (you supply **-Original/--original** and **-Corrected/--corrected**).

| Scenario | PowerShell launcher | Python script |
| --- | --- | --- |
| **Apply the fix + auto-validation** | `\.\Fix-VivaExport.ps1 -Input <path\to\export.csv>` | `python fix_viva_export.py --input <path/to/export.csv>` |
| **Validation-only run** | `\.\Fix-VivaExport.ps1 -Test -Original <orig.csv> -Corrected <fixed.csv>` | `python fix_viva_export.py --test -original <orig.csv> -corrected <fixed.csv>` |

#### Common options
| Purpose | PowerShell | Python | Default |
| --- | --- | --- | --- |
| Override reference window | `-SourceStart`, `-SourceEnd` | `--source-start`, `--source-end` | `2025-07-27 to 2025-08-30` |
| Override corrected window | `-TargetStart`, `-TargetEnd` | `--target-start`, `--target-end` | `2025-08-31 to 2025-11-01` |
| Switch granularity (weekly/monthly) | `-Granularity weekly` or `-Granularity monthly` | `--granularity weekly` or `--granularity monthly` | `weekly` |
| Quiet mode (suppress summary output) | `-Quiet` | `--quiet` | Off (shows progress) |
| Auto-continue when metrics missing | `-AcceptPartial` | `--accept-partial` | Off (requires full metric set) |
| Validation tolerance override | *(Use `-Tolerance` with `-Test`)* | `--tolerance 0.000001` | `0.000001` |
| Validation-only run | `-Test` *(requires `-Original`, `-Corrected`)* | `--test` *(requires `--original`, `--corrected`)* | Runs validation instead of applying fix |
| Validation source file | `-Original <path>` | `--original <path>` | Required when using validation-only run |
| Validation corrected file | `-Corrected <path>` | `--corrected <path>` | Required when using validation-only run |

Notes:
- When **-Test/--test** is present, both **-Original/--original** and **-Corrected/--corrected** are required.
- When running the fix without **-Test/--test**, the script automatically validates the new CSV against the original and prints the full validation report in the summary (respecting **--tolerance**). Use **-Test/--test** when you need validation-only runs.
- When running the fix, **--output/-Output** is optional; the default output is `<input>_corrected_YYYYMMDD_HHMMSS.csv`. A log is always written alongside the corrected file, reusing the same path/name stem with a `.log` extension.
- The PowerShell launcher automatically locates or installs Python (using `winget`) and forwards the appropriate switches to the Python script.

</details>

---

<a id="example-workflow"></a>
<details>
<summary><strong>🧪 Example workflow</strong></summary>

1. **Apply the fix** (generates a new CSV + log + validation summary):
   ```powershell
   .\Fix-VivaExport.ps1 -Input "reference\sample_viva_export.csv"
   ```
   ```bash
   python fix_viva_export.py --input reference/sample_viva_export.csv
   ```

2. **(Optional) Re-run standalone validation** using saved exports (handy for pipelines or historical comparisons):
   ```powershell
   .\Fix-VivaExport.ps1 -Test -Original "reference\sample_viva_export.csv" -Corrected "reference\sample_viva_export_corrected_<timestamp>.csv"
   ```
   ```bash
   python fix_viva_export.py --test -original reference/sample_viva_export.csv \
                              -corrected reference/sample_viva_export_corrected_<timestamp>.csv
   ```

3. **Review the log** (same folder as the corrected file) for multiplier, rows updated, and any skipped metrics.

</details>

---

<a id="validation-output"></a>
<details>
<summary><strong>📈 Validation output explained</strong></summary>

- **Multiplier**: Derived from your reference window; confirms the same calculation the fix uses.
- **Formula Δ max**: Verifies meeting hours equal multiplier * corrected actions; should remain extremely small.
- **Assisted Δ max**: Ensures `Copilot assisted hours` changed by the same amount as meeting hours.
- **Actions Δ max**: Confirms action counts never changed.
- **Meeting/Assisted totals**: Shows how many hours were restored across the corrected window.

If any value exceeds the tolerated range, the script exits with an error and highlights the issue so you can investigate specific rows.

</details>

---

<a id="troubleshooting"></a>
<details>
<summary><strong>🛠️ Troubleshooting</strong></summary>

- **Missing columns?** You need `Total Meeting hours summarized or recapped by Copilot`, `Copilot assisted hours`, `Intelligent recap actions taken`, `Summarize meeting actions taken using Copilot in Teams`, and `MetricDate`. `Copilot assisted value` is optional. The script lists absent fields before continuing; the full reference lives in the schema checklist section. Override with `--accept-partial`/`-AcceptPartial` only if you're intentionally running with partial data.
- **Custom columns?** They're preserved exactly; the script only rewrites the official Copilot metrics noted earlier.
- **No Python installed?** Run the PowerShell launcher—it bootstraps Python 3.11+ via `winget` if needed.

</details>

---

<a id="license-support"></a>
<details>
<summary><strong>📬 License & Support</strong></summary>

License: MIT

Feedback? Email the Microsoft Copilot Growth ROI Advisory Team at [copilot-roi-advisory-team-gh@microsoft.com](mailto:copilot-roi-advisory-team-gh@microsoft.com).

</details>

---

<a id="useful-links"></a>
<details>
<summary><strong>🔗 Useful links</strong></summary>

- [Microsoft Learn: Viva Insights alternate calculation for total meeting hours summarized/recapped](https://learn.microsoft.com/en-us/viva/insights/org-team-insights/alternate-calculation-total-meeting-hours-summarized-recapped)

</details>

name: Transfer Portal Scraper

on:
  workflow_dispatch:
    inputs:
      limit:
        description: "Max athletes to process (blank = all unchecked)"
        required: false
        default: ""
      dry_run:
        description: "Dry run (no DB writes)"
        type: boolean
        default: false
  schedule:
    # Run weekly on Mondays at 6am UTC (transfer windows are active May-Aug)
    - cron: "0 6 * * 1"

jobs:
  scrape:
    runs-on: ubuntu-latest
    timeout-minutes: 360

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install requests beautifulsoup4 supabase

      - name: Run transfer portal scraper
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
        run: |
          ARGS=""
          if [ "${{ inputs.dry_run }}" = "true" ]; then ARGS="$ARGS --dry-run"; fi
          if [ -n "${{ inputs.limit }}" ]; then ARGS="$ARGS --limit ${{ inputs.limit }}"; fi
          python scraper/transfer_portal_scraper.py $ARGS

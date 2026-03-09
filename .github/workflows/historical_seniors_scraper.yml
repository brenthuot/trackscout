name: Historical Seniors Scraper

on:
  workflow_dispatch:
    inputs:
      years:
        description: 'Graduation spring years to target (space-separated, e.g. "2023 2024 2025")'
        required: false
        default: "2023 2024 2025"
      limit:
        description: 'Max schools to process (leave blank for all)'
        required: false
        default: ""
      dry_run:
        description: 'Dry run (no DB writes)'
        type: boolean
        required: false
        default: false

  # Run once per quarter to catch any stragglers
  schedule:
    - cron: "0 8 1 */3 *"   # 8am UTC on the 1st of every 3rd month

jobs:
  scrape:
    name: Scrape historical seniors
    runs-on: ubuntu-latest
    timeout-minutes: 360   # up to 6 hours (full run ~3-4h for all schools)

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install requests beautifulsoup4 supabase

      - name: Run historical seniors scraper
        env:
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: ${{ secrets.SUPABASE_SERVICE_KEY }}
        run: |
          ARGS=""
          if [ "${{ github.event.inputs.dry_run }}" = "true" ]; then ARGS="$ARGS --dry-run"; fi
          if [ -n "${{ github.event.inputs.limit }}" ]; then ARGS="$ARGS --limit ${{ github.event.inputs.limit }}"; fi

          # Build --years argument
          YEARS="${{ github.event.inputs.years }}"
          if [ -n "$YEARS" ]; then
            YEAR_ARGS=""
            for y in $YEARS; do YEAR_ARGS="$YEAR_ARGS $y"; done
            ARGS="$ARGS --years$YEAR_ARGS"
          fi

          python scraper/historical_seniors_scraper.py $ARGS

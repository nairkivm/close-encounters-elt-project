# Close Encounters ELT Project

This is a project to build and ELT data pipeline from NASA based on NASA JPL's SBDB Close-Approach Data API.

How to run this project locally:
- Create virtual environment `python -m venv venv`
- Install requirements.txt `pip install -r requirements.txt`
- Make a `temp` directory (acting as a data lake)
- If you want to run the task using local scheduler, in `etl.py` file, set `local_scheduler` `FALSE` and then run `./run_etl.sh`
- If you want to run task using central scheduler and access luigi UI
    - Set `local_scheduler` `TRUE`
    - Run this to allow Luigi scheduler interface `sudo ufw allow 8082/tcp`
    - Run the scheduler by executing `sudo sh -c ". {your-venv}/bin/activate ;luigid --background --port 8082"`
    - Execute `./run_etl.sh`

## Data Preview
![Preview Data Captured via PgAdmin](/pgadmin-screenshot-1721875797027.png)

## Luigi UI
![Luigi UI - 1](/luigiui-screenshot-1721875797024.png)
![Luigi UI - 2](/luigiui-screenshot-1721875797025.png)
![Luigi UI - 3](/luigiui-screenshot-1721875797026.png)

## Dashboard (created via Power BI)
![Dashboard created via Power BI](/close-encounters-2-1.png)
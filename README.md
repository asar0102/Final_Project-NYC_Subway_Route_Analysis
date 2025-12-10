Title: Predicting Fastest NYC Subway Routes Through Integrated Transit Data


The original raw data (gtfs_subway.zip) is omitted. It can be downloaded at: https://catalog.data.gov/dataset/mta-general-transit-feed-specification-gtfs-static-data/resource/95615476-892e-47cd-9251-7acb31ed1698

The created database (subway_network.db) and trained model (subway_time_model.pkl) are also omitted due to their file sizes (they exceed GitHub's size limits).

Project Execution Steps:
1) After obtaining the raw data zip file, run clean_gtfs.py
2) Once clean data is obtained, run gtfs_to_sql.py. This creates subway_network.db
3) Run gtfs_astar.py
4) Run train_model.py, this will create subway_time_model.pkl

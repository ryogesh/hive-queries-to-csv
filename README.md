# Capture Hive database queries information from the log file, and save as a csv file
## Overview

This repository contains the script to capture information on Hive database queries from the Hiveserver2 log file. Extracts Hive queries and it's metrics from HiveServer2 log file. Can be scheduled to gather metrics periodically. It's similar to the python script on hive-mr-tez-csv repository.

## Installation
- A Linux/Unix based system
- [Julia](https://julialang.org/downloads/) v1.4.2


### Additional packages used
- [JSON](https://github.com/JuliaIO/JSON.jl)
- [ArgParse](https://github.com/carlobaldassi/ArgParse.jl)


### Running the script
- Capturing Hive queries. Creates a csv(default), json or both file with compile, execution times, queryuser, queuename, full query and yarn application id.

   ```
   $ julia hive_qry_csv.jl
   ```    
   
Additional information on the script

    ```
    $ julia hive_qry_csv.jl -h
    usage: hive_qry_csv.jl [--logfile LOGFILE] [--dir DIR]
                           [--periodic PERIODIC] [--format FORMAT] [-h]

    Save user queries execution metrics to a csv file

    optional arguments:
      --logfile LOGFILE    HiveServer2 Log file location,
                           Default:/home/theia/hiveserver2.log.2020-03-27_1
                           (default:
                           "/home/theia/hiveserver2.log.2020-03-27_1")
      --dir DIR            Folder to save the csv files,
                           Default:/home/theia (default: "/home/theia")
      --periodic PERIODIC  Capture queries periodically (y), use with
                           scheduler, Default:n (default: "n")
      --format FORMAT      File format(csv:c, json:j, both:b,
                           Default:csv(c) (default: "c")
      -h, --help           show this help message and exit

    Make sure the program has read access to the hiveserver2 log file

    ```
        
Scheduling using cron to run every 15 minutes

  ```
  */15 * * * * julia hive_qry_csv.jl --format b --periodic y >> ~/hive_queries.log 2>&1
  ``` 

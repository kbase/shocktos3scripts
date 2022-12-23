# Shock to S3 scripts

Scripts for moving / transforming KBase data files and database records as part of the Shock to S3 data storage conversion.

# Transferring S3 to S3
* Use syncs3tos3.py
* fill out syncs3blobstore.ini example config file
* you may need to edit /etc/hosts for mongo replicas to correctly get queried e.g

```
X.X.1.1 next01
X.X.1.6 next06
```
* for the first run, it will create a "now" file with the timestamp. You can cancel the run, and edit the file, and set the date to the year 2000
* you can monitor progress in the rsync log at blobstoresync.log 

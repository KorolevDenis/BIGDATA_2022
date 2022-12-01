import subprocess
import os
import sys

years = [i for i in range(int(sys.argv[1]), int(sys.argv[2]))]
years.reverse()
proc_numb = int(sys.argv[3])

for ind, year in enumerate(years):
    os.system("curl http://localhost:6800/schedule.json -d project=APIScopusCrawler -d spider=scopus_api -d domains='['{}', '{}', '{}']'".format(year, proc_numb, ind % proc_numb))


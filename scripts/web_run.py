import subprocess
import os
import sys

proc_numb = int(sys.argv[3])

for numb in range(0, proc_numb):
    os.system("curl http://localhost:6800/schedule.json -d project=WebScopusCrawler -d spider=web_spider -d domains='['{}', '{}', '{}']'".format(proc_numb, numb, sys.argv[1], sys.argv[2]))


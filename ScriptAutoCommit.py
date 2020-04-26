import os
import time

os.system("git add .")
os.system('git commit -m "Autocommit {}"'.format(time.strftime("%Y%m%d %H%M")))
os.system("git push -u origin master")



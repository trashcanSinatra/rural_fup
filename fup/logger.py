import webbrowser
import time
import shutil
import os



class Logger:
    
    def __init__(self, report, logger):
        self._report = report
        self._log_file = logger

    def debug(self, msg):
        with open(self._log_file, "a") as file:
            file.write("{0}\n".format(msg))
    
    def report(self, msg):
        with open(self._report, "a") as file:
            file.write("{0}\n".format(msg))


    def refresh(self):
       with open(self._report, "w") as file:
           file.truncate()
       with open(self._log_file, "w") as file:
           file.truncate()


    def open(self):
        # webbrowser.open(self._report)
        cmd = "notepad.exe {0}".format(self._report)
        os.system(cmd)

        

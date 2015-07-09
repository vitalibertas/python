#!/usr/bin/python33-virtual-hadoop
import datetime
import os

class StateFile:

    def __init__(self, state_file):
        self.state_file = state_file

    def lockState(self):
        if os.path.isfile(self.state_file):
            os.rename(self.state_file, "{0}.lock".format(self.state_file))
            result = "success"
        elif os.path.isfile("{0}.lock".format(self.state_file)):
            result = "{0} is either in use or an error occurred previously.".format(self.state_file)
        else:
            result = "Could not find {0}.".format(self.state_file)
        return result

    def unlockState(self):
        if os.path.isfile("{0}.lock".format(self.state_file)):
            os.rename("{0}.lock".format(self.state_file), self.state_file)
            result = "success"
        elif os.path.isfile(self.state_file):
            result = "success"
        else:
            result = "Could not find {0}.lock.".format(self.state_file)
        return result

    def readState(self):
        with open("{0}.lock".format(self.state_file)) as state:
            process_day = state.readline()
        return datetime.datetime.strptime(process_day.replace('\n', ''),'%Y-%m-%d').date()

    def writeState(self, process_day):
        process_day = process_day.strftime('%Y-%m-%d')
        with open("{0}.lock".format(self.state_file), 'w') as state:
            state.write(process_day)

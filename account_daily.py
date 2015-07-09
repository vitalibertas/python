#!/usr/bin/python33-virtual-hadoop
import time
import sys
import datetime
from statefile import StateFile
from corejobs import CoreJobs

def incrementDay(process_day):
    next_day = process_day + datetime.timedelta(days=1)
    return next_day

def unlockState(state, job):
    unlock_file = state.unlockState()
    if unlock_file != 'success':
        job.sendMail("Unable to unlock {0}.".format(state.state_file), unlock_file)
        sys.exit()

def iterateList(events, job, process_day):
    for hive_script in events:
        tryEventInsert(job, hive_script, process_day)
        time.sleep(2)

def tryEventInsert(job, hive_script, process_day):
    print('Running: ', hive_script)
    hive_result = job.executeHive(hive_script, process_day)
    if hive_result['error'] > 0:
        job.sendMail("Unable to execute {0}".format(hive_script), hive_result)
        sys.exit()

def checkReplicationDate(job):
    command_result = job.checkHadoopFileSystem('/directory/database/tableFile', '{0}'.format(datetime.date.today()))
    if int(command_result['output'].rstrip("\n")) == 0:
        command_result = job.checkHadoopFileSystem('/directory/database/tableFile', '{0} 2[0123]'.format(datetime.date.today() - datetime.timedelta(days=1)))
        if int(command_result['output'].rstrip("\n")) == 0:
            print('Replication Check: ', command_result['output'].rstrip("\n"))
            job.sendMail("Hadoop Replication Date Not Current.", command_result)
            sys.exit()

def main():

    state_file = "/direcory/productStateFile.state"
    state = StateFile(state_file)
    job = CoreJobs("")
    eventList = ["/direcory/script.sql", "/direcory/script2.sql", "/direcory/script3.sql"]

    print('State file: ', state_file)

    checkReplicationDate(job)

    #get the state file
    lock_file = state.lockState()
    if lock_file != 'success':
        job.sendMail("Unable to lock {0}".format(state_file), lock_file)
        sys.exit()
    else:
        process_day = state.readState()

    while (process_day < datetime.date.today()):
        print('Running the following date: ', process_day)
        iterateList(eventList, job, process_day)
        next_day = incrementDay(process_day)
        state.writeState(next_day)
        process_day = next_day

    unlockState(state, job)

if __name__ == "__main__":
    main()

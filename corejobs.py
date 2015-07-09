#!/usr/bin/python33-virtual-hadoop
import subprocess
import platform
import email.utils
from smtplib import SMTP

class CoreJobs:

    def __init__(self, emailList):
        coreList = ["email@domain.com","othermail@domain.com"]
        if emailList != "":
            for mail in emailList:
                coreList.append(mail)
        self.email = coreList

    def sendMail(self, subject, message_body):
        hostname = platform.node()
        sender = "hadoop-noreply@{0}".format(hostname)
        message = "Date: {0}\r\nFrom: JobFailure <{1}>\r\nTo: {2}\r\nSubject: {3}\r\n\r\n{4}\r\n".format(email.utils.formatdate(localtime=True), sender, ", ".join(self.email), subject, message_body)
        with SMTP('localhost') as smtp:
            smtp.sendmail(sender, self.email, message)

    def addPartition(self, partition_name, process_day):
        command = "userWithInsertPrivs hive -e \"SET hive.execution.engine = tez; USE database_name; ALTER TABLE {0} ADD PARTITION (year={1}, month={2}, day={3}) LOCATION \'/feedDataWithPartitions/{0}/year={1}/month={2}/day={3}\';\"".format(partition_name, process_day.year, process_day.month, process_day.day)
        result = self.executeCmd(command)
        return result

    def addPartitionMapping(self, partition_name, process_day):
        command = "userWithInsertPrivs hive -e \"SET hive.execution.engine = tez; USE database_name; ALTER TABLE {0} ADD PARTITION (year={1}, month={2}, day={3}) LOCATION \'/feedDataWithPartitions/{0}_mapping/year={1}/month={2}/day={3}\';\"".format(partition_name, process_day.year, process_day.month, process_day.day)
        result = self.executeCmd(command)
        return result

    def executeHive(self, hive_script, process_day):
        command = "userWithInsertPrivs hive --define YEAR={0!r} --define MONTH={1!r} --define DAY={2!r} --define START_DATE={3!r} -f {4}".format(process_day.year, process_day.month, process_day.day, process_day.isoformat(), hive_script)
        result = self.executeCmd(command)
        return result

    def checkHadoopFileSystem(self, filesystem, search):
        command = "hadoop fs -ls {0} | grep -c '{1}'".format(filesystem, search)
        result = self.executeCmd(command)
        return result

    def executeCmd(self, command):
        try:
            cmd = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
            output = cmd.communicate()[0]
        except subprocess.CalledProcessError as e:
            print("Error: ", e.returncode, "! ", command, " did not execute correctly.")

        result = {'command' : command, 'output' : str(output, encoding='utf-8'), 'error' : cmd.returncode}
        return result

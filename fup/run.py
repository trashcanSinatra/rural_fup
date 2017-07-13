import os
import math
import shutil
import json
import time
import re
from decimal import Decimal
from random import randint
from time import strftime as sft
from string import Template
from .logger import Logger


# Set directory Contants:
PWD = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = (PWD + '/../')
DATA_DIR = (ROOT_DIR + 'data/')


# Set Module level constans from config.json
with open(PWD + '/config.json') as configFile:
    config = json.loads(configFile.read())
    EXT_FOLDER = config['extract_folder']
    EXT_FILE = config['extract_file']
    BACKUP_FILE = config['backup_file']
    UPDATES_FILE = config['raw_fup']
    REPORT_FILE = config['report_file']
    LOG_FILE = config['log_file']
    IMP_FOLDER = config['import_folder']
    ATT_COUNT = config['att_count']
    DRAWER = config['info']['drawer']
    FILE = config['info']['file']
    NUMBER = config['info']['number']
    NAME = config['info']['name']


# Creaete logging object.
log = Logger(ROOT_DIR + REPORT_FILE, ROOT_DIR + LOG_FILE)
log.refresh()
log.report("REPORT FOR: {0}".format(sft("%d/%m/%Y")))


# get_last_row: Determines the number of the last row
# when the attribute count is divided by five
def get_last_row(items):
    row = int(str(math.floor(ATT_COUNT / 5))[0:1])
    if items != 0:
        row = row + 1
    return row

# How many attributes will be in the last row.
ITEMS = int(ATT_COUNT % 5)
# Number of the last row given how many ITEMS.
LAST_ROW = get_last_row(ITEMS)

log.debug("\n\nDEBUG FOR:{}".format(sft("%d/%m/%Y")))
log.debug("LAST ROW: {}".format(LAST_ROW))
log.debug("ITEMS in last row: {}".format(ITEMS))


# Get unique FUP ID.
FUP_NAME = "{0}{1}".format(str(math.floor(randint(1345, 9999) * ((8585929 ^ 2) / 3.4))), '.fup')
log.report("FUP_NAME: {}".format(FUP_NAME))


class Extractor:

    def __init__(self):
        self.backups = dict()

    def get_new_policies(self):
        backups = list()
        raw_updates = list()

        with open(DATA_DIR + BACKUP_FILE) as file:
            for line in file:
                backups.append(line)
        with open(EXT_FOLDER + EXT_FILE) as file:
            for line in file:
                raw_updates.append(line)

        start = time.clock()
        new_polices = [line for line in raw_updates if line not in backups]
        end = time.clock()  
          
        log.report("DIFF TIME: {} seconds".format(end - start))
        log.report("New/Changed Policies: {0:,}".format(len(new_polices)))

        return new_polices


    def write_new_policies(self, policies):
        if len(policies) > 0:
            with open(DATA_DIR + UPDATES_FILE, "w") as file:
                for line in policies:
                    file.write(line)


class Manager:

    """ Manager is responsible for getting the csv file, matching
        the indexes from config.json to the data in each row, and
        putting each string into an array to get ready for
        further parsing by the Processor class.  """

    # Sets config, puts unedited data into raw_updates array.
    def __init__(self):
        self.attribute_idx = dict()
        self.raw_updates = list()
        self.prepped_updates = list()
        self.__config_opt()

        with open(DATA_DIR + UPDATES_FILE) as file:
            for line in file:
                self.raw_updates.append(line)
            log.debug("self.raw_updates: {}".format(self.raw_updates))


    #  prep_updates: sets all values for fup string.  If the key from
    #  attribute_idx matches it grabs the data position on the unpacked_line
    #   array that is pointed to by the matching key
    def prep(self):
        for line in self.raw_updates:
            unpacked_line = line.split(','); del unpacked_line[-1]
            fup = "~".join([DRAWER, NUMBER, NAME, FILE])
            for key, idx in self.attribute_idx.items():
                if key == NUMBER or key == NAME:
                    fup = fup.replace(key, unpacked_line[idx - 1])
                else:
                    for label in self.attribute_idx.keys():
                        if label == key:
                            fup = self.__insert(fup, label, unpacked_line, idx)
            self.prepped_updates.append(fup)
        self.__strip_quotes()
        log.debug("self.prepped_updates: {}".format(self.prepped_updates))
        return self.prepped_updates


    # Subs the last attribute plus the file name, back to replace again.
    def __insert(self, fup_string, current_label, current_line, current_idx):
        fup_string = re.sub(FILE + '$', \
                current_label.upper() + "=" + \
                current_line[current_idx - 1] + \
                "~" + DRAWER, fup_string)
        return fup_string


    # Runs through all items in config['indexes'], and places
    # the same dict inside self.attribute_idx
    def __config_opt(self):
        configFile = open(PWD + '/config.json')
        config = json.load(configFile)
        self.attribute_idx = {key: pos for key, pos in config['indexes'].items()}


    def  __strip_quotes(self):
        for i, update in enumerate(self.prepped_updates):
            self.prepped_updates[i] = "".join(filter(lambda ch: ch != "\"", self.prepped_updates[i]))


class Processor:
    """ TextBuilder:  Responsible for creating the fup template based
        on the number of attributes. Substituting values for markers in
        the template, and writing the fup file accordingly.
    """

    # create_fup: generates a new template based on template_base()
    # Builds the argument list, substitutes values, and writes the line.
    @staticmethod
    def create(queue):
        start = time.clock()
        log.report("FUPS written: {0:,}".format(len(queue)))
        template = Template(Processor.build_template())
        if len(queue) > 0:
            with open(DATA_DIR + FUP_NAME, 'a') as file:
                for row in queue:
                    row = row.split('~')
                    args = Processor.build_args(row)
                    line = template.substitute(**args)
                    file.write(line)
                end = time.clock()
                log.report("CREATION TIME: {} seconds".format(end - start))


    @staticmethod
    def send():
        there_is_fup = os.path.isfile(DATA_DIR + FUP_NAME)
        fup_stuck = os.path.isfile(ROOT_DIR + FUP_NAME)
        if there_is_fup:
            if not fup_stuck:
                shutil.move(DATA_DIR + FUP_NAME, ROOT_DIR)
            else:
                log.report("Error dropping file. Moved to {0}. {1} needs redropped.".format(DATA_DIR, FUP_NAME))
        else:
            log.report("NO fup for you! Come back, one year.")


    @staticmethod
    def build_template():
        base = "$drawer~$policy~$name"
        row = 1
        for i in range(1, ATT_COUNT + 1):
            if i % 5 == 0 and row < LAST_ROW:
                base = base + "~$att" + str(i) + "~$file\n"
                base = base + "$drawer~$policy~$name"
                row += 1
            else:
                base = base + "~$att" + str(i)
        base = base + "~$file\n"
        return(base)


    @staticmethod
    def build_args(row):
        arg = dict()
        arg.update(drawer=row[0])
        arg.update(policy=row[1][:-5]) #  This is to remove the -NNNN in the Rural Client File Code, used for Policy #
        arg.update(name=row[2])
        arg.update(file=row[ATT_COUNT + 3])
        for i in range(1, ATT_COUNT + 1):
            arg.update({"att" + str(i) : row[i + 2]})
        return arg


    @staticmethod
    def write_backups():
        backup_raw = open(DATA_DIR + BACKUP_FILE, "w")
        with open(EXT_FOLDER + EXT_FILE) as file:
            for line in file:
                backup_raw.write(line)
        backup_raw.close()
        open(DATA_DIR + UPDATES_FILE, "w").close()
        log.open()
    

if __name__ == "__main__":
    print("This module is to be imported.")

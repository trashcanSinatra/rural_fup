from fup.run import Extractor
from fup.run import Manager as fetch
from fup.run import Processor as fup


""" Extractor compares the Care Extract file, to the saved
    copy of yesterdays extract (fup.yesterday). It will 
    pull out all new files, and any existing lines that 
    have changed.  It writes those lines to fup.today.
"""
deltas = Extractor()
new_policies = deltas.get_new_policies()
deltas.write_new_policies(new_policies)


""" Manager (fetch) pre-processes raw csv from fup.today """
new_updates = fetch()
updates = new_updates.prep()


""" Processor (fup) writes each line into a template
    matching the IR requirements.  Finally, sending
    to the IR monitored folder
"""
fup.create(updates)
fup.send()
fup.write_backups()


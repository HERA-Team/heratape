from __future__ import annotations

import datetime

from astropy.time import Time
from sqlalchemy import BigInteger, Column, Date, String
from sqlalchemy.orm import Session

from heratape.base import Base, HTSessionWrapper
from heratape.tapes import Tapes, add_tape
import subprocess

from heratape.base import get_heratape_testing_db  # DEPLOY TODO: replace with real DB

TESTING = True
"""
(base) djacobs@herastore01:~$ sudo mtx -f /dev/sg25 status
  Storage Changer /dev/sg25:2 Drives, 24 Slots ( 1 Import/Export )
Data Transfer Element 0:Full (Storage Element 8 Loaded):VolumeTag = 042142L9
Data Transfer Element 1:Full (Storage Element 8 Loaded):VolumeTag = HER002L9
      Storage Element 1:Full :VolumeTag=HER006L9
      Storage Element 2:Full :VolumeTag=HER009L9
      Storage Element 3:Full :VolumeTag=HER012L9
      Storage Element 4:Full :VolumeTag=HER001L9
      Storage Element 5:Full :VolumeTag=HER005L9
      Storage Element 6:Full :VolumeTag=HER008L9
      Storage Element 7:Full :VolumeTag=HER011L9
      Storage Element 8:Empty
      Storage Element 9:Full :VolumeTag=HER004L9
      Storage Element 10:Full :VolumeTag=HER007L9
      Storage Element 11:Full :VolumeTag=HER010L9
      Storage Element 12:Full :VolumeTag=CLN003L1
      Storage Element 13:Full :VolumeTag=HER021L9
      Storage Element 14:Full :VolumeTag=HER018L9
      Storage Element 15:Full :VolumeTag=HER015L9
      Storage Element 16:Full :VolumeTag=HER023L9
      Storage Element 17:Full :VolumeTag=HER020L9
      Storage Element 18:Full :VolumeTag=HER017L9
      Storage Element 19:Full :VolumeTag=HER014L9
      Storage Element 20:Full :VolumeTag=HER022L9
      Storage Element 21:Full :VolumeTag=HER019L9
      Storage Element 22:Full :VolumeTag=HER016L9
      Storage Element 23:Full :VolumeTag=HER013L9
      Storage Element 24 IMPORT/EXPORT:Full :VolumeTag=HER003L9
"""
from heratape.tape_system import list_tapes_in_system, tape_in_drive, update_tapes
from heratape.tapes import list_tapes_in_db, add_new_tapes_to_db

update_tapes(testing=TESTING)
# def add_new_tapes_to_db(tapes,testing=TESTING):
#    #input a list of tape names, eg as output by list_tapes_in_system
#    #tapes will be added to tape table in heratape db
#    # then they will be ready for use by the backup service
#    # return 0 on success
#    addcount = 0
#    added  = []
#    for tape in tapes:
#        add_tape(
#            tape_id=tape,
#            tape_type='lto9',
#            size=1.8e13,
#            purchase_date=Time.now(),
#            testing=testing,
#        )
#        added.append(tape)
#        addcount += 1
#    return 0
#
# def update_tapes():
#    # add anything thats new
#    # skip any cleaner tapes
#    tapes = list_tapes_in_system()
#
#    # get list of tapes already known to us
#    dbtapes = list_tapes_in_db(testing=TESTING)
#    print(f'Tape table has {len(dbtapes)} entries')
#    newtapes = list(set(tapes) - set(dbtapes))
#    #don't add the CLN tapes
#    newtapes = [t for t in newtapes if not t.startswith('CLN')]
#    print(f'adding {len(newtapes)} new tapes')
#    add_new_tapes_to_db(newtapes)

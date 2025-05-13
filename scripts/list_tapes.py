# list tapes in the db and their fullness
import numpy as np
from hera_librarian import LibrarianClient
from heratape.base import get_heratape_testing_db                                                                           
from heratape.files import add_files_to_tape,Files,get_all_jds,\
 set_write_date,query_tape_usage,list_incomplete, update_tape_usage
from heratape.tapes import Tapes,list_tapes_in_db
from heratape.base import HTSessionWrapper
import subprocess,os
from astropy.time import Time                                                                                               
import sys,time
from sqlalchemy.sql import func

from heratape.tape_system import tape_in_drive,query_tape_jukebox,find_empty_slots,unload_tape,load_tape, update_tapes


import logging
from systemd import journal

from sqlalchemy.orm import Session 
TESTING=True
tapes = list_tapes_in_db(testing=TESTING)
print("Tapes in DB")
print('Tape,   Usage TB (out of 18TB)')
for tape in tapes:
    usage = query_tape_usage(tape)
    with HTSessionWrapper(session=Session, testing=testing) as ht_sess:                                                       
        jd_tuple_list = ht_sess.query(Files.jd).where(tape_id=tape).distinct().all()                                          
    jd_list = ','.join([str(val[0]) for val in jd_tuple_list])
    
    print(f'{tape}  {float(usage)/1e12:.2f} {jd_list}')
        


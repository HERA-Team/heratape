import numpy as np
from hera_librarian import LibrarianClient
from heratape.base import get_heratape_testing_db                                                                           
from heratape.files import add_files_to_tape,Files,get_all_jds,set_write_date,query_tape_usage
from heratape.tapes import Tapes                                                                                            
from heratape.base import HTSessionWrapper
import subprocess,os
from astropy.time import Time                                                                                               
import sys,time
from sqlalchemy.sql import func

from heratape.tape_system import tape_in_drive,query_tape_jukebox,find_empty_slots,unload_tape,load_tape


import logging


# some AI bullshyt                                                                                                          
logging.basicConfig(                                                                                                        
    level=logging.DEBUG if os.environ.get('LOG_LEVEL') == 'DEBUG' else logging.INFO,                                        
    format='%(asctime)s - %(levelname)s - %(message)s'                                                                      
)                                                                                                                           
logger = logging.getLogger('retry_failed_tape')


TESTING = True

#recover from an interrupted tape write

# first list all the tapes in the DB with incomplete writes. Hopefully FEW
tape_jds = {}
with HTSessionWrapper(testing=TESTING) as ht_sess: 
    unfinished_tapes = ht_sess.query(func.distinct(Files.tape_id)).where(Files.write_date == None).all()
    logger.info(f'found {len(unfinished_tapes)} tapes with unfinished business')
    for utape in unfinished_tapes:
        unfinished_jds = ht_sess.query(func.distinct(Files.jd)).\
        where(Files.write_date == None).where(Files.tape_id == utape[0]).all() 
        unfinished_jds = [ujd[0] for ujd in unfinished_jds] #why do I get a list of length 1 tuples??
        logger.info(f'Tape ID: {utape[0]} has JDS {unfinished_jds}')
        tape_jds[utape[0]] = unfinished_jds
if len(unfinished_tapes)==0: 
    logger.info('No unfinished files found! Exiting...')
# then prompt user to select a tape to be retried. 
#   NOTE! THIS WILL OVERWRITE THE CONTENTS OF THE TAPE. This is probably what you want to do. 
mytape = input('Enter a tape to be retried:' )
mydrive = np.array(tape_jds[mytape][0]) % 2
IN =  input(f'''Please load {mytape} into drive {mydrive} and rewind it.
             CAUTION. CONFIRM NO ACTIVE BACKUPS IN PROGRESS ON DRIVE {mydrive}. 

             Enter R when ready, A to Abort: ''')
if IN=='A': logger.info("Aborting... "); sys.exit()
if IN!='R': logger.info('Enter R to continue, or A to abort'); sys.exit()
# check that the tape in the drive is the right one
drivetape = tape_in_drive(mydrive)
if mytape != drivetape: 
    logger.info(f'ERROR: found {drivetape} in Drive {mydrive}, please insert {mytape}')


#  LOAD the desired (first unloading whatever is in the drive.)
#  REWIND the tape
logger.info('get the list of files to backup')
with HTSessionWrapper(testing=TESTING) as ht_sess:
    unfinished_files = ht_sess.query(Files.filepath).\
        where(Files.write_date == None).where(Files.tape_id==mytape).all()
    unfinished_files =[uff[0] for uff in unfinished_files]#why a len 1 tuple. again why
logger.info(f'writing {len(unfinished_files)} files to {mytape}')
logger.debug(f'first file in list {unfinished_files[0]}')

#write the file list to be read by tar                                                                                  
filelistfile = f'ht_d{mydrive}_{drivetapeid}_{Time.now().isot}.txt'                                                     
logger.info(f'writing file list to {filename}')                                                                         
F = open(filelistfile,'w')                                                                                                  for i in np.arange(len(files)):                                                                                         
    F.write(f'{files[i]}\n')                                                                                            
    #F.write(f'{drivetapeid},{files[i]}, {obsids[i]}, {start_jds[i]}, {sizes[i]}\n')                                    
F.close()                                                                                                               
if not TESTING:                                                                                                         
    logger.info("running tar: tar -cjf /dev/st{mydrive} -T {filelistfile}")                                             
    tstart = time.time()                                                                                                
    with subprocess.Popen(f'time tar -cf /dev/nst{mydrive} -T {filelistfile} ', shell=True, stdout=subprocess.PIPE) as proc:                                                                                                                        
                lines = proc.stdout.readlines()                                                                         
    logger.info(f'finished in {(time.time() - tstart)/60} minutes')                                                     
else:                                                                                                                   
    logger.info(" TESTING MODE: skipping real tar to tape")                                                             
    logger.debug(f' the tar command: time tar -cf /dev/nst{mydrive} -T {filelistfile}')                                 
                                                                                                                        
logger.info("files written OK")  


logger.info(f'setting write date to {Time.now()}')
file_bases = [os.path.basename(f) for f in unfinished_files]
set_write_date(file_bases,Time.now(),testing=TESTING) 
#  TAR all those files to the taope
#  set the write_date
#  EXUENT




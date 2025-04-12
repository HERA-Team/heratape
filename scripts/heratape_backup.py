# script tool to help find files to backup
# input range of JDs
# output instance fully qualified paths 
import numpy as np
from hera_librarian import LibrarianClient
from heratape.base import get_heratape_testing_db                                                                           
from heratape.files import add_files_to_tape,Files,get_all_jds,set_write_date,query_tape_usage,list_incomplete
from heratape.tapes import Tapes                                                                                            
from heratape.base import HTSessionWrapper
import subprocess,os
from astropy.time import Time                                                                                               
import sys,time
from sqlalchemy.sql import func

from heratape.tape_system import tape_in_drive,query_tape_jukebox,find_empty_slots,unload_tape,load_tape, update_tapes


import logging
from systemd import journal



def find_dups(Xs):
    #return a list of repeat entries
    dups = []
    seen = []
    for x in Xs:
        if x in seen: dups.append(x)
        else: seen.append(x)
    return dups
 #extract the lists metadata items 


jdskip_cache = '/users/djacobs/src/heratape/scripts/hera_jdskip_cache.txt'
LOCAL_CONN_NAME = "local" #TODO: What is this called in the lib config file?e
TESTING=True #if true, use test db, skip actual tape  writing.
DOTAPE = False
TAPESIZE = 18e12

local_client = LibrarianClient(LOCAL_CONN_NAME)
mydrive = 0 # tape drive selector. 0 or 1. TODO make this set the oddness of the jd 
mydrive  = int(sys.argv[1])
if not mydrive in [0,1]: 
    logging.error('input tape drive must be 0 or 1')
    sys.exit(1)

# some AI bullshyt                                                                                                          
logging.basicConfig(                                                                                                        
    level=logging.DEBUG if os.environ.get('LOG_LEVEL') == 'DEBUG' else logging.INFO,                                        
    format='%(asctime)s - %(levelname)s - %(message)s'                                                                      
)                                                                                                                           
logger = logging.getLogger('heratape-drive[{mydrive}]')                 
                                                                                                                            
                                                                                                                            
                                                                                                                            
logger.info("heratape_backup starting")   

logger.info(f'using tape drive {mydrive}')
logger.info('(tape 0 works on even JDs, 1 takes care off odd)')

#accumulate 18TB worth of whole nights.
#write them to a tape known to be blank
# if interrupted, just try again
# avoids writing multiple archives to a single tape, a practice which is avoided by seasoned veterans 
backupsize = 0 # tracks total size of planned backup, in bytes
while(True):
    if backupsize ==0:
        joined_results = [] #this is where we'll accumulate files to backup.
        jds_to_backup = [] #keep a list of the JDS going into this backup
        #otherwise we're in the process of adding days to the planned backup

    # check for incomplete writes (identified as files with no write date)
    incomplete_jds, incomplete_files = list_incomplete()
    incomplete_jds = np.array(incomplete_jds)
    if  np.any(incomplete_jds % 2 == mydrive):
        logger.error(f'undated entries found in heratape.files for jds {incomplete_jds[incomplete_jds % 2 ==mydrive]}')
        logger.error('This error might indicate that tape writing was interrupted. Check tape position.')
        logger.error('TODO: add more helpful printout telling name of suspect tape')
        logger.error('to proceed please delete these records and restart me')
        sys.exit(1)

    #   select JD from a priority scheme that is NOT in the backed up JD list
    priority_list = '/users/djacobs/src/heratape/scripts/hera_backup_jd_priorities.txt'
    logger.info(f'loading priorities from {priority_list}')
    JD_priorities = np.loadtxt(priority_list,delimiter=',')
    if os.path.exists(jdskip_cache):
        jdskip = open(jdskip_cache).readlines()# list of nights with no data
        jdskip = [int(np.floor(float(j.strip()))) for j in jdskip]
        logger.debug(f'skipping {len(jdskip)} jds')
    else:
        jdskip = []

    #dont add the JDs we're already backing up in this round
    jdskip += jds_to_backup 

    #work through priority ranges in chunks, don't assume chunks will come in time order
    #we might backup H1C and THEN go back to do the summers and other off season stuff
    jdtobackup = None
    for jdrange in JD_priorities: 
        jdpriorities = np.arange(jdrange[0],jdrange[1])
        backedupjds = get_all_jds(testing=TESTING)
        jdbackup_list = np.array(list(sorted(list(set(jdpriorities) - set(backedupjds)-set(list(jdskip))))))
        if len(jdbackup_list)==1: 
            jdbackup = jdbackup_list #special case for the LAST DAY to be backedup
            break
        # drive 0 takes the even JDs, drive 1 the odd
        jdbackup_list = jdbackup_list[np.array(jdbackup_list,dtype=int)%(2+mydrive)==0]
        if len(jdbackup_list)<1: continue #this jd range is all DONE
        jdtobackup  = jdbackup_list[-1]#the most recent night waiting to be backed up
    if jdtobackup is None: 
        logger.info(f'All data in the priority list {priority_list} has been backed up!')
        logger.info('Exiting')
        sys.exit() #TODO replace this with an infinite loop for daemon mode.

    #check that the JD has not already been backed up                                                                       
    backed_up_jds = get_all_jds(testing=TESTING)                                                                            
    if jdtobackup in backed_up_jds:
        logger.info(f'JD={jdtobackup} already backed up')
        open(jdskip_cache,'a').writelines([str(int(jdtobackup))+'\n'])
        continue
        

    # next find out how much data is in this JD.
    query = f'{{"start-time-jd-in-range": [{jdtobackup}, {jdtobackup+0.999}]}}'  
    logger.info("using query: " + query)
    local_files = local_client.search_files(query)
    local_sizes = [entry["size"] for entry in local_files["results"]]
    total_size = np.sum([e["size"] for e in local_files["results"]]) #size of selection in bytes                            
    logger.info(f'Found {len(local_files)} files, total size {total_size/1e12}TB')    
    if len(local_sizes)==0:                                                                              
        logger.warn(f'no data found {np.round(jdtobackup)}. skipping!')                                                 
        open(jdskip_cache,'a').writelines([str(int(jdtobackup))+'\n'])                                                  
        continue            


    #add these files to the backup list and go back to data selection again.
    # otherwise, head down to the tape writing!
    if total_size + backupsize < TAPESIZE:
        logger.info('this data will fit on the tape, adding to the backup batch')
        backupsize += total_size #IMPORTANT: This is where we commit to adding this day
        local_instances = local_client.search_instances(query)
        logger.info(f'planned backup size: {backupsize/1e12:.1f} TB')
        logger.info('gathering more file info')
        logger.debug(f'found {len(local_instances["results"])} instances')
        local_obses = local_client.search_observations(query)
        logger.debug(f'found {len(local_obses["results"])} obses')
        # logger.info the gory details.
        local_names = [entry["name"] for entry in local_instances["results"]]
        logger.debug(f'there are {len(set(local_names))} unique file instances')
        
        #extract the lists metadata items 
        local_paths = [entry["full_path_on_store"] for entry in local_instances["results"]]
        local_obsids = [entry["obsid"] for entry in local_files["results"]]
        
        
        logger.info(f'Found {len(local_names)} files, total size {total_size/1e12}TB')
        logger.debug('first 10 duplicates')
        for dup in find_dups(local_names)[:10]:
            logger.debug(dup)
        
        # to build a record in heratape, we need information from Observations, Files, and Instances all zipped together
        # append to the running list of data we're going to backup
        # for each file, find one file instance, and find its matching observation info
        for local_file in local_files["results"]:
            #find a matching obs and instance
            for local_instance in local_instances["results"]:
                if local_instance["name"]==local_file["name"]: break
            for local_obs in local_obses["results"]:
                if local_file["obsid"]==local_obs["obsid"]: break
            joined_results.append((local_instance,local_file,local_obs)) #IMPORTANT: This is where we are building the full backup list
        jds_to_backup.append(jdtobackup)
        continue #back to the top and see if we can add another day to this backup

    logger.info('Moving on from data selection to tape writing!')
    #example entry in "joined_results" which is the main data object that will be used to create db records
    # and do the actual TAR
    '''
    ({'store_name': 'herastore01-8',
      'store_ssh_host': 'heralib.aoc.nrao.edu',
      'parent_dirs': '2459977',
      'name': 'zen.2459977.21630.sum.autos.uvh5',
      'deletion_policy': 'disallowed',
      'full_path_on_store': '/export/hera/herastore01-8/2459977/zen.2459977.21630.sum.autos.uvh5'},
     {'name': 'zen.2459977.21630.sum.autos.uvh5',
      'type': 'uvh5',
      'create_time': 1675303951,
      'obsid': 1359306701,
      'size': 19859900,
      'md5': 'e910dffca62c8c859aacd873d2c14c6b'},
     {'obsid': 1359306701,
      'start_time_jd': 2459977.21624788,
      'stop_time_jd': 2459977.21678797,
      'start_lst_hr': 3.39516861968927,
      'session_id': 1359303644})
    '''
    #turn our list of joined results into filenames, obsids, jds, and sizes
    files = []
    sizes = []
    obsids = []
    start_jds = []
    for F in joined_results:
        files.append(F[0]['full_path_on_store'])
        sizes.append(F[1]['size'])
        obsids.append(F[2]['obsid'])
        start_jds.append(F[2]['start_time_jd'])
    logger.info(f'Backing up JDS {"".join([str(jd) for jd in jds_to_backup])}. Nfiles = {len(files)}, Total Size = {np.round(np.sum(sizes)/1e12,1)}TB')
    def update_tape_usage(tapes,testing=False):
        #input: dict  from query_tape_usage and a session for heratape db
        #output: input dict with added key: usage (sum in bytes known to heratape for each tape)
        for i in range(len(tapes)):
            tapeusage = query_tape_usage(tapes[i]['tape_id'],testing=testing)
            tapes[i]['usage'] =  tapeusage
        return tapes
    def select_empty_tape(drive_id,testing=False):
        # Impliments tape selection logic
        # drive 1 uses tape slots 1-12, drive 2 uses 13-24
        # there needs to be one empty slot for shuffling and another is taken up by a cleaner tape
        # ideally these are split between the two halves of the jukebox
        # input: drive number 0 or 1
        # get the next tape in the relevant half which is not full
        # full being defined as > 50%
        # return: slot, tape_id, and tape_usage (in bytes) to be used
        TAPECAPACITY = 18e12 #TODO: replace this with the number in heratape.Tapes
        assert(drive_id  in [0,1])
        tape_archive = query_tape_jukebox() #get the list of tapes currently in the jukebox
        tape_archive = update_tape_usage(tape_archive,testing=testing) #find out how full they are.
        #throw out any that are too full
        empty_tapes = []
        for tape in tape_archive:
            if tape['tape_id'].startswith('CLN'):continue#ignore the cleaner!
            logger.info(tape['usage'])
            if tape['usage'] < TAPECAPACITY *0.5:
                logger.info(tape['tape_id'])
                if (int(tape['slot']) <= 12) == (drive_id==0): # slot<=12 and drive 0, or both opposite.
                    empty_tapes.append(tape)
        # a nice flourish, lets use the lowest serial number first
        usetape_id = sorted([t['tape_id'] for t in empty_tapes])[0]
        for tape in empty_tapes:
            if tape['tape_id'] == usetape_id:
                return tape['slot'], tape['tape_id'], tape['usage'] 
        return None, None, None # if no tapes found, its probably time for a fresh batch of tapes!
        
    #Armed with all our metadata from the Librarian, we can move to the Tape side of things
    
    # prepare tape for backup
    drivetapeid = tape_in_drive(mydrive)
    logger.info(f'Drive {mydrive}, Tape {drivetapeid}')
    #check the total against the remaining space
    tape_usage = float(query_tape_usage(drivetapeid,testing=TESTING))
    if tape_usage >0:
        logger.info(f'Size of this backup = {np.sum(sizes)/1e12} TB is < the {(TAPESIZE - tape_usage)/1e12}TB theoretically remaining on the tape')
        logger.info('Load new tape')
   
        #load a fresh tape
        #1 find an empty slot.  If there isn't one, we're done. Be ok if theres more than one
        empty_slots = find_empty_slots()
        if len(empty_slots)==0: 
            logger.info("ERROR: No empty slot into which I can unload a tape. The jukebox is too full! Remove a tape and try again.")
            sys.exit()
        #if theres more than one, choose the topmost empty slot
        if len(empty_slots)==1:
            emptyslot = empty_slots
        else:
            emptyslot = empty_slots[0]
        #unload working drive to this empty slot
        logger.info(f'unloading drive {mydrive} back to slot {empyslot}') 
        unload_tape(mydrive,emptyslot)
        logger.info('tape unloaded')
        #select a fresh tape
        #  this might require human intervention, which we pessimistically assume might be the case until 
        #  proven otherwise
        waitingforhuman = True
        while(waitingforhuman):
            update_tapes(testing=TESTING) #check for any new tapes in the library
            newslot,newtape_id,newtape_usage = select_empty_tape(mydrive,testing=TESTING)
            if newslot is None:
                if mydrive==0:
                    logger.warning("ALERT: New Tapes needed in slots 13-24")
                    #sys.exit()#TODO: pause and wait like a good daemon
                    time.sleep(30)
                    continue
                else:
                    logger.warning("ALERT: New Tapes needed in slots 1-12")
                    #sys.exit()  #TODO: pause ad wait like a good daemon
                    time.sleep(30)
                    continue
            else:
                #we have an empty tape lets proceed
                logger.info(f'loading {newtape_id} from {newslot} to drive {mydrive}')
                load_tape(mydrive,newslot)
                logger.info('tape load complete')
                drivetapeid = newtape_id
                break
    logger.info(f'adding {len(files)} files to heratape db')
    add_files_to_tape(
        tape_id=drivetapeid,
        write_date=None,
        #write_date=Time('2025-01-01'), #missing date implies writing in progress
        filepath_list=files,
        obsid_list=obsids,
        jd_start_list=start_jds,
        size_list=sizes,
        testing=TESTING
    )
    logger.info('files added, moving on to tape writing')

    #write the file list to be read by tar
    filelistfile = f'ht_d{mydrive}_{drivetapeid}_{Time.now().isot}.txt'
    logger.info(f'writing file list to {filelistfile}')
    F = open(filelistfile,'w')
    for i in np.arange(len(files)):
        F.write(f'{files[i]}\n')
        #F.write(f'{drivetapeid},{files[i]}, {obsids[i]}, {start_jds[i]}, {sizes[i]}\n')
    F.close()
    if  DOTAPE:
        logger.info(f'running tar: tar -cjf /dev/st{mydrive} -T {filelistfile}')
        tstart = time.time()
        with subprocess.Popen(f'time tar -cf /dev/nst{mydrive} -T {filelistfile} ', shell=True, stdout=subprocess.PIPE) as proc:                       
                    lines = proc.stdout.readlines()
        logger.info(f'finished in {(time.time() - tstart)/60} minutes')
    else:
        logger.info(" TESTING MODE: skipping real tar to tape")
        logger.debug(f' the tar command: time tar -cf /dev/nst{mydrive} -T {filelistfile}')
    if False:
        logger.error(f' A SIMULATED ERROR HAS OCCURRED. Like for example someone restarted the daemon during a tape write.')
        sys.exit() 
    
    logger.info("files written OK")
    
    logger.info("updating write date in db")
    file_bases = [os.path.basename(f) for f in files]
    set_write_date(file_bases,Time.now(),testing=TESTING)
    logger.info("DONE")
    # BACK to top and JD selection.
    backupsize=0

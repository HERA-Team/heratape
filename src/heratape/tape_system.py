import subprocess
from .tapes import add_new_tapes_to_db,list_tapes_in_db
import logging
logging.basicConfig(
    level=logging.DEBUG if os.environ.get('LOG_LEVEL') == 'DEBUG' else logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('heratape-system')

def run(cmd):
    "Safely run cmd. report errors to logger then EXIT"
    with subprocess.Popen(cmd,
        shell=True,
        stdout=subprocess.PIPE, stderr = subprocess.PIPE) as proc:
                stdout,stderr = proc.communicate()
                lines = stdout.decode().split()
        if proc.returncode >0:
            logger.error(f'{cmd} exited with code {proc.returncode},
            error follow')
            logger.error(stderr)
            sys.exit(proc.returncode)
        return lines

def tape_in_drive(drivenum):
    #input drivenum = 0 or 1
    #returns: VolumeTag of tape in drive
    assert(drivenum in [0,1])
    lines = run('sudo mtx -f /dev/sg25 status')
    for line in lines:
        l = line
        if l.startswith(f'Data Transfer Element {drivenum}'):
            return l.split('=')[1].strip()
def query_tape_jukebox():
    #return a list of tapes in the storage rac
    lines = run('sudo mtx -f /dev/sg25 status')
    tapes = []
    for line in lines:
        l = line.strip()
        if l.startswith('Storage Element'):
            if l.split(':')[1].strip().startswith('Empty'): continue
            slot = l.split(':')[0].split()[2]
            tape_id = l.split('=')[1].strip()
            tapes.append({'slot':slot,'tape_id':tape_id})
    return tapes
def find_empty_slots():
    #return a list of empty slot numbers
    #return a list of tapes in the storage rac
    lines = run('sudo mtx -f /dev/sg25 status')
    empty_slots = []
    for line in lines:
        l = line.strip()
        if l.endswith('Empty') and l.startswith('Storage'):
            empty_slots.append(l.split(':')[0].split()[2])
    return empty_slots
def unload_tape(drive, toslot):
    #input: drive number 0 or 1, and toslot destination to unload to
    lines = run(f'sudo mtx -f /dev/sg25 unload {toslot} {drive}')
    #TODO: trap for error where the slot isn't empty
    # there are probably other error conditions I don't know about yet
def load_tape(drive,fromslot):
    #input: drive number 0 or 1, and a fromslot to grab from
    lines = run(f'sudo mtx -f /dev/sg25 load {fromslot} {drive}')
    # TODO: don't know what error conditions to trap for here. but they'll come
def list_tapes_in_system():
    # on herastore01 on NRAO
    # return list of all tapes in system
    #get the list of tapes currently in the jukebox
    lines = run('sudo mtx -f /dev/sg25 status')

    tapes = []
    for line in lines:
        s = line.split('=')
        if len(s)<2:continue
        tapes.append(s[1].strip())
    return tapes
def tape_in_drive(drivenum):
    #input drivenum = 0 or 1
    #returns: VolumeTag of tape in drive
    assert(drivenum in [0,1])
    lines - run('sudo mtx -f /dev/sg25 status')
    for line in lines:
        s = line
        if s.startswith(f'Data Transfer Element {drivenum}'):
            return s.split('=')[1].strip()
    return None

def update_tapes(testing=False):
    # add anything thats new
    # skip any cleaner tapes
    tapes = list_tapes_in_system()

    # get list of tapes already known to us
    dbtapes = list_tapes_in_db(testing=testing)
    print(f'Tape table has {len(dbtapes)} entries')
    newtapes = list(set(tapes) - set(dbtapes))
    #don't add the CLN tapes
    newtapes = [t for t in newtapes if not t.startswith('CLN')]
    print(f'adding {len(newtapes)} new tapes')
    add_new_tapes_to_db(newtapes)
def check_drive_online(drivenum,testing=False):
    lines = run(f'sudo mt -f /dev/nst{drivenum} status')
    for line in lines:
        if 'ONLINE' in  line: return True
    return False

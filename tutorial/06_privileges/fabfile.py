from fabric.api import run, env, cd, sudo, parallel
from fabric.context_managers import shell_env

env.hosts = ['hop2', 'hop3', 'hop0', 'hop1']
env.user = 'hcooney'

repo = 'https://github.com/lionkov/legion/'
code_dir = '/home/' + env.user + '/code'
legion_root = code_dir + '/legion'
work_dir = legion_root + '/tutorial/06_privileges'
branch = 'fabric-iso'
send_port = 8080
recv_port = 8081
exchange_host = 'hop2'
nnodes = 4
loglevel = 3
ncpus = 4
handlers = 1


def remove_repos():
    run('rm -rf code/legion')

def clone():
    with cd(code_dir):
        run('git clone ' + repo)
    with cd(legion_root):
        run('git checkout ' + branch)
        
def pull():
    with cd(legion_root):
        run('git checkout ' + branch)
        run('git pull')

@parallel
def build():
    with cd(work_dir), shell_env(LG_RT_DIR=str(legion_root + '/runtime')):
        run('make')

@parallel
def clean():
    with cd(work_dir), shell_env(LG_RT_DIR=str(legion_root + '/runtime')):
        run('make clean')

@parallel
def clean_exchange():
    with cd (legion_root + '/fabric_startup_server'):
        run('make clean')

@parallel
def build_exchange():
    with cd (legion_root + '/fabric_startup_server'):
        run('make')

@parallel
def build_all():
    pull()
    build()
    build_exchange()

@parallel
def rebuild():
    clean()
    clean_exchange()
    build_all()

@parallel
def start():
    with cd(work_dir): #, shell_env(REALM_FREEZE_ON_ERROR='1'):
        run('./privileges'
            + ' -ll:num_nodes ' + str(nnodes)
            + ' -ll:exchange_server_host hop2'
            + ' -ll:cpu ' + str(ncpus)
            + ' -ll:handlers ' + str(handlers)
            + ' -level '   + str(loglevel))
        

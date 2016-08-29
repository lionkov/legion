from fabric.api import run, env, cd, sudo, parallel
from fabric.context_managers import shell_env

env.hosts = ['hop2', 'hop3']
env.user = 'hcooney'

repo = 'https://github.com/lionkov/legion/'
code_dir = '/home/' + env.user + '/code'
legion_root = code_dir + '/legion'
branch = 'fabric-iso'
send_port = 8080
recv_port = 8081
exchange_host = 'hop2'
nnodes = 2

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
def build_fabric():
    with cd(legion_root + '/test/fabric'), shell_env(LG_RT_DIR=str(legion_root + '/runtime')):
        run('make build')
@parallel
def build_realm():
    with cd(legion_root + '/test/realm'), shell_env(LG_RT_DIR=str(legion_root + '/runtime')):
        run('make build')
@parallel
def clean_realm():
    with cd(legion_root + '/test/realm'), shell_env(LG_RT_DIR=str(legion_root + '/runtime')):
        run('make clean')
@parallel
def build(path):
    with cd(legion_root + path), shell_env(LG_RT_DIR=str(legion_root + '/runtime')):
        run('make build')
@parallel
def clean(path):
    with cd(legion_roon + path), shell_env(LG_RT_DIR=str(legion_root + '/runtime')):
        run('make clean')
       
@parallel
def start():
    with cd(legion_root + '/test/fabric'), shell_env(LG_RT_DIR=str(legion_root + '/runtime')):
        run('./runtests -ll:num_nodes '
            + str(nnodes) +
            ' -ll:exchange_server_host '
            + exchange_host)

@parallel
def ctxswitch():
    with cd(legion_root + '/test/realm'), shell_env(LG_RT_DIR=str(legion_root + '/runtime')):
        run('raa./test_profiling -ll:num_nodes '
            + str(nnodes) +
            ' -ll:exchange_server_host '
            + exchange_host)


def clean_test():
    with cd(legion_root + '/test/fabric'), shell_env(LG_RT_DIR=str(legion_root + '/runtime')):
        run('make clean')

def remake_test():
    clean_test()
    build_test()

def build_exchange():
    with cd(legion_root + '/fabric_startup_server'):
        run('make')
        
def clean_exchange():
    with cd(legion_root + '/fabric_startup_server'):
        run('make clean')
    
def start_exchange():
    with cd (legion_root + '/fabric_startup_server'):
        run('./exchange ' + str(nnodes))

def build_all():
    pull()
    build_exchange()
    build_fabric()

def rebuild_all():
    pull()
    clean_test()
    clean_exchange()
    build_fabric()
    build_exchange()

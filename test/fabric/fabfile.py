from fabric.api import run, env, cd, sudo
from fabric.context_managers import shell_env

env.hosts = ['hop2', 'hop3']
env.user = 'hcooney'

repo = 'https://github.com/lionkov/legion/'
branch = 'fabric-iso'
send_port = 8080
recv_port = 8081
exchange_host = 'hop2'
nclients = 2

def remove_repos():
    run('rm -rf code/legion')

def clone():
    with cd('code'):
        run('git clone ' + repo)
    with cd('code/legion'):
        run('git checkout ' + branch)
        
def pull():
    with cd('code/legion/'):
        run('git pull')

def build_test():
    with cd('code/legion/test/fabric'), shell_env(LG_RT_DIR='../../runtime'):
        run('make build')

def start():
    with cd('code/legion/test/fabric'):
        run('./runtests -ll:num_nodes '
            + str(nclients) +
            ' -ll:exchange_server_host '
            + exchange_host)

def clean_test():
    with cd('code/legion/test/fabric'), shell_env(LG_RT_DIR='../../runtime'):        
        run('make clean')

def remake_test():
    clean_test()
    build_test()

def build_exchange():
    with cd('code/legion/fabric_startup_server'):
        run('make')
def clean_exchange():
    with cd('code/legion/fabric_startup_server'):
        run('make clean')
    
def start_exchange():
    with cd ('code/legion/fabric_startup_server'):
        run('./exchange ' + str(nclients))

def build_all():
    pull()
    build_exchange()
    build_test()

def rebuild_all():
    pull()
    clean_test()
    clean_exchange()
    build_test()
    build_exchange

import socket
import sys
from pathlib import Path

from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

from tools.json_rw import json_read, json_write
from tools.logs import init_logger
from tools.names import get_hostname
from tools.net_use import net_use

disable_warnings(InsecureRequestWarning)

# ? ROOT
root_path = Path(sys.argv[0]).parent

# ? LOCAL
local_path = Path.home().joinpath(f'AppData\\Local\\.rpa')
local_path.mkdir(exist_ok=True, parents=True)
local_env_path = local_path.joinpath('env.json')
if not local_env_path.is_file():
    json_write(local_env_path, {
        "global_path": "\\\\172.16.8.87\\d\\.rpa",
        "global_username": "rpa.robot",
        "global_password": "Aa1234567"
    })
local_env_data = json_read(local_env_path)
process_list_path = local_path.joinpath('process_list.json')
if not process_list_path.is_file():
    process_list_path.write_text('[]', encoding='utf-8')

# ? GLOBAL
global_path = Path(local_env_data['global_path'])
global_username = local_env_data['global_username']
global_password = local_env_data['global_password']
net_use(global_path, global_username, global_password)
global_env_path = global_path.joinpath('env.json')
global_env_data = json_read(global_env_path)

orc_host = global_env_data['orc_host']
orc_port = global_env_data['new_orc_port']
tg_token = global_env_data['tg_token']
smtp_host = global_env_data['smtp_host']
smtp_author = global_env_data['smtp_author']
sprut_username = global_env_data['sprut_username']
sprut_password = global_env_data['sprut_password']
odines_username = global_env_data['odines_username']
odines_password = global_env_data['odines_password']
odines_username_rpa = global_env_data['odines_username_rpa']
odines_password_rpa = global_env_data['odines_password_rpa']
owa_username = global_env_data['owa_username']
owa_password = global_env_data['owa_password']
owa_username_compl = global_env_data['owa_username_compl']
owa_password_compl = global_env_data['owa_password_compl']
sed_username = global_env_data['sed_username']
sed_password = global_env_data['sed_password']
cups_host = global_env_data['cups_host']
cups_username = global_env_data['cups_username']
cups_password = global_env_data['cups_password']
cas_username = global_env_data['cas_username']
cas_password = global_env_data['cas_password']
postgre_ip = global_env_data['postgre_ip']
postgre_port = global_env_data['postgre_port']

# ? PROJECT
robot_name = 'robot-stat-1p'
config_path = global_path.joinpath(f'.agent\\{robot_name}\\config.json')
config_data = json_read(config_path)
chat_id = config_data['chat_id']

ecp_paths = r'\\vault.magnum.local\common\Stuff\_06_Бухгалтерия\! Актуальные ЭЦП'

ip_address = socket.gethostbyname(socket.gethostname())

saving_path = global_path.joinpath(f".agent\\{robot_name}\\Output")
saving_path.mkdir(exist_ok=True, parents=True)
reports_saving_path = global_path.joinpath(f".agent\\{robot_name}\\Отчёты 1П")
reports_saving_path.mkdir(exist_ok=True, parents=True)

download_path = Path.home().joinpath('downloads')
db_host = global_env_data['postgre_ip']

db_port = global_env_data['postgre_port']
db_name = global_env_data['postgre_db_name']
db_schema = 'robot'
db_user = global_env_data['postgre_db_username']
db_pass = global_env_data['postgre_db_password']
project_path = global_path.joinpath(f'.agent').joinpath(robot_name).joinpath(get_hostname())
project_path.mkdir(exist_ok=True, parents=True)
config_path = project_path.joinpath('config.json')

log_path = project_path.joinpath(f'{sys.argv[1]}.log' if len(sys.argv) > 1 else 'dev.log')
logger = init_logger(tg_token=tg_token, chat_id=chat_id, log_path=log_path)

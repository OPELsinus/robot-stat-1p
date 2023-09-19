from pathlib import Path
from typing import Union, List


def kill_exe(pid: int):
    import os
    import traceback
    from contextlib import suppress
    import psutil

    try:
        process = psutil.Process(int(pid))
        root = psutil.Process(int(os.getppid()))
        if process.name() == root.name():
            return
        with suppress(Exception):
            if process.is_running():
                with suppress(Exception):
                    children_ = process.children(recursive=True)
                    for child_ in children_:
                        with suppress(Exception):
                            if child_.is_running():
                                with suppress(Exception):
                                    child_.kill()
        if process.is_running():
            process.kill()
    except (Exception,):
        traceback.print_exc()


def kill_process_list(process_list: Union[Path, List] = None):
    import json
    import traceback
    import psutil
    import win32api

    if isinstance(process_list, Path) and process_list.is_file():
        with open(process_list.__str__(), 'r', encoding='utf-8') as pl_fp:
            process_list = json.load(pl_fp)
    elif isinstance(process_list, Path) and not process_list.is_file():
        process_list = list()
        with open(process_list.__str__(), 'w+', encoding='utf-8') as pl_fp:
            json.dump(process_list, pl_fp, ensure_ascii=False)
    elif isinstance(process_list, List):
        pass
    else:
        process_list = list()

    username = win32api.GetUserNameEx(win32api.NameSamCompatible)
    processes = list(p for p in psutil.process_iter() if p.name() in process_list and p.username() == username)
    for proc in processes:
        try:
            kill_exe(proc.pid)
        except (Exception,):
            traceback.print_exc()

def get_hostname() -> str:
    import socket

    return socket.gethostbyname(socket.gethostname())


# ? tested
def get_username() -> str:
    from win32api import GetUserNameEx, NameSamCompatible

    return GetUserNameEx(NameSamCompatible)

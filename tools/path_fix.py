def protect_path(value: str) -> str:
    import re

    return re.sub(r'[<>:"/\\|?*]', '_', value)

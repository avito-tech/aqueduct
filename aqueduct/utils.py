def get_env_with_prefix(prefix: str, env_name: str) -> str:
    return '_'.join((prefix, env_name))
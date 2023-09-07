import logging
import time
import os
import sys


def get_service_account_info(config_center_key: str):
    """ 获取 service account json 信息，目前从 config center 获取"""
    from config_center import ConfigLoader

    max_retries = 10
    count = 0
    sleep = 10
    while count < max_retries:
        try:
            loader = ConfigLoader.default()
            app_config = loader.load()  # type: dict

            # access config entries by key
            if app_config.__contains__(config_center_key):
                return app_config[config_center_key]
            else:
                raise KeyError(f"Unknown key: {config_center_key}.")
        except Exception as e:
            logging.error(f"Get service account failed! exception:{e}")

        # sleep 10s 后重试
        time.sleep(sleep)
        count = count + 1
        logging.info(f"Retry: {count}")


def save_service_account(service_account_info: str, dest_path: str):
    """ 获取 token 信息，并保存到本地 """
    if service_account_info:
        f = open(dest_path, 'w')
        f.write(service_account_info)
        f.close()
        logging.info(f"service account path: {dest_path}.")
    else:
        raise Exception("Save service account info to local file failed!")


if __name__ == '__main__':
    args = sys.argv
    if len(args) < 6:
        logging.error(
            f"Usage: service_account_init.py 'testing' 'dataworks.automizelyapi.org_integration' 'config-center-url' "
            f"'token_key' 'service_account_key' '/tmp/service_account_info.json'")
        exit(1)

    env = args[1]
    project_name = args[2]
    endpoint = args[3]
    token_key = args[4]
    service_account_key = args[5]
    dest_path = args[6]

    os.environ['CONFIG_CENTER_DEBUG'] = 'false'
    os.environ['CONFIG_CENTER_ENV'] = env
    os.environ['CONFIG_CENTER_PROJECT_NAME'] = project_name
    os.environ['CONFIG_CENTER_ENDPOINT'] = endpoint
    os.environ['CONFIG_CENTER_API_KEY'] = os.getenv(token_key)

    service_account_info = get_service_account_info(service_account_key)
    save_service_account(service_account_info, dest_path)

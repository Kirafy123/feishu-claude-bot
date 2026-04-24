import datetime
import re
import requests
import json
import os
import logging
import time as _time

logger = logging.getLogger(__name__)

app_id = os.getenv('APP_ID')
app_secret = os.getenv('APP_SECRET')

assert app_id and app_secret, 'app_id and app_secret is required'

_DEFAULT_TIMEOUT = 30


class FeishuAPIError(Exception):
    """飞书 API 返回错误"""
    pass


def get_tenant_access_token():
    """
    获取飞书的tenant_access_token
    :return:
    """
    try:
        res = requests.post(
            url='https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal',
            json={"app_id": app_id, "app_secret": app_secret},
            timeout=_DEFAULT_TIMEOUT,
        ).json()
    except requests.exceptions.RequestException as e:
        raise FeishuAPIError(f"获取 token 网络错误: {e}")
    if res.get('code') != 0:
        raise FeishuAPIError(f"获取 token 失败: {res.get('msg', '未知错误')} (code={res.get('code')})")
    return res['app_access_token']

def get_headers(access_token):
    return {'Authorization': 'Bearer ' + access_token}

def reply_message(message_id, text, access_token=None):
    if access_token is None:
        access_token = get_tenant_access_token()

    url = 'https://open.feishu.cn/open-apis/im/v1/messages/{}/reply'.format(message_id)

    ret_data = {'text': text}

    body = {
        "msg_type": "text",
        "content": json.dumps(ret_data, ensure_ascii=False, indent=4),
        'uuid': str(datetime.datetime.now().timestamp())
    }
    res = requests.post(url, headers=get_headers(access_token), json=body, timeout=_DEFAULT_TIMEOUT).json()
    return res

def send_message(receive_id, text, access_token=None):
    if access_token is None:
        access_token = get_tenant_access_token()

    url = 'https://open.feishu.cn/open-apis/im/v1/messages'
    param = {'receive_id_type': 'chat_id'}

    ret_data = {'text': text}

    body = {
        'receive_id': receive_id,
        "msg_type": "text",
        "content": json.dumps(ret_data, ensure_ascii=False, indent=4),
        'uuid': str(datetime.datetime.now().timestamp())
    }
    res = requests.post(url, headers=get_headers(access_token), json=body, params=param, timeout=_DEFAULT_TIMEOUT).json()
    return res


def send_card_message(receive_id, text, access_token=None, workspace=None):
    """发送可编辑的卡片消息"""
    if access_token is None:
        access_token = get_tenant_access_token()

    url = 'https://open.feishu.cn/open-apis/im/v1/messages'
    param = {'receive_id_type': 'chat_id'}

    title = "CCwin"
    if workspace:
        import os
        title += f" | {os.path.basename(workspace)}"

    card = {
        "header": {
            "title": {"tag": "plain_text", "content": title},
            "template": "grey"
        },
        "elements": [
            {"tag": "markdown", "content": text}
        ]
    }

    body = {
        'receive_id': receive_id,
        'msg_type': 'interactive',
        'uuid': str(datetime.datetime.now().timestamp()),
        'content': json.dumps(card, ensure_ascii=False)
    }
    res = requests.post(url, headers=get_headers(access_token), json=body, params=param, timeout=_DEFAULT_TIMEOUT).json()
    return res


def update_card_message(message_id, text, access_token=None, workspace=None):
    """更新卡片消息内容"""
    if access_token is None:
        access_token = get_tenant_access_token()

    url = f'https://open.feishu.cn/open-apis/im/v1/messages/{message_id}'

    title = "CCwin"
    if workspace:
        import os
        title += f" | {os.path.basename(workspace)}"

    card = {
        "header": {
            "title": {"tag": "plain_text", "content": title},
            "template": "grey"
        },
        "elements": [
            {"tag": "markdown", "content": text}
        ]
    }

    body = {
        "content": json.dumps(card, ensure_ascii=False)
    }
    res = requests.patch(url, headers=get_headers(access_token), json=body, timeout=_DEFAULT_TIMEOUT).json()
    return res


def download_file_message(message_id: str, file_key: str, save_path: str, access_token=None, timeout: int = 300, res_type: str = "file") -> bool:
    """
    下载消息中的资源文件（文件、图片等）
    GET /open-apis/im/v1/messages/{message_id}/resources/{file_key}?type=file|image
    res_type: "file" 或 "image"
    返回 True 表示下载成功
    """
    if access_token is None:
        access_token = get_tenant_access_token()

    if res_type not in ("file", "image"):
        res_type = "file"
    url = f'https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/resources/{file_key}?type={res_type}'
    headers = get_headers(access_token)

    res = requests.get(url, headers=headers, timeout=timeout)
    if res.status_code == 200:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, 'wb') as f:
            f.write(res.content)
        return True
    else:
        raise Exception(f'下载文件失败: {res.status_code}, {res.text}')


def upload_file_to_feishu(file_path: str, access_token=None, timeout: int = 120, file_name: str = None) -> dict:
    """
    上传本地文件到飞书 IM，返回 file_key。
    限制：≤30MB，超时 120 秒。失败自动重试 2 次，间隔 3 秒。
    file_name: 可选，指定飞书侧显示的文件名。
    返回格式: {"success": True, "file_key": "xxx"} 或 {"success": False, "error": "xxx"}
    """
    if access_token is None:
        access_token = get_tenant_access_token()

    # 检查文件存在性和大小
    if not os.path.isfile(file_path):
        return {"success": False, "error": f"文件不存在: {file_path}"}
    file_size = os.path.getsize(file_path)
    max_size = 30 * 1024 * 1024  # 30MB
    if file_size > max_size:
        return {"success": False, "error": f"文件过大（{file_size / 1024 / 1024:.1f}MB > 30MB）"}

    url = 'https://open.feishu.cn/open-apis/im/v1/files'
    display_name = file_name if file_name else os.path.basename(file_path)

    max_retries = 2
    for attempt in range(max_retries + 1):
        if attempt > 0:
            logger.info(f"[上传] 重试 {attempt}/{max_retries}: {display_name} (等待 3 秒)")
            _time.sleep(3)

        headers = get_headers(access_token)
        start_t = _time.time()

        try:
            with open(file_path, 'rb') as f:
                files = {'file': (display_name, f)}
                data = {
                    'file_type': 'stream',
                    'file_name': display_name,
                }
                res = requests.post(
                    url,
                    headers={'Authorization': headers['Authorization']},
                    data=data,
                    files=files,
                    timeout=timeout,
                )
                res_json = res.json()
                elapsed = _time.time() - start_t

                if res_json.get('code') == 0:
                    file_key = res_json['data']['file_key']
                    logger.info(f"[上传] 成功: {display_name} ({file_size} bytes, {elapsed:.1f}s)")
                    return {"success": True, "file_key": file_key}
                else:
                    error_msg = res_json.get('msg', '上传失败')
                    logger.warning(f"[上传] 失败 (attempt {attempt+1}/{max_retries+1}): {display_name} - {error_msg} ({elapsed:.1f}s)")
                    # API 逻辑错误不重试
                    return {"success": False, "error": error_msg}
        except requests.exceptions.Timeout:
            elapsed = _time.time() - start_t
            logger.warning(f"[上传] 超时 (attempt {attempt+1}/{max_retries+1}): {display_name} ({elapsed:.1f}s)")
            if attempt == max_retries:
                return {"success": False, "error": "上传超时（120 秒）"}
            # 继续重试
        except Exception as e:
            elapsed = _time.time() - start_t
            logger.warning(f"[上传] 异常 (attempt {attempt+1}/{max_retries+1}): {display_name} - {e} ({elapsed:.1f}s)")
            if attempt == max_retries:
                return {"success": False, "error": str(e)}
            # 继续重试

    return {"success": False, "error": "上传失败（重试耗尽）"}


def send_file_message(receive_id: str, file_key: str, file_name: str, access_token=None, chat_type: str = "chat") -> dict:
    """发送文件消息到飞书"""
    if access_token is None:
        access_token = get_tenant_access_token()

    url = 'https://open.feishu.cn/open-apis/im/v1/messages'
    param = {'receive_id_type': 'chat_id'}

    content = json.dumps({"file_key": file_key, "file_name": file_name}, ensure_ascii=False)
    body = {
        'receive_id': receive_id,
        'msg_type': 'file',
        'content': content,
    }
    try:
        res = requests.post(url, headers=get_headers(access_token), json=body, params=param, timeout=30)
        res_json = res.json()
        if res_json.get('code') == 0:
            logger.info(f"[发送文件消息] 成功: {file_name}")
        else:
            logger.warning(f"[发送文件消息] 失败: {file_name} - {res_json.get('msg', '未知错误')}")
        return res_json
    except Exception as e:
        logger.warning(f"[发送文件消息] 异常: {file_name} - {e}")
        return {"code": -1, "msg": str(e)}


def zip_folder(folder_path: str, output_path: str = None) -> str:
    """将文件夹压缩为 zip，返回 zip 文件路径"""
    import shutil
    if output_path is None:
        output_path = folder_path + '.zip'
    archive_path = shutil.make_archive(folder_path, 'zip', folder_path)
    return archive_path


def update_message(message_id, text, access_token=None):
    """编辑已发送的消息内容（仅限卡片消息）"""
    if access_token is None:
        access_token = get_tenant_access_token()

    url = f'https://open.feishu.cn/open-apis/im/v1/messages/{message_id}'
    ret_data = {'text': text}
    body = {
        "msg_type": "text",
        "content": json.dumps(ret_data, ensure_ascii=False, indent=4),
    }
    res = requests.patch(url, headers=get_headers(access_token), json=body, timeout=_DEFAULT_TIMEOUT).json()
    return res

def get_department_member_list(department_id, access_token=None):
    if access_token is None:
        access_token = get_tenant_access_token()

    # 获取部门直属用户列表
    url = 'https://open.feishu.cn/open-apis/contact/v3/users/find_by_department'
    params = {'department_id': department_id}
    res = requests.get(url, headers=get_headers(access_token), params=params, timeout=_DEFAULT_TIMEOUT).json()
    if res['code'] != 0:
        raise FeishuAPIError(f'get_department_member_list() get err res:{json.dumps(res)}')
    return res

def get_chats_member_list(chat_id, access_token=None):
    if access_token is None:
        access_token = get_tenant_access_token()

    # 先查看机器人是否在群里
    url = f'https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}/members/is_in_chat'
    res = requests.get(url, headers=get_headers(access_token), timeout=_DEFAULT_TIMEOUT).json()
    if res['code'] != 0 or not res['data']['is_in_chat']:
        return {"data": {"items": []}}

    # 获取群成员列表
    url = f'https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}/members'
    res = requests.get(url, headers=get_headers(access_token), timeout=_DEFAULT_TIMEOUT).json()
    
    if res['code'] != 0:
        raise FeishuAPIError(f'get_chats_member_list() get err res:{json.dumps(res)}')
    return res
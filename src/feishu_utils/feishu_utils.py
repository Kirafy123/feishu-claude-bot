import datetime
import re
import requests
import json
import os

app_id = os.getenv('APP_ID')
app_secret = os.getenv('APP_SECRET')

assert app_id and app_secret, 'app_id and app_secret is required'

def get_tenant_access_token():
    """
    获取飞书的tenant_access_token
    :return:
    """
    res = requests.post(url='https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal', json={"app_id": app_id, "app_secret": app_secret}).json()
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
    res = requests.post(url, headers=get_headers(access_token), json=body).json()
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
    res = requests.post(url, headers=get_headers(access_token), json=body, params=param).json()
    return res


def send_card_message(receive_id, text, access_token=None, workspace=None):
    """发送可编辑的卡片消息"""
    if access_token is None:
        access_token = get_tenant_access_token()

    url = 'https://open.feishu.cn/open-apis/im/v1/messages'
    param = {'receive_id_type': 'chat_id'}

    title = "🤖 CCwin"
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
    res = requests.post(url, headers=get_headers(access_token), json=body, params=param).json()
    return res


def update_card_message(message_id, text, access_token=None, workspace=None):
    """更新卡片消息内容"""
    if access_token is None:
        access_token = get_tenant_access_token()

    url = f'https://open.feishu.cn/open-apis/im/v1/messages/{message_id}'

    title = "🤖 CCwin"
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
    res = requests.patch(url, headers=get_headers(access_token), json=body).json()
    return res


def download_file_message(message_id: str, file_key: str, save_path: str, access_token=None, timeout: int = 300) -> bool:
    """
    下载消息中的资源文件（文件、图片等）
    GET /open-apis/im/v1/messages/{message_id}/resources/{file_key}
    返回 True 表示下载成功
    """
    if access_token is None:
        access_token = get_tenant_access_token()

    url = f'https://open.feishu.cn/open-apis/im/v1/messages/{message_id}/resources/{file_key}'
    headers = get_headers(access_token)

    res = requests.get(url, headers=headers, timeout=timeout)
    if res.status_code == 200:
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, 'wb') as f:
            f.write(res.content)
        return True
    else:
        raise Exception(f'下载文件失败: {res.status_code}, {res.text}')


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
    res = requests.patch(url, headers=get_headers(access_token), json=body).json()
    return res

def get_department_member_list(department_id, access_token=None):
    if access_token is None:
        access_token = get_tenant_access_token()
        
    # 获取部门直属用户列表
    url = 'https://open.feishu.cn/open-apis/contact/v3/users/find_by_department'
    params = {'department_id': department_id}
    res = requests.get(url, headers=get_headers(access_token), params=params).json()
    if res['code'] !=0:
        raise Exception(f'get_department_member_list() get err res:{json.dumps(res)}')
    return res

def get_chats_member_list(chat_id, access_token=None):
    if access_token is None:
        access_token = get_tenant_access_token()
        
    # 先查看机器人是否在群里
    url = f'https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}/members/is_in_chat'
    res = requests.get(url, headers=get_headers(access_token)).json()
    if res['code'] !=0 or not res['data']['is_in_chat']:
        return {"data" : {"items": []}}
        # raise Exception(f'get_chats_member_list() get err res:{json.dumps(res)}')
    
    # 获取群成员列表
    url = f'https://open.feishu.cn/open-apis/im/v1/chats/{chat_id}/members'
    res = requests.get(url, headers=get_headers(access_token)).json()
    
    if res['code'] !=0:
        raise Exception(f'get_chats_member_list() get err res:{json.dumps(res)}')
    return res
# 飞书应用配置指南

## 1. 创建应用

1. 进入 [飞书开放平台](https://open.feishu.cn/)
2. 点击「创建自建应用」
3. 填写应用名称（如 `Claude Code Bot`）和图标

## 2. 添加机器人能力

进入 **应用能力 → 添加应用能力**：

- 勾选 **「机器人」**（必须开启，否则无法接收消息）

## 3. 权限配置

进入 **权限管理 → 批量导入权限** → 粘贴以下内容：

```
im:message
im:message:send_as_bot
im:message.p2p_msg_readonly
im:message.group_msg_readonly
im:chat
im:chat.p2p_msg
im:chat.group_msg
im:resource:read
```

| 权限 | 用途 |
|------|------|
| `im:message` | 发送消息基础权限 |
| `im:message:send_as_bot` | 以机器人身份发送消息 |
| `im:message.p2p_msg_readonly` | 读取私聊消息 |
| `im:message.group_msg_readonly` | 读取群聊消息 |
| `im:chat` | 访问群/私聊信息 |
| `im:chat.p2p_msg` | 私聊消息事件 |
| `im:chat.group_msg` | 群聊消息事件 |
| `im:resource:read` | 下载消息中的文件/图片 |

## 4. 事件订阅

进入 **事件与回调 → 事件订阅**：

1. 勾选 **「使用长连接接收事件」**（无需公网域名）
2. 点击「添加事件」→ 搜索并添加 `im.message.receive_v1`（接收消息）

## 5. 获取凭证

进入 **凭证与基础信息**：

- 复制 **App ID** 和 **App Secret**
- 填入项目目录下的 `.env` 文件：

```env
APP_ID=cli_xxxxxx
APP_SECRET=xxxxxx
```

## 6. 发布应用

进入 **版本管理与发布 → 创建版本** → 提交审核。

- 如果是企业内部使用，审核通过后即可在组织内使用
- 机器人需要管理员在飞书管理后台中「启用」

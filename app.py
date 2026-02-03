from flask import Flask, request
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
from lark_oapi.api.im.v1 import *
from lark_oapi.api.drive.v1 import *
import json
import re
import os
import time
import requests  # 🆕 新增导入

processed_messages = set()

app = Flask(__name__)

# ============================================================
# 📌 配置区域
# ============================================================

APP_ID = os.environ.get("APP_ID", "")
APP_SECRET = os.environ.get("APP_SECRET", "")

FIELD_BATCH = "批次"

PROJECTS = [
    {
        "name": "货架",
        "app_token": "ADUtbWDICacuqisymHBc5doHnMd",
        "table_id": "tbloC4PHzAeRw2HR",
        "chat_ids": ["oc_8433370f765f6c1134e14c71c46615a9"]
    },
    {
        "name": "测试",
        "app_token": "ADUtbWDICacuqisymHBc5doHnMd",
        "table_id": "tbloC4PHzAeRw2HR",
        "chat_ids": ["oc_bf660fc9a537b568e4737e19c18bc73a"]
    },
]

# ============================================================
# 创建客户端
# ============================================================

def get_client():
    return lark.Client.builder() \
        .app_id(APP_ID) \
        .app_secret(APP_SECRET) \
        .build()


def get_access_token():
    """获取 tenant_access_token"""
    token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    response = requests.post(token_url, json={
        "app_id": APP_ID,
        "app_secret": APP_SECRET
    })
    data = response.json()
    if data.get("code") == 0:
        return data.get("tenant_access_token")
    print(f"  ❌ 获取token失败: {data}")
    return None

# ============================================================
# 业务函数
# ============================================================

def find_project_by_chat_id(chat_id):
    """根据群ID查找对应的项目"""
    for project in PROJECTS:
        if chat_id in project.get("chat_ids", []):
            return project
    return None


def find_records_by_batch(project, batch_name):
    """在指定项目中查找批次匹配的所有记录"""
    client = get_client()
    
    print(f"  🔍 查找批次: 「{batch_name}」")
    
    try:
        request_body = SearchAppTableRecordRequest.builder() \
            .app_token(project["app_token"]) \
            .table_id(project["table_id"]) \
            .request_body(SearchAppTableRecordRequestBody.builder()
                .filter(FilterInfo.builder()
                    .conjunction("and")
                    .conditions([
                        Condition.builder()
                            .field_name(FIELD_BATCH)
                            .operator("is")
                            .value([batch_name])
                            .build()
                    ])
                    .build())
                .build()) \
            .build()
        
        response = client.bitable.v1.app_table_record.search(request_body)
        
        print(f"  📊 搜索响应: success={response.success()}, code={response.code}, msg={response.msg}")
        
        if response.success() and response.data and response.data.items:
            print(f"  ✅ 找到 {len(response.data.items)} 条记录")
            return response.data.items
        else:
            print(f"  ⚠️ 未找到匹配记录")
            return []
            
    except Exception as e:
        print(f"  ❌ 搜索出错: {e}")
        import traceback
        traceback.print_exc()
        return []


def find_records_by_batch_in_all_projects(batch_name):
    """遍历所有项目查找批次匹配的记录"""
    all_matches = []
    for project in PROJECTS:
        records = find_records_by_batch(project, batch_name)
        if records:
            all_matches.append({
                "project": project,
                "records": records
            })
    return all_matches


def get_message_link(message_id):
    """生成飞书消息链接"""
    return f"https://applink.feishu.cn/client/message/link?token={message_id}"


def add_comment_to_record(project, record_id, comment_text):
    """给多维表格记录添加评论"""
    access_token = get_access_token()
    if not access_token:
        return False
    
    comment_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{project['app_token']}/tables/{project['table_id']}/records/{record_id}/comments"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "content": [
            {
                "type": "text",
                "text": comment_text
            }
        ]
    }
    
    response = requests.post(comment_url, headers=headers, json=payload)
    result = response.json()
    
    print(f"  📊 评论响应: code={result.get('code')}, msg={result.get('msg')}")
    
    if result.get("code") == 0:
        return True
    else:
        print(f"  ❌ 评论失败: {result}")
        return False


def reply_message(message_id, text):
    """回复消息"""
    client = get_client()
    content = json.dumps({"text": text})
    
    print(f"  💬 回复: {text[:50]}...")
    
    try:
        request_body = ReplyMessageRequest.builder() \
            .message_id(message_id) \
            .request_body(ReplyMessageRequestBody.builder()
                .msg_type("text")
                .content(content)
                .build()) \
            .build()
        
        response = client.im.v1.message.reply(request_body)
        
        if response.success():
            print(f"  💬 回复成功")
        else:
            print(f"  ❌ 回复失败: {response.code}, {response.msg}")
            
    except Exception as e:
        print(f"  ❌ 回复出错: {e}")


def handle_batch_feedback(message, chat_id):
    """处理批次反馈消息"""
    content = json.loads(message.get("content", "{}"))
    text = content.get("text", "")
    message_id = message.get("message_id")
    
    print(f"\n{'='*50}")
    print(f"收到消息: {text}")
    print(f"来自群聊: {chat_id}")
    
    # 匹配【xxx】物品需求反馈 格式
    match = re.search(r"【(.+?)】.*?物品需求反馈", text)
    if not match:
        print("未匹配到批次反馈格式")
        return False
    
    batch_name = match.group(1).strip()
    print(f"📦 识别到批次反馈: {batch_name}")
    
    message_link = get_message_link(message_id)
    print(f"🔗 消息链接: {message_link}")
    
    project = find_project_by_chat_id(chat_id)
    
    if project:
        print(f"📌 根据群ID匹配到项目: {project['name']}")
        records = find_records_by_batch(project, batch_name)
        
        if not records:
            reply_message(message_id, f"❌ 在「{project['name']}」中未找到批次「{batch_name}」")
            return True
        
        success_count = 0
        for record in records:
            comment_text = f"📬 收到物品需求反馈\n🔗 消息链接: {message_link}"
            if add_comment_to_record(project, record.record_id, comment_text):
                success_count += 1
                print(f"  ✅ 已评论记录: {record.record_id}")
            else:
                print(f"  ❌ 评论失败: {record.record_id}")
        
        reply_message(message_id, 
            f"✅ 已将反馈链接评论到「{project['name']}」批次「{batch_name}」的 {success_count}/{len(records)} 条记录")
        return True
    
    else:
        print(f"⚠️ 群 {chat_id} 未关联项目，搜索所有项目...")
        all_matches = find_records_by_batch_in_all_projects(batch_name)
        
        if not all_matches:
            reply_message(message_id, f"❌ 未找到批次「{batch_name}」")
            return True
        
        if len(all_matches) > 1:
            project_list = "\n".join([f"  • {m['project']['name']} ({len(m['records'])}条)" for m in all_matches])
            reply_message(message_id, 
                f"⚠️ 找到 {len(all_matches)} 个项目包含批次「{batch_name}」：\n{project_list}\n\n"
                f"请联系管理员配置群ID关联")
            return True
        
        project = all_matches[0]["project"]
        records = all_matches[0]["records"]
        
        success_count = 0
        for record in records:
            comment_text = f"📬 收到物品需求反馈\n🔗 消息链接: {message_link}"
            if add_comment_to_record(project, record.record_id, comment_text):
                success_count += 1
        
        reply_message(message_id, 
            f"✅ 已将反馈链接评论到「{project['name']}」批次「{batch_name}」的 {success_count}/{len(records)} 条记录")
        return True

# ============================================================
# Webhook 路由
# ============================================================

@app.route("/", methods=["GET"])
def index():
    return {
        "status": "running",
        "message": "🤖 批次反馈机器人运行中",
        "projects": [{"name": p["name"], "chat_ids": p.get("chat_ids", [])} for p in PROJECTS]
    }


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    
    if "challenge" in data:
        return {"challenge": data["challenge"]}
    
    try:
        header = data.get("header", {})
        event = data.get("event", {})
        
        event_type = header.get("event_type")
        if event_type != "im.message.receive_v1":
            return {"code": 0}
        
        message = event.get("message", {})
        message_id = message.get("message_id", "")
        chat_id = message.get("chat_id", "")

        create_time = message.get("create_time", "")
        if create_time:
            msg_time = int(create_time) / 1000
            if time.time() - msg_time > 300:
                print(f"忽略过旧的消息（超过5分钟）: {message_id}")
                return {"code": 0}
        
        if message_id in processed_messages:
            print(f"消息已处理，跳过: {message_id}")
            return {"code": 0}
        
        sender = event.get("sender", {})
        sender_type = sender.get("sender_type", "")
        if sender_type == "app":
            return {"code": 0}
        
        processed_messages.add(message_id)
        if len(processed_messages) > 1000:
            processed_messages.clear()
        
        handle_batch_feedback(message, chat_id)
            
    except Exception as e:
        print(f"处理出错: {e}")
        import traceback
        traceback.print_exc()
    
    return {"code": 0}

# ============================================================
# 启动
# ============================================================

if __name__ == "__main__":
    print("=" * 50)
    print("🤖 批次反馈机器人 (Webhook版)")
    print("=" * 50)
    print(f"APP_ID: {APP_ID[:10]}..." if APP_ID else "APP_ID: 未配置")
    print(f"已配置 {len(PROJECTS)} 个项目:")
    for p in PROJECTS:
        print(f"  - {p['name']} (关联 {len(p.get('chat_ids', []))} 个群)")
    print("=" * 50)
    
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port, debug=False)

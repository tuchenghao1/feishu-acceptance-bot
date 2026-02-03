from flask import Flask, request
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
from lark_oapi.api.im.v1 import *
from lark_oapi.api.drive.v1 import *
import json
import re
import os
import time
processed_messages = set()

app = Flask(__name__)

# ============================================================
# 📌 配置区域（根据实际情况修改）
# ============================================================

# 飞书应用凭证（从环境变量读取，更安全）
APP_ID = os.environ.get("APP_ID", "")
APP_SECRET = os.environ.get("APP_SECRET", "")

# 字段名称（根据你的表格字段名修改）
FIELD_BATCH = "批次"

# 项目配置（新增项目在这里添加）
# 🆕 添加 chat_ids 字段，关联项目群
PROJECTS = [
    {
        "name": "货架",
        "app_token": "ADUtbWDICacuqisymHBc5doHnMd",
        "table_id": "tbloC4PHzAeRw2HR",
        "chat_ids": ["oc_8433370f765f6c1134e14c71c46615a9"]  # Goodsort&图灵项目沟通群 群ID
    },
    # 新增项目模板：
    # {
    #     "name": "新项目名称",
    #     "app_token": "从URL的base/后面复制",
    #     "table_id": "从URL的table=后面复制",
    #     "chat_ids": ["oc_xxx", "oc_yyy"]  # 可以配置多个群
    # },
]

# ============================================================
# 创建客户端
# ============================================================

def get_client():
    return lark.Client.builder() \
        .app_id(APP_ID) \
        .app_secret(APP_SECRET) \
        .build()

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
    if response.success() and response.data.items:
        return response.data.items
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
    client = get_client()
    
    request_body = CreateAppTableRecordCommentRequest.builder() \
        .app_token(project["app_token"]) \
        .table_id(project["table_id"]) \
        .record_id(record_id) \
        .request_body(CreateAppTableRecordCommentRequestBody.builder()
            .content(comment_text)
            .build()) \
        .build()
    
    response = client.bitable.v1.app_table_record_comment.create(request_body)
    return response.success()


def reply_message(message_id, text):
    """回复消息"""
    client = get_client()
    content = json.dumps({"text": text})
    request_body = ReplyMessageRequest.builder() \
        .message_id(message_id) \
        .request_body(ReplyMessageRequestBody.builder()
            .msg_type("text")
            .content(content)
            .build()) \
        .build()
    
    client.im.v1.message.reply(request_body)


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
        return False
    
    batch_name = match.group(1).strip()
    print(f"📦 识别到批次反馈: {batch_name}")
    
    # 生成消息链接
    message_link = get_message_link(message_id)
    print(f"🔗 消息链接: {message_link}")
    
    # 确定项目
    project = find_project_by_chat_id(chat_id)
    
    if project:
        # 根据群ID找到对应项目
        records = find_records_by_batch(project, batch_name)
        print(f"📌 根据群ID匹配到项目: {project['name']}")
        
        if not records:
            reply_message(message_id, f"❌ 在「{project['name']}」中未找到批次「{batch_name}」")
            return True
        
        # 给所有匹配的记录添加评论
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
        # 未配置群ID，搜索所有项目
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
        
        # 只有一个项目匹配
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
    """首页 - 用于检查服务状态"""
    return {
        "status": "running",
        "message": "🤖 需求验收机器人运行中",
        "projects": [{"name": p["name"], "chat_ids": p.get("chat_ids", [])} for p in PROJECTS]
    }

@app.route("/webhook", methods=["POST"])
def webhook():
    """接收飞书事件回调"""
    data = request.json
    
    # URL 验证（飞书首次配置时会发送）
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
        chat_id = message.get("chat_id", "")  # 🆕 获取群ID

        # ========== 忽略旧消息 ==========
        create_time = message.get("create_time", "")
        if create_time:
            msg_time = int(create_time) / 1000
            if time.time() - msg_time > 300:
                print(f"忽略过旧的消息（超过5分钟）: {message_id}")
                return {"code": 0}
        # =============================================
        
        # 消息去重
        if message_id in processed_messages:
            print(f"消息已处理，跳过: {message_id}")
            return {"code": 0}
        
        # 过滤机器人自己发的消息
        sender = event.get("sender", {})
        sender_type = sender.get("sender_type", "")
        if sender_type == "app":
            print("跳过机器人自己的消息")
            return {"code": 0}
        
        # 记录已处理的消息
        processed_messages.add(message_id)
        
        # 限制集合大小，防止内存溢出
        if len(processed_messages) > 1000:
            processed_messages.clear()
        
        # 🆕 处理验收消息（传入 chat_id）
        handle_acceptance(message, chat_id)
            
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
    print("🤖 需求验收机器人 (Webhook版)")
    print("=" * 50)
    print(f"APP_ID: {APP_ID[:10]}..." if APP_ID else "APP_ID: 未配置")
    print(f"已配置 {len(PROJECTS)} 个项目:")
    for p in PROJECTS:
        chat_ids = p.get("chat_ids", [])
        print(f"  - {p['name']} (关联 {len(chat_ids)} 个群)")
    print("=" * 50)
    
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port, debug=False)

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
FIELD_REQUIREMENT = "需求内容"
FIELD_STATUS = "验收状态"
FIELD_ATTACHMENT = "验收附件"
STATUS_VALUE = "验收通过"

# 项目配置（新增项目在这里添加）
# 🆕 添加 chat_ids 字段，关联项目群
PROJECTS = [
    {
        "name": "项目1",
        "app_token": "Q8...",
        "table_id": "tb...",
        "chat_ids": ["oc_xxx"]  # JigArt 项目群ID
    },
    {
        "name": "项目2",
        "app_token": "Ok...",
        "table_id": "tb...",
        "chat_ids": ["oc_yyy"]  # BusJam 项目群ID
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
    """🆕 根据群ID查找对应的项目"""
    for project in PROJECTS:
        if chat_id in project.get("chat_ids", []):
            return project
    return None

def find_project_by_name(project_name):
    """🆕 根据项目名查找项目"""
    for project in PROJECTS:
        if project["name"].lower() == project_name.lower():
            return project
    return None

def find_record_in_all_projects(requirement_name):
    """遍历所有项目查找需求"""
    for project in PROJECTS:
        record = find_record(project, requirement_name)
        if record:
            return project, record
    return None, None

def find_record_in_all_projects_v2(requirement_name):
    """🆕 遍历所有项目查找需求，返回所有匹配"""
    matches = []
    for project in PROJECTS:
        record = find_record(project, requirement_name)
        if record:
            matches.append({"project": project, "record": record})
    return matches

def find_record(project, requirement_name):
    """在指定项目中查找需求（排除已验收的）"""
    client = get_client()
    request_body = SearchAppTableRecordRequest.builder() \
        .app_token(project["app_token"]) \
        .table_id(project["table_id"]) \
        .request_body(SearchAppTableRecordRequestBody.builder()
            .filter(FilterInfo.builder()
                .conjunction("and")
                .conditions([
                    # 条件1：需求内容精确匹配
                    Condition.builder()
                        .field_name(FIELD_REQUIREMENT)
                        .operator("is")
                        .value([requirement_name])
                        .build(),
                    # 条件2：验收状态不是"验收通过"（排除已验收的）
                    Condition.builder()
                        .field_name(FIELD_STATUS)
                        .operator("isNot")
                        .value([STATUS_VALUE])
                        .build()
                ])
                .build())
            .build()) \
        .build()
    
    response = client.bitable.v1.app_table_record.search(request_body)
    if response.success() and response.data.items:
        return response.data.items[0]
    return None

def update_record(project, record_id, attachments=None):
    """更新验收状态和附件"""
    client = get_client()
    fields = {FIELD_STATUS: STATUS_VALUE}
    if attachments:
        fields[FIELD_ATTACHMENT] = attachments
    
    request_body = UpdateAppTableRecordRequest.builder() \
        .app_token(project["app_token"]) \
        .table_id(project["table_id"]) \
        .record_id(record_id) \
        .request_body(AppTableRecord.builder()
            .fields(fields)
            .build()) \
        .build()
    
    response = client.bitable.v1.app_table_record.update(request_body)
    return response.success()

def get_parent_message(message_id):
    """获取引用的原始消息"""
    client = get_client()
    request_body = GetMessageRequest.builder() \
        .message_id(message_id) \
        .build()
    
    response = client.im.v1.message.get(request_body)
    if response.success() and response.data.items:
        return response.data.items[0]
    return None

def download_resource(message_id, file_key, res_type):
    """下载消息中的图片/视频"""
    client = get_client()
    request_body = GetMessageResourceRequest.builder() \
        .message_id(message_id) \
        .file_key(file_key) \
        .type(res_type) \
        .build()
    
    response = client.im.v1.message_resource.get(request_body)
    if response.success():
        return response.file.read()
    return None

def upload_to_bitable(project, file_content, file_name):
    """上传文件到多维表格"""
    import io
    client = get_client()
    file_obj = io.BytesIO(file_content)
    file_obj.name = file_name
    
    request_body = UploadAllMediaRequest.builder() \
        .request_body(UploadAllMediaRequestBody.builder()
            .file_name(file_name)
            .parent_type("bitable_file")
            .parent_node(project["app_token"])
            .size(len(file_content))
            .file(file_obj)
            .build()) \
        .build()
    
    response = client.drive.v1.media.upload_all(request_body)
    if response.success():
        return response.data.file_token
    return None

def extract_attachments(project, parent_message):
    """提取并上传引用消息中的附件"""
    attachments = []
    if not parent_message:
        return attachments
    
    message_id = parent_message.message_id
    msg_type = parent_message.msg_type
    content = json.loads(parent_message.body.content)
    
    print(f"  引用消息类型: {msg_type}")
    
    # 单张图片
    if msg_type == "image":
        image_key = content.get("image_key")
        if image_key:
            print(f"  下载图片: {image_key}")
            file_content = download_resource(message_id, image_key, "image")
            if file_content:
                file_token = upload_to_bitable(project, file_content, f"{image_key}.png")
                if file_token:
                    attachments.append({"file_token": file_token})
                    print(f"  ✅ 图片上传成功")
    
    # 视频
    elif msg_type == "media":
        file_key = content.get("file_key")
        if file_key:
            print(f"  下载视频: {file_key}")
            file_content = download_resource(message_id, file_key, "file")
            if file_content:
                file_token = upload_to_bitable(project, file_content, f"{file_key}.mp4")
                if file_token:
                    attachments.append({"file_token": file_token})
                    print(f"  ✅ 视频上传成功")
    
    # 富文本消息（可能包含多张图片）
    elif msg_type == "post":
        post_content = content.get("content", [])
        for line in post_content:
            for element in line:
                if element.get("tag") == "img":
                    image_key = element.get("image_key")
                    if image_key:
                        print(f"  下载图片: {image_key}")
                        file_content = download_resource(message_id, image_key, "image")
                        if file_content:
                            file_token = upload_to_bitable(project, file_content, f"{image_key}.png")
                            if file_token:
                                attachments.append({"file_token": file_token})
                                print(f"  ✅ 图片上传成功")
    
    return attachments

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

def handle_acceptance(message, chat_id):
    """🆕 处理验收消息（增加 chat_id 参数）"""
    content = json.loads(message.get("content", "{}"))
    text = content.get("text", "")
    message_id = message.get("message_id")
    parent_id = message.get("parent_id")
    
    print(f"\n{'='*50}")
    print(f"收到消息: {text}")
    print(f"来自群聊: {chat_id}")
    
    # 匹配【验收通过】
    match = re.search(r"【验收通过】(.+)", text)
    if not match:
        return
    
    full_text = match.group(1).strip()
    # 去除可能的@机器人文本
    full_text = re.sub(r"@\S+\s*", "", full_text).strip()
    
    # 🆕 解析项目名和需求内容
    specified_project_name = None
    requirement_name = full_text
    
    if "/" in full_text:
        parts = full_text.split("/", 1)
        specified_project_name = parts[0].strip()
        requirement_name = parts[1].strip()
    
    print(f"指定项目: {specified_project_name or '未指定'}")
    print(f"需求内容: {requirement_name}")
    
    # 🆕 确定项目的优先级：
    # 1. 如果消息中指定了项目名，使用指定的项目
    # 2. 否则根据群ID自动匹配项目
    # 3. 如果群ID也没匹配到，搜索所有项目
    
    project = None
    record = None
    
    if specified_project_name:
        # 方式1：使用消息中指定的项目名
        project = find_project_by_name(specified_project_name)
        if not project:
            project_names = ', '.join([p['name'] for p in PROJECTS])
            reply_message(message_id, f"❌ 未找到项目「{specified_project_name}」\n可用项目: {project_names}")
            return
        record = find_record(project, requirement_name)
        print(f"📌 使用指定项目: {project['name']}")
        
    else:
        # 方式2：根据群ID自动匹配
        project = find_project_by_chat_id(chat_id)
        
        if project:
            # 找到了对应的项目群
            record = find_record(project, requirement_name)
            print(f"📌 根据群ID自动匹配到项目: {project['name']}")
        else:
            # 方式3：未配置群ID，搜索所有项目
            print(f"⚠️ 群 {chat_id} 未关联项目，搜索所有项目...")
            matches = find_record_in_all_projects_v2(requirement_name)
            
            if len(matches) == 0:
                reply_message(message_id, f"❌ 未找到需求「{requirement_name}」")
                return
            elif len(matches) > 1:
                # 找到多个匹配，提示用户
                project_list = "\n".join([f"  • {m['project']['name']}" for m in matches])
                reply_message(message_id, 
                    f"⚠️ 找到 {len(matches)} 个同名需求：\n{project_list}\n\n"
                    f"请使用格式：【验收通过】项目名/{requirement_name}\n"
                    f"或联系管理员配置群ID关联")
                return
            else:
                project = matches[0]["project"]
                record = matches[0]["record"]
                print(f"📌 全局搜索找到唯一匹配: {project['name']}")
    
    # 检查是否找到需求
    if not record:
        reply_message(message_id, f"❌ 在「{project['name']}」中未找到需求「{requirement_name}」")
        print(f"❌ 未找到需求")
        return
    
    print(f"✅ 在「{project['name']}」中找到需求")
    
    # 处理附件
    attachments = []
    if parent_id:
        print(f"检测到引用消息，处理附件...")
        parent_message = get_parent_message(parent_id)
        attachments = extract_attachments(project, parent_message)
    
    # 更新记录
    if update_record(project, record.record_id, attachments):
        attachment_info = f"\n📎 已同步 {len(attachments)} 个附件" if attachments else ""
        reply_message(message_id, f"✅ 「{project['name']}」需求「{requirement_name}」验收通过{attachment_info}")
        print(f"✅ 更新成功")
    else:
        reply_message(message_id, f"❌ 更新失败，请重试")
        print(f"❌ 更新失败")

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

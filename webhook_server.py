#!/usr/bin/env python3
"""
Finger 话术质检 Webhook 服务 - 完整版
支持：实时分析 + 员工识别 + 翻译 + 回复建议
"""
import json, os, logging, re, threading, time, queue
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify, Response

BJ_TZ = timezone(timedelta(hours=8))
def bj_now(): return datetime.now(BJ_TZ)

PORT = int(os.environ.get("PORT", 8888))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, "qc_sessions.json")

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# SSE
sse_clients = []; sse_lock = threading.Lock(); recent_msgs = []; recent_lock = threading.Lock()
def broadcast(d):
    with recent_lock:
        recent_msgs.append(d)
        if len(recent_msgs) > 100: recent_msgs.pop(0)
    with sse_lock:
        p = f"data: {json.dumps(d, ensure_ascii=False)}\n\n"
        dead = []
        for q in sse_clients:
            try: q.put_nowait(p)
            except: dead.append(q)
        for q in dead: sse_clients.remove(q)

# Data persistence
data_lock = threading.Lock()
def load_data():
    with data_lock:
        if os.path.exists(DATA_FILE):
            try: return json.load(open(DATA_FILE,'r',encoding='utf-8'))
            except: pass
        return {"sessions":{}, "total_messages":0, "all_scores":[]}
def save_data(d):
    with data_lock:
        json.dump(d, open(DATA_FILE,'w',encoding='utf-8'), ensure_ascii=False, indent=2)
def get_session(phone):
    d = load_data()
    if phone not in d["sessions"]:
        d["sessions"][phone] = {"phone":phone,"messages":[],"score_history":[],"contact_name":"","source":"","stage":"unknown","start_time":bj_now().isoformat(),"last_activity":bj_now().isoformat(),"employee":""}
    return d["sessions"][phone], d
def save_session(phone,s,d):
    d["sessions"][phone]=s; save_data(d)

# Employee detection
_emp_cache = {}
# Complete known staff list
ALL_STAFF = ['mia','andy','cici','jestina','sophia','小枫','may','john','fong','iris','christine','vicky','lily']

def identify_employee(text, phone=""):
    if phone and phone in _emp_cache: return _emp_cache[phone]
    t = text.lower().strip()
    name = '员工'
    # 1. Direct name mention (anywhere in text)
    for staff in ALL_STAFF:
        if staff in t:
            name = staff.capitalize() if staff == staff.lower() else staff
            break
    # 2. Intro patterns: "I'm X" / "我是X" / "this is X"
    if name == '员工':
        m = re.search(r'(?:我.?是|i.?m|i am|this is|name is)\s*(?:医生助理|种植牙客服|美芽|me(?:ya)?.?dental.?的)?\s*(mia|andy|cici|jestina|sophia|小枫|may|john|fong|iris|christine|vicky|lily)', t, re.IGNORECASE)
        if m: name = m.group(1).capitalize() if m.group(1) == m.group(1).lower() else m.group(1)
    # 3. Role-based detection (no explicit name mentioned)
    if name == '员工':
        if re.search(r'consultation assistant|dental technology', t): name = 'John'
        elif '我是 meya dental 的' in t or '免费为你提供' in t: name = '🤖 Bot'
        # Mia: emoji + greeting or dental context (her signature style)
        elif re.search(r'😉|🥰|🤗|😊|😃', t):
            if re.search(r'(?:种植|implant|dental|诊所|牙|teeth|tooth|promotion|rm|puchong|pfcc|医生|检查|免费|预约|您好|你好|早上好|hi|hello|住|哪里|方便|明天)', t):
                name = 'Mia'
        # Fong/其他: 医生助理 + 电话号 + meya
        elif re.search(r'(?:01[0-9]-\d{3,})', t) and re.search(r'医生助理|助理|meya|美芽|dental', t):
            name = 'Fong'
        # Cici: 医生助理
        elif re.search(r'医生助理|助理', t) and re.search(r'meya|美芽|dental', t): name = 'Cici'
    if phone and name not in ('员工','🤖 Bot'): _emp_cache[phone] = name
    return name

# Translation
def translate_text(text):
    if not text or re.search(r'[\u4e00-\u9fff]', text): return ""
    try:
        from deep_translator import GoogleTranslator
        r = GoogleTranslator(source='auto', target='zh-CN').translate(text[:800])
        return r if r and r != text else ""
    except: return ""

# ====== AI 建议引擎 ======
DEEPSEEK_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"

# Finger 话术知识库
KNOWLEDGE = '''你是美芽口腔（MEYA Dental PFCC Puchong）的销售话术专家Finger。
以下是核心话术知识：

### 基本信息
- 地址：Puchong PFCC，Tower 4&5
- 专做种植牙专科诊所，十几年经验
- 活动价：RM3980/颗（原价RM6K），含种植体+基台+牙冠+手术+CBCT
- 市场价：其他诊所RM6000-8000/颗
- 免费检查：CBCT拍片+医生面诊（价值RM400-600）
- 提供接送服务

### 核心话术原则
1. 先问缺牙情况（缺几颗、位置、多久），不直接报价
2. 报价前对比市场价："原价6K现在3980"
3. 每轮对话提免费检查
4. 邀约降低门槛："先检查了解，不一定要做"
5. 客户说贵→"公益义诊，即使不做也免费"
6. 客户犹豫→"完全理解，先看看情况"
7. 共情比推销有效，先接住情绪
8. 客户说远→有接送

### 常见场景话术

#### 问价格
先问几颗/位置/多久，再报RM3980对比市场6-8K，最后邀约免费检查。

#### 半口/全口缺失
不说缺几颗。对比两种方式：
1. 活动假牙：不舒服、咬不了硬的、压迫牙槽骨加速吸收、松动、一两年换
2. 种植牙：四个牙根+纯钛支架+12颗牙，恢复半口功能
引检查：要拍CBCT看骨头情况定方案

#### 嫌贵
"公益义诊，即使不做也免费，先检查再决定"

#### 怕痛
无痛种植，15-20分钟，比拔牙轻松

#### 对比牙桥
种植牙不磨好牙，受力90%以上；牙桥磨两颗好牙做桥墩永久损伤

#### 对比别家
只做种植牙，医生每天做经验集中，设备专业

#### 预约
留姓名电话，安排时间，到楼下接

#### 英文客户
RM3,980 per implant (orig RM6K). Free check-up. Korea Osstem.

### 注意事项
- 每条回复控制在100字以内
- 语气友好温暖，用😊🤗emoji
- 不说过度承诺的话
- **不要每句话都邀约。根据对话阶段决定：开场/问情况时不需要邀约，报价后/客户有意向时再邀约**
'''

def ai_suggest(text):
    if not DEEPSEEK_KEY: return None
    prompt = f"{KNOWLEDGE}\n\n客户发来消息：\"{text[:300]}\"\n\n请根据话术知识，给出3条建议回复。一条一行。不要序号。"
    try:
        import requests
        r = requests.post(DEEPSEEK_URL, json={
            "model": "deepseek-chat",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 300,
            "temperature": 0.7
        }, headers={
            "Authorization": f"Bearer {DEEPSEEK_KEY}",
            "Content-Type": "application/json"
        }, timeout=15)
        if r.status_code == 200:
            lines = [l.strip() for l in r.json()["choices"][0]["message"]["content"].split('\n') if l.strip()]
            return [l for l in lines if len(l) > 5][:3]
    except:
        return None
    return None

# 备用规则（AI不可用时）
SCENARIOS = [
    (r"多少钱|price|how much|cost|harga|berapa|rm\s*\\?", [
        "先问：\"请问您缺了几颗牙？在什么位置？缺了多久了？😊\"",
        "报价：\"现在活动RM3980全包，市场价6000-8000\"",
        "邀约：\"有免费CBCT检查，您方便过来先了解再决定？\""
    ]),
    (r"半口|separa|half.*mouth|全口|活动.*假牙|假牙.*松|咬不了", [
        "对比假牙：\"活动假牙不舒服、咬不了硬的、压迫牙槽骨、容易松动、一两年换\"",
        "推荐种植：\"做四个牙根+纯钛支架+12颗牙，恢复半口功能\"",
        "引检查：\"具体哪种要拍CBCT看骨头，我们有免费检查\""
    ]),
    (r"贵|太贵|mahal|expensive|budget|超出.*预算|优惠|discount", [
        "接顾虑：\"公益义诊，即使不做全部免费🤝\"",
        "转价值：\"先免费CBCT检查，医生出方案您看了再决定\"",
        "强调：\"外面CBCT要RM400+，我们是免费的\""
    ]),
    (r"怕痛|痛不痛|sakit|pain|害怕|恐惧", [
        "共情：\"很多人第一次听到都以为会很痛😊\"",
        "解释：\"无痛种植，15-20分钟，比拔牙轻松\"",
        "安心：\"很多患者种完说比想象中简单\""
    ]),
    (r"牙桥|假牙|活动牙|bridge|denture|gigi palsu", [
        "对比：\"种植牙不磨好牙，受力恢复90%以上\"",
        "点差：\"牙桥要磨两颗好牙做桥墩，永久损伤\"",
        "引检：\"来免费检查，医生拿模型比划一下就明白\""
    ]),
    (r"远|太远|jauh|距离|location|地址", [
        "发定位：\"Puchong PFCC，靠近MRT站📍\"",
        "接送：\"我们有安排接送的车\"",
        "邀约：\"您看今天下午方便来免费检查？\""
    ]),
    (r"考虑|想想|saya fikir|think|need.*time|再想想|回头|改天|slowly", [
        "理解：\"完全理解，做牙齿需要慎重决定😊\"",
        "降门槛：\"先预约免费检查名额，不急着决定\"",
        "引导：\"免费检查不花钱，检查了再决定\""
    ]),
    (r"区别|不同|为什么选|比.*好|别家|比较", [
        "专科：\"只做种植牙，医生每天做经验集中\"",
        "设备：\"专业CBCT+种植手术室\"",
        "引检：\"先来免费检查感受一下\""
    ]),
    (r"预约|appointment|book|temujanji|过来|visit|come", [
        "确认：\"您方便什么时间来？帮您预约免费检查\"",
        "留信息：\"留姓名电话，给您登记😊\"",
        "发定位：\"到时会安排同事接您🤝\""
    ]),
    (r"能用多久|耐用|寿命|lasting|life|tahan|lama", [
        "信心：\"保养好可以用几十年甚至一辈子\"",
        "价值：\"国际一线品牌+终身免费复查\"",
        "条件：\"医生技术+种植体品质+保养，我们都到位\""
    ]),
    (r"你好|hello|hi|hai|selamat|早上好|下午好", [
        "开场：\"请问您缺了几颗牙？什么位置？😊\"",
        "介绍：\"我是MEYA dental的，现在有免费CBCT检查\"",
        "选项：\"了解种植牙回复1，了解诊所回复2\""
    ]),
    (r"英文|english|eng|inggeris", [
        "Eng intro: \"We provide FREE check-up + CBCT scan.\"",
        "Eng price: \"RM3,980/implant (orig RM6K).\"",
        "Eng booking: \"May I schedule a free check-up?\""
    ]),
    (r"b[ah]asa|bm|malay|melayu", [
        "Malay: \"Selamat sejahtera! Pemeriksaan PERCUMA.\"",
        "Harga: \"RM3,980 untuk 1 implan.\"",
        "Temujanji: \"Nak saya bantu aturkan?\""
    ]),
]
DEFAULT_SUG = [
    "了解：\"缺了几颗牙？什么位置？缺了多久？\"",
    "引检查：\"有免费CBCT+医生面诊，过来了解？\"",
    "控节奏：\"先检查了解，做不做没关系😊\""
]

def detect_scenario(text):
    # 优先用 AI
    r = ai_suggest(text)
    if r: return r
    # AI 不可用时用规则
    t = text.lower().strip()
    words = set(re.findall(r'[a-z]+',t))
    if len(words & {'selamat','saya','anda','boleh','gigi','tanam','harga','klinik','percuma'}) >= 3:
        return ["Malay intro","Free checkup available","Book appointment?"]
    for pat,sug in SCENARIOS:
        if re.search(pat, t): return sug[:3]
    return DEFAULT_SUG[:3]



@app.route("/webhook", methods=["GET","POST"])
def webhook():
    if request.method == "GET":
        ch = request.args.get("challenge")
        if ch: return ch, 200, {"Content-Type":"text/plain"}
        return "Webhook OK", 200
    d = request.get_json(silent=True) or {}
    et = d.get("type","")
    if et == "whatsapp.inbound_message.received": handle_inbound(d.get("whatsappInboundMessage",{}))
    elif et == "whatsapp.message.updated": handle_outbound(d.get("whatsappMessage",{}))
    return jsonify({"status":"ok"}), 200

def generate_training_scripts(text):
    """为新客户问题自动生成3条建议话术"""
    t = text.lower().strip()
    
    # 马来文
    if any(w in t for w in ['selamat','saya','anda','boleh','gigi','tanam','harga','klinik','percuma','melayu','bahasa']):
        return [
            "Selamat sejahtera! Ada apa-apa yang kami boleh bantu? Kami ada pemeriksaan PERCUMA termasuk CBCT scan. Klinik kami pakar implan gigi di Puchong PFCC. 😊",
            "Jom dtg pemeriksaan percuma dulu. Doctor akan check dan bagi tau option yg sesuai. Takde kewajipan langsung. Lepas tu baru bincang harga dan plan.",
            "InsyaAllah kami boleh bantu. Pemeriksaan PERCUMA dulu, lepas tu baru kita tengok dan buat keputusan. Takpe kalau tak jadi pun. 😊"
        ]
    
    # 英文 - price
    if any(w in t for w in ['how much','cost','price','rm','rate','fee','ching']):
        return [
            "We have a promo: RM3,980 per implant (orig RM6K), inclusive of CBCT, implant, crown & whole life free checkup. Free consultation first!",
            "Same Korean implant brand (Osstem) that other clinics charge RM6K-8K for. We offer RM3,980 all-in because we're a specialist clinic.",
            "Come for a free check-up first, get a proper assessment and exact quote, then decide. No obligation 😊"
        ]
    
    if any(w in t for w in ['location','where','address','lokasi','附近']):
        return [
            "Kami di Puchong PFCC, NO.1-F Tower 4&5@PFCC. Free shuttle! Which area are you from?",
            "📍 PFCC Puchong (near MRT). We provide free pickup service.",
            "Let me know your area and I can arrange pickup for your free check-up visit 😊"
        ]
    
    if any(w in t for w in ['appointment','schedule','book','consultation','temujanji','预约']):
        return [
            "Of course! What time works for you? Open 10am-7pm daily. Free check-up included.",
            "Sure! May I have your name and contact to reserve a slot? Free CBCT + consultation available.",
            "Let me help you book! We offer free check-up with CBCT scan and doctor consultation."
        ]
    
    if any(w in t for w in ['english','please','pls','can ar']):
        return [
            "Sure! I can assist in English. What dental concern? Free check-up with CBCT available. 😊",
            "Happy to help in English! We specialize in dental implants at Puchong PFCC.",
            "No problem, I'll continue in English. May I know your dental issue? Free consultation here!"
        ]
    
    if any(w in t for w in ['wisdom','pain','hurt','swell','sakit','痛','疼','肿','疼']):
        return [
            "Sorry to hear! Let's do a free X-ray to check the tooth position. Quick procedure with local anesthesia.",
            "Don't suffer! Free check-up + X-ray to see what's happening. Removal is quick and manageable.",
            "Let's take a look first. Free CBCT scan to assess. You'll know what to do after that."
        ]
    
    if any(w in t for w in ['free check','percuma','免费']):
        return [
            "YES! We offer free CBCT scan + doctor consultation (worth RM400+ elsewhere). No strings attached.",
            "Absolutely free! Full mouth CBCT, doctor examination, treatment plan discussion. Would you like to book?",
            "Free check-up includes CBCT panoramic X-ray + doctor consultation. Come and find out your options 😊"
        ]
    
    if any(w in t for w in ['hello','hi','hai','您好','你好','info']):
        return [
            "Welcome! Are you looking for dental implants or general check-up? Free CBCT scan at Puchong PFCC.",
            "Hello! Free consultation available 😊 What can I help you with today?",
            "Hi there! Let me know your dental concern. We specialize in implants at Puchong PFCC."
        ]
    
    # 中文 - 价格
    if re.search(r'[\u4e00-\u9fff]', text):
        if any(w in t for w in ['多少','价格','费用','贵','多少钱']):
            return [
                "先问一下您的情况：缺了几颗牙？在哪个位置？缺了多久了？不同情况方案和价格不一样。",
                "我们现在活动价RM3,980一颗全包（种植体+基台+牙冠+CBCT）。外面市场价RM6,000-8,000。",
                "先来做个免费检查，医生看了情况出方案和准确报价，了解清楚再决定😊"
            ]
        if any(w in t for w in ['种','植','牙','牙套','牙桥']):
            return [
                "种植牙独立种在牙槽骨里，不磨好牙，咀嚼力恢复90%以上，保养好可以用几十年。",
                "我们有免费CBCT检查，先来拍片看看骨头条件，再定最适合的方案。",
                "您方便过来做个免费检查吗？医生当面给您分析，比文字沟通清楚多啦😊"
            ]
        # 其他中文
        return [
            "先帮您了解一下具体情况。什么牙齿问题呢？缺牙还是牙痛？有几颗？多久了？",
            "我们有免费CBCT检查+医生面诊，先来看看了解清楚，再决定做不做😊",
            "别担心，先来免费检查了解情况。医生会帮您全面评估，给最适合的方案建议。"
        ]
    
    # 英文默认
    return [
        "Let me understand your situation. What dental issue are you facing? Free assessment available.",
        "Come for a free CBCT scan + doctor consultation. We'll give you a clear plan and quote.",
        "No pressure, just come for a free check-up first. Understand your options, then decide 😊"
    ]


def handle_inbound(msg):
    msg_type = msg.get("type","")
    p = msg.get("from",""); n = msg.get("customerProfile",{}).get("name","")
    s = ""; ref = msg.get("referral",{})
    if isinstance(ref,dict): s = "fb_ad" if "fb" in ref.get("source_url","") else ref.get("source_type","") or "direct"
    if not p: return
    
    # 处理不同类型的消息
    content = ""
    if msg_type == "text":
        td = msg.get("text",{}); c = td.get("body","") if isinstance(td,dict) else str(td)
        content = c
    elif msg_type in ("audio", "voice"):
        content = "🎤 [语音消息]"
    elif msg_type == "image":
        content = "🖼️ [图片]"
    elif msg_type == "video":
        content = "🎬 [视频]"
    elif msg_type == "document":
        content = "📄 [文件]"
    elif msg_type == "location":
        content = "📍 [位置]"
    elif msg_type == "sticker":
        content = "🟨 [贴纸]"
    else:
        content = f"[{msg_type or '其他'}]"
    session, data = get_session(p)
    if n: session["contact_name"] = n
    if s: session["source"] = s
    session["last_activity"] = bj_now().isoformat()
    data["total_messages"] = data.get("total_messages",0)+1
    session["messages"].append({"type":"customer","content":content,"time":bj_now().isoformat()})
    c = content
    tag = n or p[-4:]
    save_session(p,session,data)
    # 自动加入话术培训（仅文本消息）
    if msg_type == "text" and c.strip() and len(c) > 5:
        try:
            td = load_training_data()
            # 去重：检查最近50条是否有相同问题
            dup = False
            for tp in td["training_pairs"][-50:]:
                if tp["customer_question"].strip() == c.strip():
                    dup = True
                    break
            if not dup:
                nid = f"q_{len(td['training_pairs']) + 1:03d}"
                my_suggestions = generate_training_scripts(c.strip())
                while len(my_suggestions) < 3:
                    my_suggestions.append("")
                answers = [{"text": ""}, {"text": my_suggestions[0]}, {"text": my_suggestions[1]}, {"text": my_suggestions[2]}, {"text": ""}]
                td["training_pairs"].append({
                    "id": nid, "customer_question": c.strip(),
                    "source": f"客户 {tag}", "status": "pending",
                    "answers": answers, "best_answer_index": None,
                    "created_at": bj_now().isoformat(), "notes": "自动记录"
                })
                save_training_data(td)
                log.info(f"📚 已加入培训: {nid} - {c[:50]} (含3条推荐话术)")
        except Exception as e:
            log.error(f"培训记录失败: {e}")
    log.info(f"📩 {tag}: {c[:60]}")
    now = bj_now().strftime("%H:%M")
    cn = translate_text(c)
    sug = detect_scenario(c)
    m = {"type":"customer","name":tag,"content":c,"source":s,"time":now,"suggestions":sug}
    if cn: m["cn"] = cn
    broadcast(m)

def handle_outbound(msg):
    td = msg.get("text",{}); c = td.get("body","") if isinstance(td,dict) else str(td)
    p = msg.get("to",""); st = msg.get("status","")
    if not c or (st and st!="sent"): return
    if not p: p = f"u_{int(time.time())}"
    session, data = get_session(p)
    analysis = analyze_message(c)
    session["messages"].append({"type":"emp","content":c,"time":bj_now().isoformat(),"emp_name":emp})
    session["score_history"].append({"score":analysis["score"],"time":bj_now().isoformat()})
    data["all_scores"].append(analysis["score"])
    data["total_messages"] = data.get("total_messages",0)+1
    tag = session.get("contact_name","") or p[-4:]
    emp = identify_employee(c, p)
    if emp not in ('员工','🤖 Bot'):
        session["employee"] = emp
    # Stage tracking: update stage based on message patterns
    c_lower = c.lower()
    current_stage = session.get("stage","unknown")
    if current_stage == "unknown" and re.search(r'(?:hi|hello|hai|您好|你好|咨询|ask|info)', c_lower):
        session["stage"] = "initial_contact"
    elif current_stage in ("unknown","initial_contact") and re.search(r'(?:多少|price|cost|rm|how much|harga)', c_lower):
        session["stage"] = "price_inquiry"
    elif current_stage in ("unknown","initial_contact","price_inquiry") and re.search(r'(?:预约|appointment|book|schedule|过来|come|visit|dtg|jom)', c_lower):
        session["stage"] = "booking"
    elif re.search(r'(?:ok|好的|setuju|agree|done|thank)', c_lower) and "booking" not in c_lower:
        # Don't auto-downgrade - only upgrade
        pass
    save_session(p,session,data)
    log.info(f"📤 [{emp}] {tag}: {c[:60]} | {analysis['score']}")
    for i in analysis["issues"]: log.info(f"  ⚠️ {i['label']}")
    now = bj_now().strftime("%H:%M")
    cn = translate_text(c)
    bcast = {"type":"employee","employee":emp,"content":c,"score":analysis["score"],"issues":analysis["issues"],"time":now}
    if cn: bcast["cn"] = cn
    broadcast(bcast)
    if analysis["score"]<60: broadcast({"type":"alert","content":c,"score":analysis["score"],"time":now})

RULES = {
    "missing_info":{"keywords":["缺","颗","位置","多久","情况"],"min_match":2,"label":"未了解缺牙情况","desc":"应先问缺几颗/位置/多久"},
    "free_checkup":{"keywords":["免费","检查","CBCT","拍片","free","check-up"],"min_match":2,"label":"遗漏免费检查","desc":"应主动提免费CBCT+面诊"},
    "invite":{"keywords":["预约","过来","方便","安排","appointment","come"],"min_match":1,"label":"未主动邀约","desc":"应主动邀约到店"},
    "price_comp":{"keywords":["原价","市场","RM","3980","promotion","优惠","便宜"],"min_match":1,"label":"未强调价格优势","desc":"报价应先对比市场价"},
    "negative":{"keywords":["不关","没办法","随便","你自己看","就这样"],"min_match":1,"is_negative":True,"label":"语气不当","desc":"避免不耐烦语气"},
    "over_promise":{"keywords":["100%","保证","一定成功","绝对没"],"min_match":1,"is_negative":True,"label":"过度承诺","desc":"避免绝对化承诺"},
    "low_barrier":{"keywords":["检查了再决定","不一定要做","先来看看","free","no pressure"],"min_match":1,"label":"未降心理门槛","desc":"应说'先检查，不做也没关系'"},
    "empathy":{"keywords":["理解","完全","我明白","我懂","担心","顾虑"],"min_match":1,"label":"缺少共情","desc":"客户有顾虑应先共情"},
}
def analyze_message(text):
    results = []; tl = text.lower()
    for n,r in RULES.items():
        cnt = sum(1 for kw in r["keywords"] if kw in text or kw in tl)
        if r.get("is_negative"):
            if cnt >= r["min_match"]: results.append({"rule":n,"label":r["label"],"detail":r["desc"],"severity":"high"})
        else:
            if cnt < r["min_match"]: results.append({"rule":n,"label":r["label"],"detail":r["desc"],"severity":"low"})
    issues = [x for x in results if x["severity"]!="low"]; warns = [x for x in results if x["severity"]=="low"]
    return {"score":max(0,min(100,100-len(issues)*15-len(warns)*5)),"issues":issues,"warnings":warns}

# Routes
@app.route("/status")
def status():
    d = load_data(); ss = d.get("all_scores",[])
    return jsonify({"server":"Finger QC 🖐️","active_sessions":sum(1 for s in d["sessions"].values() if s["messages"]),"total_messages":d.get("total_messages",0),"avg_score":round(sum(ss)/len(ss),1) if ss else 0})

@app.route("/sessions")
def list_sessions():
    d = load_data(); res = {}
    for p,s in d["sessions"].items():
        if not s["messages"]: continue
        sc = [h["score"] for h in s["score_history"]]
        msgs = []
        emp_name = s.get("employee","")
        for m in s["messages"]:
            msgs.append({"t":m["type"], "c":m["content"][:300], "cn":m.get("cn",""), "tm": (lambda t: re.search(r'T(\d{2}:\d{2})', t).group(1) if isinstance(t,str) and re.search(r'T(\d{2}:\d{2})', t) else t[-5:] if isinstance(t,str) and len(t)>=5 else '')(m.get("time","")), "e":emp_name if m["type"]=="emp" else ""})
        res[p[-8:]] = {"n":s.get("contact_name",""),"p":p[-4:],"e":emp_name,"s":s["stage"],"c":len(s["messages"]),"as":round(sum(sc)/len(sc),1) if sc else 0,"msgs":msgs,"la":s.get("last_activity","")}
    return jsonify(res)

@app.route("/translate", methods=["POST"])
def translate_api():
    d = request.get_json(silent=True) or {}; return jsonify({"cn":translate_text(d.get("text",""))})

@app.route("/sse-stream")
def sse_stream():
    def gen():
        q = queue.Queue()
        with sse_lock: sse_clients.append(q)
        try:
            with recent_lock:
                for m in recent_msgs[-20:]: yield f"data: {json.dumps(m, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'type':'ready'}, ensure_ascii=False)}\n\n"
            while True:
                try: yield q.get(timeout=30)
                except queue.Empty: yield ": heartbeat\n\n"
        except GeneratorExit:
            with sse_lock:
                if q in sse_clients: sse_clients.remove(q)
    return Response(gen(), mimetype="text/event-stream", headers={"Cache-Control":"no-cache","Access-Control-Allow-Origin":"*"})

LIVE_HTML = """<!DOCTYPE html>
<html lang=zh-CN><head><meta charset=UTF-8><meta name=viewport content="width=device-width,initial-scale=1.0">
<title>Finger 实时看板</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,system-ui,sans-serif;background:#0f172a;color:#e2e8f0;height:100vh;display:flex;flex-direction:column}
.header{background:#1e293b;padding:12px 20px;border-bottom:2px solid #334155;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}
.header h1{font-size:17px;color:#38bdf8}
.header .stats{display:flex;gap:14px;font-size:12px;color:#94a3b8}
.header .stats b{color:#38bdf8}
.main{display:flex;flex:1;overflow:hidden}
.feed-col{flex:1;display:flex;flex-direction:column;overflow:hidden}
#feed{flex:1;overflow-y:auto;padding:10px 14px;display:flex;flex-direction:column;gap:5px}
.msg{padding:8px 12px;border-radius:8px;max-width:92%;animation:fadeIn .2s}
.msg.in{background:#1e3a5f;align-self:flex-start;border-left:3px solid #38bdf8}
.msg.out{background:#1a2e1a;align-self:flex-end;border-right:3px solid #4ade80}
.msg .meta{font-size:10px;color:#64748b;margin-bottom:2px;display:flex;gap:6px}
.msg .text{font-size:13px;line-height:1.5}
.msg .score{display:inline-block;font-size:10px;padding:1px 6px;border-radius:6px;margin-top:3px}
.s-good{background:#166534;color:#4ade80}.s-ok{background:#854d0e;color:#facc15}.s-bad{background:#7f1d1d;color:#fca5a5}
.msg .issues{font-size:10px;color:#fca5a5;margin-top:2px}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
.suggest-col{width:340px;background:#1e293b;border-left:2px solid #334155;display:flex;flex-direction:column;overflow:hidden}
.sug-hd{padding:12px 14px;border-bottom:1px solid #334155;font-size:13px;font-weight:bold;color:#38bdf8}
#sugFeed{flex:1;overflow-y:auto;padding:10px 14px}
.sug-card{background:#0f172a;border:1px solid #334155;border-radius:8px;padding:12px;margin-bottom:10px}
.sug-card .num{font-size:10px;color:#64748b;margin-bottom:4px}
.sug-card .txt{font-size:12px;line-height:1.6;color:#e2e8f0}
.sug-empty{text-align:center;color:#475569;font-size:12px;padding:30px 14px;line-height:2}
::-webkit-scrollbar{width:10px}::-webkit-scrollbar-track{background:#1e293b}::-webkit-scrollbar-thumb{background:#475569;border-radius:5px}::-webkit-scrollbar-thumb:hover{background:#60a5fa}
.test-btn{position:fixed;bottom:24px;right:24px;background:linear-gradient(135deg,#7c3aed,#6366f1);border:none;border-radius:50px;color:#fff;padding:14px 24px;font-size:15px;font-weight:700;cursor:pointer;z-index:1000;box-shadow:0 4px 16px rgba(124,58,237,.4);transition:all .2s;display:flex;align-items:center;gap:6px}
.test-btn:hover{transform:translateY(-2px);box-shadow:0 6px 24px rgba(124,58,237,.6)}
.test-modal{display:none;position:fixed;top:0;left:0;width:100%;height:100%;z-index:999;background:rgba(0,0,0,.6)}
.test-modal.show{display:flex;align-items:center;justify-content:center}
.test-panel{background:#1e293b;border:1px solid #334155;border-radius:16px;width:600px;max-width:90vw;max-height:85vh;overflow-y:auto;padding:24px;box-shadow:0 20px 60px rgba(0,0,0,.5)}
.test-panel h2{font-size:18px;color:#e2e8f0;margin-bottom:16px;display:flex;align-items:center;gap:8px;justify-content:space-between}
.test-panel h2 span{font-size:13px;color:#64748b;font-weight:400}
.test-panel .close-btn{background:transparent;border:1px solid #334155;border-radius:8px;color:#94a3b8;padding:6px 12px;cursor:pointer;font-size:13px}
.test-panel .close-btn:hover{background:#334155;color:#e2e8f0}
.test-input{width:100%;background:#0f172a;border:1px solid #334155;border-radius:10px;color:#e2e8f0;padding:14px;font-size:15px;font-family:-apple-system,system-ui,sans-serif;outline:none;transition:border-color .2s;min-height:80px;resize:vertical;line-height:1.6;margin-bottom:12px}
.test-input:focus{border-color:#7c3aed}
.test-gen-btn{background:linear-gradient(135deg,#7c3aed,#6366f1);border:none;border-radius:10px;color:#fff;padding:14px 0;font-size:16px;font-weight:700;cursor:pointer;width:100%;transition:all .2s}
.test-gen-btn:hover{opacity:.9}
.test-gen-btn:disabled{background:#334155;color:#64748b;cursor:not-allowed}
.test-result{margin-top:16px;display:none}
.test-result.show{display:block}
.test-result .result-card{background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px;margin-bottom:10px;animation:fadeIn .3s}
.test-result .result-card .num{font-size:11px;color:#7c3aed;font-weight:600;margin-bottom:4px}
.test-result .result-card .txt{font-size:14px;line-height:1.6;color:#e2e8f0;white-space:pre-wrap}
.test-result .loading{text-align:center;padding:20px;color:#64748b}
</style></head><body>
<div class=header><h1>Finger 实时看板</h1>
<div class=stats><span>消息 <b id=msgC>0</b></span><span>客户 <b id=custC>0</b></span><span>均分 <b id=avgS>-</b></span></div>
<div style=font-size:12px><a href='/records'+location.search style=color:#94a3b8;text-decoration:none;margin-right:10px>📋 记录</a><a href='/training'+location.search style=color:#94a3b8;text-decoration:none>📚 培训</a></div></div>
<div class=main>
<div class=feed-col><div id=feed></div></div>
<div class=suggest-col><div class=sug-hd>💡 建议回复</div><div id=sugFeed><div class=sug-empty>等待客户消息...<br>将根据客户问题给出建议</div></div></div>
</div>
<script>
const feed=document.getElementById('feed'), sf=document.getElementById('sugFeed'), mc=document.getElementById('msgC'), cc=document.getElementById('custC'), av=document.getElementById('avgS');
let c=0, cs=new Set(), lastCust='';
const convs={};
const SHOW=5;

function renderBlock(name){
  var msgs=convs[name];
  if(!msgs||!msgs.length)return;
  var el=document.getElementById('b-'+esc(name));
  if(!el){
    el=document.createElement('div');el.id='b-'+esc(name);
    el.style.cssText='margin-bottom:12px;background:#1e293b;border-radius:10px;padding:10px;border:1px solid #334155';
    feed.appendChild(el);
  }
  var show=msgs.slice(-SHOW);
  var h='<div style="font-size:11px;color:#38bdf8;font-weight:600;padding-bottom:6px;border-bottom:1px solid #334155;margin-bottom:6px">' + esc(name) + '</div>';
  show.forEach(function(m){
    var side=m.tp=='c'?'in':'out';
    var who=m.tp=='c'?esc(name):esc(m.emp||'员工');
    var ext='';
    if(m.tp=='e'&&m.sc!=null){
      var cl=m.sc>=80?'s-good':(m.sc>=60?'s-ok':'s-bad');
      ext+='<span class="score '+cl+'">'+m.sc+'/100</span>';
    }
    if(m.tp=='c'&&m.cn){
      ext+='<div style="color:#94a3b8;font-size:11px;margin-top:2px;border-top:1px solid #334155;padding-top:2px">🌐 ' + m.cn + '</div>';
    }
    if(m.issues&&m.issues.length){
      ext+='<div class=issues>'+m.issues.map(function(i){return '!'+i.label}).join(' . ')+'</div>';
    }
    h+='<div class="msg '+side+'"><div class=meta>'+(m.tm||'')+' <b>'+who+'</b></div><div class=text>'+esc(m.txt)+'</div>'+ext+'</div>';
  });
  el.innerHTML=h;
  feed.scrollTop=feed.scrollHeight;
}

// Load existing data from both sessions + training
fetch('/sessions').then(function(r){return r.json()}).then(function(sdata){
  var hasData=false;
  for(var phone in sdata){
    var s=sdata[phone];
    if(!s.msgs||!s.msgs.length)continue;
    hasData=true;
    var nm=s.n||phone.slice(-4);
    lastCust=nm;
    cs.add(nm); cc.textContent=cs.size;
    if(!convs[nm])convs[nm]=[];
    s.msgs.forEach(function(m){
      if(m.t=='customer'){
        convs[nm].push({tp:'c',txt:m.c,tm:m.tm||'',cn:m.cn||'',issues:[]});
        c++; mc.textContent=c;
      }else if(m.t=='emp'){
        convs[nm].push({tp:'e',txt:m.c,tm:m.tm||'',emp:s.e||'员工',sc:75,cn:'',issues:[]});
        c++; mc.textContent=c;
      }
    });
    renderBlock(nm);
  }
  av.textContent=hasData?'-':'等待消息...';
  if(!hasData){
    // Fallback: show training questions as demo
    fetch('/api/training').then(function(r2){return r2.json()}).then(function(td){
      var tps=(td.training_pairs||[]).slice(-10);
      tps.forEach(function(tp){
        c++; mc.textContent=c;
        var nm='培训: '+tp.id;
        cs.add(nm); cc.textContent=cs.size;
        if(!convs[nm])convs[nm]=[];
        convs[nm].push({tp:'c',txt:tp.customer_question,tm:'',cn:'',issues:[]});
        var ans=tp.answers||[];
        if(ans.length&&ans[0].text)convs[nm].push({tp:'e',txt:ans[0].text.slice(0,100),tm:'',emp:tp.source||'员工',sc:75,cn:'',issues:[]});
        renderBlock(nm);
      });
      if(!tps.length)feed.innerHTML='<div style="text-align:center;color:#475569;font-size:13px;padding:50px 20px;line-height:2">暂无数据<br>等待Webhook新消息流入...</div>';
    });
  }
});

// SSE realtime
const es=new EventSource('/sse-stream');
es.onmessage=function(e){
  const d=JSON.parse(e.data);
  if(d.type=='ready')return;
  c++; mc.textContent=c;
  if(d.type=='customer'){
    var nm=d.name||d.phone||'客户';
    lastCust=nm;
    cs.add(nm); cc.textContent=cs.size;
    if(!convs[nm])convs[nm]=[];
    convs[nm].push({tp:'c',txt:d.content,tm:d.time||'',cn:d.cn||'',issues:[]});
    if(convs[nm].length>SHOW*2)convs[nm].splice(0,convs[nm].length-SHOW);
    renderBlock(nm);
    if(d.suggestions&&d.suggestions.length){
      document.querySelector('.sug-hd').textContent='💡 建议回复';
      d.suggestions.forEach(function(s,i){var card=document.createElement('div');card.className='sug-card';card.innerHTML='<div class=num>' + (i+1) + ' · ' + nm + '</div><div class=txt>'+esc(s).replace(/\\n/g,'<br>')+'</div>';sf.appendChild(card)});
      sf.scrollTop=0;
    }
  }else if(d.type=='employee'){
    var nm=lastCust||d.employee||'员工';
    if(!convs[nm])convs[nm]=[];
    var sc=d.score||0;av.textContent=sc+'%';
    convs[nm].push({tp:'e',txt:d.content,tm:d.time||'',emp:d.employee||'',sc:sc,cn:d.cn||'',issues:d.issues||[]});
    if(convs[nm].length>SHOW*2)convs[nm].splice(0,convs[nm].length-SHOW);
    renderBlock(nm);
  }else if(d.type=='alert'){
    var div=document.createElement('div');
    div.style.cssText='background:#3b0a0a;align-self:center;border:2px solid #ef4444;text-align:center;padding:8px;margin:8px 0';
    div.innerHTML='<b style=color:#fca5a5>!' + esc(d.content) + '</b></div>';
    feed.appendChild(div); feed.scrollTop=feed.scrollHeight;
  }
};
function esc(t){var d=document.createElement('div');d.textContent=t;return d.innerHTML}
</script></body></html>"""

@app.route("/live")
def live(): return LIVE_HTML, 200, {"Content-Type":"text/html; charset=utf-8"}


RECORDS_HTML = """<!DOCTYPE html>
<html lang=zh-CN><head><meta charset=UTF-8><meta name=viewport content="width=device-width,initial-scale=1.0">
<title>Finger 沟通记录</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,system-ui,sans-serif;background:#0f172a;color:#e2e8f0;height:100vh;display:flex;flex-direction:column}
.header{background:#1e293b;padding:14px 20px;border-bottom:2px solid #334155;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px}
.header h1{font-size:17px;color:#38bdf8}
.header nav a{color:#94a3b8;font-size:12px;margin-left:12px;text-decoration:none}
.header nav a:hover{color:#38bdf8}
.main{display:flex;flex:1;overflow:hidden}
.sidebar{width:240px;background:#1e293b;border-right:2px solid #334155;display:flex;flex-direction:column;overflow:hidden}
.sidebar .hd{padding:12px 14px;border-bottom:1px solid #334155;font-size:12px;color:#94a3b8}
#empList{flex:1;overflow-y:auto;padding:6px}
.emp-item{padding:8px 10px;border-radius:6px;cursor:pointer;font-size:12px;color:#94a3b8;display:flex;justify-content:space-between}
.emp-item:hover{background:#334155;color:#e2e8f0}
.emp-item.active{background:#1e3a5f;color:#38bdf8}
.emp-item .badge{background:#334155;border-radius:8px;padding:1px 6px;font-size:10px}
.content{flex:1;display:flex;flex-direction:column;overflow:hidden}
#custList{overflow-y:auto;padding:6px;flex:1}
.cust-card{background:#0f172a;border:1px solid #334155;border-radius:8px;padding:12px;margin-bottom:8px;cursor:pointer}
.cust-card:hover{border-color:#38bdf8}
.cust-card .name{font-size:13px;color:#e2e8f0;font-weight:500}
.cust-card .meta{font-size:11px;color:#64748b;margin-top:4px}
.cust-card .meta span{margin-right:12px}
.cust-card .preview{font-size:11px;color:#475569;margin-top:6px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.cust-detail{display:none;flex:1;flex-direction:column;overflow:hidden}
.cust-detail.show{display:flex}
.cust-detail .hd{padding:12px 16px;border-bottom:1px solid #334155;font-size:13px;display:flex;justify-content:space-between;align-items:center}
.cust-detail .hd .back{color:#38bdf8;cursor:pointer;font-size:12px}
#convFeed{padding:12px 16px;overflow-y:auto;flex:1}
.conv-msg{margin-bottom:10px;max-width:80%}
.conv-msg.in{text-align:left}
.conv-msg.out{text-align:right;margin-left:auto}
.conv-msg .bubble{display:inline-block;padding:8px 12px;border-radius:8px;font-size:13px;line-height:1.5;text-align:left}
.conv-msg.in .bubble{background:#1e3a5f;border-left:3px solid #38bdf8}
.conv-msg.out .bubble{background:#1a2e1a;border-right:3px solid #4ade80}
.conv-msg .tm{font-size:10px;color:#64748b;margin-top:2px}
.conv-msg .score{font-size:10px;color:#facc15;margin-top:1px}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
::-webkit-scrollbar{width:10px}::-webkit-scrollbar-track{background:#1e293b}::-webkit-scrollbar-thumb{background:#475569;border-radius:5px}::-webkit-scrollbar-thumb:hover{background:#60a5fa}
.test-btn{position:fixed;bottom:24px;right:24px;background:linear-gradient(135deg,#7c3aed,#6366f1);border:none;border-radius:50px;color:#fff;padding:14px 24px;font-size:15px;font-weight:700;cursor:pointer;z-index:1000;box-shadow:0 4px 16px rgba(124,58,237,.4);transition:all .2s;display:flex;align-items:center;gap:6px}
.test-btn:hover{transform:translateY(-2px);box-shadow:0 6px 24px rgba(124,58,237,.6)}
.test-modal{display:none;position:fixed;top:0;left:0;width:100%;height:100%;z-index:999;background:rgba(0,0,0,.6)}
.test-modal.show{display:flex;align-items:center;justify-content:center}
.test-panel{background:#1e293b;border:1px solid #334155;border-radius:16px;width:600px;max-width:90vw;max-height:85vh;overflow-y:auto;padding:24px;box-shadow:0 20px 60px rgba(0,0,0,.5)}
.test-panel h2{font-size:18px;color:#e2e8f0;margin-bottom:16px;display:flex;align-items:center;gap:8px;justify-content:space-between}
.test-panel h2 span{font-size:13px;color:#64748b;font-weight:400}
.test-panel .close-btn{background:transparent;border:1px solid #334155;border-radius:8px;color:#94a3b8;padding:6px 12px;cursor:pointer;font-size:13px}
.test-panel .close-btn:hover{background:#334155;color:#e2e8f0}
.test-input{width:100%;background:#0f172a;border:1px solid #334155;border-radius:10px;color:#e2e8f0;padding:14px;font-size:15px;font-family:-apple-system,system-ui,sans-serif;outline:none;transition:border-color .2s;min-height:80px;resize:vertical;line-height:1.6;margin-bottom:12px}
.test-input:focus{border-color:#7c3aed}
.test-gen-btn{background:linear-gradient(135deg,#7c3aed,#6366f1);border:none;border-radius:10px;color:#fff;padding:14px 0;font-size:16px;font-weight:700;cursor:pointer;width:100%;transition:all .2s}
.test-gen-btn:hover{opacity:.9}
.test-gen-btn:disabled{background:#334155;color:#64748b;cursor:not-allowed}
.test-result{margin-top:16px;display:none}
.test-result.show{display:block}
.test-result .result-card{background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px;margin-bottom:10px;animation:fadeIn .3s}
.test-result .result-card .num{font-size:11px;color:#7c3aed;font-weight:600;margin-bottom:4px}
.test-result .result-card .txt{font-size:14px;line-height:1.6;color:#e2e8f0;white-space:pre-wrap}
.test-result .loading{text-align:center;padding:20px;color:#64748b}
.no-data{text-align:center;color:#475569;font-size:13px;padding:60px 20px;line-height:2}
</style></head><body>
<div class=header><h1>📋 沟通记录</h1><nav>
<a href=/live>实时看板</a>
<a href=/records>沟通记录</a>
<a href=/training>话术培训</a>
<a href=/status>状态</a>
</nav></div>
<div class=main>
<div class=sidebar><div class=hd>按员工筛选</div><div id=empList></div></div>
<div class=content><div id=custList></div>
<div class=cust-detail id=custDetail><div class=hd><span id=detailTitle></span><span class=back id=backBtn>← 返回</span></div><div id=convFeed></div></div>
</div></div>
<script>
var data=null, curEmp=null, curCust=null;
function load(){fetch('/sessions').then(function(r){return r.json()}).then(function(d){data=d;render()})}
function render(){
  // Build employee list
  var emps={};
  for(var k in data){
    var s=data[k];
    // Try to get employee name from session data - store emp mapping
    var emp=s.emp||'未分配';
    if(!emps[emp])emps[emp]={count:0,custs:[]};
    emps[emp].count++;
  }
  // Also get employee names from session store
  // We need employee-per-session, so let's rebuild
  // Actually let's fetch sessions more completely
  refreshDisplay();
}

function refreshDisplay(){
  fetch('/sessions').then(function(r){return r.json()}).then(function(d){
    data=d;
    // Group sessions by employee
    var emps={}, allCusts=[];
    for(var k in d){
      var s=d[k];
      var empName='未分配';
      // We need to find employee name from session - check the messages
      for(var mi=0;mi<(s.msgs||[]).length;mi++){
        var m=s.msgs[mi];
        if(m.emp&&m.emp!='员工'&&m.emp!='🤖 Bot'){empName=m.emp;break}
      }
      // Also check from the messages content
      var allMsgs=s.msgs||[];
      var msgs=[];
      for(var mi=0;mi<allMsgs.length;mi++){
        var mm=allMsgs[mi];
        msgs.push({type:mm.t||mm.type,content:mm.c||mm.content,score:mm.sc||mm.score,emp:mm.e||mm.employee||'',time:mm.tm||mm.time});
      }
      var custKey=k;
      if(!emps[empName])emps[empName]={sessions:{}};
      emps[empName].sessions[custKey]={name:s.n||s.name||k.slice(-4),phone:s.p||s.phone||k,msgs:msgs,score:s.as||s.avg_score||0,cnt:msgs.length,last:s.la||s.last_activity||''};
      allCusts.push({emp:empName,key:custKey,data:emps[empName].sessions[custKey]});
    }
    
    // Render employee list
    var el=document.getElementById('empList');el.innerHTML='';
    var all=document.createElement('div');all.className='emp-item'+(curEmp==null?' active':'');
    all.innerHTML='<span>全部员工</span><span class=badge>'+allCusts.length+'</span>';
    all.onclick=function(){curEmp=null;curCust=null;document.getElementById('custDetail').classList.remove('show');refreshDisplay()};
    el.appendChild(all);
    for(var e in emps){
      var cnt=Object.keys(emps[e].sessions).length;
      var item=document.createElement('div');item.className='emp-item'+(curEmp==e?' active':'');
      item.innerHTML='<span>'+e+'</span><span class=badge>'+cnt+'</span>';
      item.onclick=function(emp){return function(){curEmp=emp;curCust=null;document.getElementById('custDetail').classList.remove('show');refreshDisplay()}}(e);
      el.appendChild(item);
    }
    
    // Render customer list
    var cl=document.getElementById('custList');cl.innerHTML='';
    var filtered=curEmp?allCusts.filter(function(x){return x.emp==curEmp}):allCusts;
    if(filtered.length==0){cl.innerHTML='<div class=no-data>暂无沟通记录</div>';return}
    filtered.sort(function(a,b){return(b.data.last||'').localeCompare(a.data.last||'')});
    for(var i=0;i<filtered.length;i++){
      var x=filtered[i],s=x.data;
      var card=document.createElement('div');card.className='cust-card';
      var lastMsg=s.msgs.length?esc(s.msgs[s.msgs.length-1].content||'').slice(0,60):'';
      var lastTime=s.last?s.last.slice(11,16):'';
      card.innerHTML='<div class=name>'+esc(s.name)+'</div><div class=meta><span>\U0001f4ac '+s.cnt+'条</span><span>\U0001f4ca '+s.score+'分</span><span>'+lastTime+'</span></div><div class=preview>'+lastMsg+'</div>';
      card.onclick=function(key,data){return function(){showDetail(key,data)}}(x.key,s);
      cl.appendChild(card);
    }
  });
}

function showDetail(key,s){
  curCust=key;
  document.getElementById('custList').style.display='none';
  var dt=document.getElementById('custDetail');dt.classList.add('show');
  document.getElementById('detailTitle').innerHTML=esc(s.name)+' <span style=font-size:11px;color:#64748b;font-weight:normal>'+s.phone.slice(-4)+' · '+s.cnt+'条消息 · 平均'+s.score+'分</span>';
  var feed=document.getElementById('convFeed');feed.innerHTML='';
  for(var i=0;i<s.msgs.length;i++){
    var m=s.msgs[i];
    var div=document.createElement('div');div.className='conv-msg '+(m.type=='in'||m.type=='customer'?'in':'out');
    var b='<div class=bubble>'+esc(m.content)+'</div>';
    b+='<div class=tm>'+(m.time||'')+(m.emp?' · '+esc(m.emp):'')+'</div>';
    if(m.score)b+='<div class=score>评分 '+m.score+'</div>';
    div.innerHTML=b;
    feed.appendChild(div);
  }
  feed.scrollTop=feed.scrollHeight;
}

document.getElementById('backBtn').onclick=function(){
  document.getElementById('custDetail').classList.remove('show');
  document.getElementById('custList').style.display='block';
  curCust=null;
};

function esc(t){if(!t)return '';var d=document.createElement('div');d.textContent=t;return d.innerHTML}

refreshDisplay();
setInterval(refreshDisplay,10000);
</script></body></html>"""


@app.route("/records")
def records(): return RECORDS_HTML, 200, {"Content-Type":"text/html; charset=utf-8"}


# ====== 话术培训系统 ======
TRAINING_FILE = os.path.join(BASE_DIR, "training_data.json")

def load_training_data():
    if os.path.exists(TRAINING_FILE):
        try:
            return json.load(open(TRAINING_FILE, "r", encoding="utf-8"))
        except:
            pass
    return {"version": "1.0", "training_pairs": [], "last_updated": bj_now().isoformat()}

def save_training_data(d):
    d["last_updated"] = bj_now().isoformat()
    json.dump(d, open(TRAINING_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)


TRAINING_HTML = """<!DOCTYPE html>
<html lang=zh-CN><head><meta charset=UTF-8><meta name=viewport content="width=device-width,initial-scale=1.0">
<title>Finger 话术培训</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,system-ui,sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh;padding:20px;display:flex;flex-direction:column}
.header{display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;flex-wrap:wrap;gap:12px}
.header h1{font-size:22px;font-weight:700;background:linear-gradient(135deg,#60a5fa,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.header nav a{color:#94a3b8;font-size:12px;margin-left:12px;text-decoration:none}
.header nav a:hover{color:#38bdf8}
.stats{display:flex;gap:12px;flex-wrap:wrap}
.stat-card{background:#1e293b;border-radius:10px;padding:10px 18px;text-align:center;border:1px solid #334155}
.stat-card .num{font-size:26px;font-weight:700;color:#60a5fa}
.stat-card .label{font-size:11px;color:#94a3b8;margin-top:2px}
.last-upd{font-size:11px;color:#64748b;margin-bottom:16px}
.content{display:flex;gap:20px;flex:1;overflow:hidden;height:calc(100vh - 100px)}
.board-col{flex:1;overflow-y:auto;padding-right:400px;height:100%}
.board{display:flex;flex-direction:column;gap:14px;padding-bottom:60px}
.board-col::-webkit-scrollbar{width:10px}
.card{background:#1e293b;border-radius:12px;padding:20px;border:1px solid #334155;transition:border-color .2s;cursor:pointer}
.card:hover{border-color:#475569}
.card.active{border-color:#60a5fa}
.card-hd{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;gap:12px;flex-wrap:wrap}
.q-id{font-size:11px;color:#64748b;font-family:monospace;background:#0f172a;padding:2px 8px;border-radius:4px}
.q-text{font-size:14px;line-height:1.6;color:#f1f5f9;padding:12px;background:#0f172a;border-radius:8px;border-left:3px solid #60a5fa;margin-bottom:12px}
.q-source{font-size:11px;color:#64748b}
.answers{display:flex;flex-direction:column;gap:8px}
.answer{background:#0f172a;border-radius:8px;padding:10px 14px;border-left:3px solid #334155;font-size:13px;line-height:1.5;color:#cbd5e1}
.answer.best{border-left-color:#22c55e}
.answer-label{font-size:10px;font-weight:600;color:#64748b;margin-bottom:4px;text-transform:uppercase;letter-spacing:.5px}
.answer.best .answer-label{color:#22c55e}
.empty-state{text-align:center;padding:60px 20px;color:#64748b}
.empty-state .icon{font-size:48px;margin-bottom:12px}
.empty-state h3{font-size:18px;color:#94a3b8;margin-bottom:8px}
.empty-state p{font-size:14px;line-height:1.6}
.status-badge{display:inline-block;font-size:11px;padding:2px 10px;border-radius:10px;font-weight:500;margin-left:8px}
.status-pending{background:#f59e0b20;color:#f59e0b;border:1px solid #f59e0b40}
.status-trained{background:#22c55e20;color:#22c55e;border:1px solid #22c55e40}
.form-panel{width:380px;display:flex;flex-direction:column;gap:14px;position:fixed;top:80px;right:20px;max-height:calc(100vh - 100px);overflow-y:auto;z-index:100}
.form-panel h3{font-size:15px;color:#e2e8f0;padding-bottom:8px;border-bottom:1px solid #334155}
.form-panel .q-preview{font-size:13px;color:#e2e8f0;background:#0f172a;padding:14px;border-radius:8px;line-height:1.6;max-height:200px;overflow-y:auto;border-left:3px solid #60a5fa;word-break:break-word}
.form-panel label{font-size:13px;color:#94a3b8;font-weight:600;padding-top:4px}
.form-panel textarea{width:100%;background:#0f172a;border:1px solid #334155;border-radius:8px;color:#e2e8f0;padding:12px;font-size:14px;font-family:-apple-system,system-ui,sans-serif;resize:vertical;min-height:70px;outline:none;transition:border-color .2s;line-height:1.5}
.form-panel textarea:focus{border-color:#60a5fa}
.form-panel .btn{background:#2563eb;border:none;border-radius:10px;color:#fff;padding:16px 24px;font-size:16px;font-weight:700;cursor:pointer;transition:background .2s;width:100%;letter-spacing:0.5px}.form-panel .btn-del{background:#7f1d1d;border:none;border-radius:10px;color:#fca5a5;padding:16px 24px;font-size:16px;font-weight:600;cursor:pointer;transition:background .2s;width:100%}.form-panel .btn-del:hover{background:#991b1b}.del-btn{cursor:pointer;font-size:14px;padding:2px 6px;border-radius:4px;opacity:0.6}.del-btn:hover{opacity:1;background:#7f1d1d40}
.form-panel .btn:hover{background:#1d4ed8}
.form-panel .btn:disabled{background:#334155;color:#64748b;cursor:not-allowed}
.form-panel .msg{font-size:11px;padding:8px 12px;border-radius:6px;display:none}
.msg-ok{background:#166534;color:#4ade80;border:1px solid #22c55e40}
.msg-err{background:#7f1d1d;color:#fca5a5;border:1px solid #ef444440}
::-webkit-scrollbar{width:10px}::-webkit-scrollbar-track{background:#1e293b}::-webkit-scrollbar-thumb{background:#475569;border-radius:5px}::-webkit-scrollbar-thumb:hover{background:#60a5fa}
.test-btn{position:fixed;bottom:24px;right:24px;background:linear-gradient(135deg,#7c3aed,#6366f1);border:none;border-radius:50px;color:#fff;padding:14px 24px;font-size:15px;font-weight:700;cursor:pointer;z-index:1000;box-shadow:0 4px 16px rgba(124,58,237,.4);transition:all .2s;display:flex;align-items:center;gap:6px}
.test-btn:hover{transform:translateY(-2px);box-shadow:0 6px 24px rgba(124,58,237,.6)}
.test-modal{display:none;position:fixed;top:0;left:0;width:100%;height:100%;z-index:999;background:rgba(0,0,0,.6)}
.test-modal.show{display:flex;align-items:center;justify-content:center}
.test-panel{background:#1e293b;border:1px solid #334155;border-radius:16px;width:600px;max-width:90vw;max-height:85vh;overflow-y:auto;padding:24px;box-shadow:0 20px 60px rgba(0,0,0,.5)}
.test-panel h2{font-size:18px;color:#e2e8f0;margin-bottom:16px;display:flex;align-items:center;gap:8px;justify-content:space-between}
.test-panel h2 span{font-size:13px;color:#64748b;font-weight:400}
.test-panel .close-btn{background:transparent;border:1px solid #334155;border-radius:8px;color:#94a3b8;padding:6px 12px;cursor:pointer;font-size:13px}
.test-panel .close-btn:hover{background:#334155;color:#e2e8f0}
.test-input{width:100%;background:#0f172a;border:1px solid #334155;border-radius:10px;color:#e2e8f0;padding:14px;font-size:15px;font-family:-apple-system,system-ui,sans-serif;outline:none;transition:border-color .2s;min-height:80px;resize:vertical;line-height:1.6;margin-bottom:12px}
.test-input:focus{border-color:#7c3aed}
.test-gen-btn{background:linear-gradient(135deg,#7c3aed,#6366f1);border:none;border-radius:10px;color:#fff;padding:14px 0;font-size:16px;font-weight:700;cursor:pointer;width:100%;transition:all .2s}
.test-gen-btn:hover{opacity:.9}
.test-gen-btn:disabled{background:#334155;color:#64748b;cursor:not-allowed}
.test-result{margin-top:16px;display:none}
.test-result.show{display:block}
.test-result .result-card{background:#0f172a;border:1px solid #334155;border-radius:10px;padding:14px;margin-bottom:10px;animation:fadeIn .3s}
.test-result .result-card .num{font-size:11px;color:#7c3aed;font-weight:600;margin-bottom:4px}
.test-result .result-card .txt{font-size:14px;line-height:1.6;color:#e2e8f0;white-space:pre-wrap}
.test-result .loading{text-align:center;padding:20px;color:#64748b}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
.card{animation:fadeIn .3s}
</style></head><body>
<div class=header>
<h1>📋 话术培训看板</h1>
<nav>
<a href='/live'+location.search>实时看板</a>
<a href='/records'+location.search>沟通记录</a>
<a href='/training'+location.search>话术培训</a>
</nav>
</div>
<div style=display:flex;gap:16px;flex-wrap:wrap;margin-bottom:16px>
<div class=stat-card><div class=num id=totalQ>0</div><div class=label>问题总数</div></div>
<div class=stat-card><div class=num id=trainedQ>0</div><div class=label>已培训</div></div>
<div class=stat-card><div class=num id=pendingQ>0</div><div class=label>待培训</div></div>
<button class=stat-card style=cursor:pointer onclick=loadData()><div style=color:#94a3b8;font-size:13px>🔄 刷新</div></button>
</div>
<div class=last-upd id=lastUpdated>加载中...</div>
<div class=content>
<div class=board-col>
<div class=board id=board>
<div class=empty-state><div class=icon>📝</div><h3>暂无培训记录</h3><p>遇到客户问题时，Finger 会记录下来<br>员工原话 + Finger建议(3条) + 待补充 = 共5条话术</p></div>
</div>
</div>
<div class=form-panel id=formPanel>
<h3>✍️ 提交话术培训</h3>
<div style=font-size:11px;color:#64748b>点击左侧待培训问题开始编辑</div>
<div class=q-preview id=qPreview style=display:none></div>
<div id=answerFields style=display:none>
<label>话术 ① 👤 员工原话</label>
<textarea id=a1 placeholder="员工的真实回复..."></textarea>
<label>话术 ② 🖐️ Finger建议</label>
<textarea id=a2 placeholder="Finger推荐话术..."></textarea>
<label>话术 ③ 🖐️ Finger建议</label>
<textarea id=a3 placeholder="Finger推荐话术..."></textarea>
<label>话术 ④ 🖐️ Finger建议</label>
<textarea id=a4 placeholder="Finger推荐话术..."></textarea>
<label>话术 ⑤ 📝 待补充</label>
<textarea id=a5 placeholder="培训师补充话术..."></textarea>
<div style=display:flex;gap:8px><button class=btn id=btnSubmit onclick=submitTraining() style=flex:1>💾 提交培训</button><button class=btn-del id=btnDelete onclick=deleteTraining() style=flex:0.4>🗑️ 删除</button></div>
<div class=msg id=formMsg></div>
</div>
</div>
</div>
<script>
function qp(){return location.search}
var allData=[],selectedId=null;
async function loadData(){
  try{
    var resp=await fetch('/api/training'+qp());
    if(!resp.ok)throw Error('fail');
    var data=await resp.json();
    allData=data.training_pairs||[];
    render(data);
  }catch(e){
    document.getElementById('board').innerHTML='<div class=empty-state><div class=icon>📝</div><h3>暂无培训记录</h3></div>';
    document.getElementById('lastUpdated').textContent='等待第一条记录...';
  }
}
function render(data){
  var pairs=data.training_pairs||[];
  var total=pairs.length,trained=pairs.filter(function(p){return p.status==='trained'}).length,pending=total-trained;
  document.getElementById('totalQ').textContent=total;
  document.getElementById('trainedQ').textContent=trained;
  document.getElementById('pendingQ').textContent=pending;
  document.getElementById('lastUpdated').textContent='🕐 最后更新: '+(data.last_updated||'未知');
  var board=document.getElementById('board');
  if(total===0){
    board.innerHTML='<div class=empty-state><div class=icon>📝</div><h3>暂无培训记录</h3></div>';
    return;
  }
  var sorted=[...pairs].reverse();
  board.innerHTML=sorted.map(function(p){
    var isTrained=p.status==='trained',ans=p.answers||[],best=p.best_answer_index,active=selectedId===p.id?' active':'';
    return '<div class=card'+active+' onclick=selectQuestion("'+esc(p.id)+'")><div class=card-hd><div><span class=q-id>#'+esc(p.id)+'</span><span class="status-badge '+(isTrained?'status-trained':'status-pending')+'">'+(isTrained?'✅ 已培训':'⏳ 待培训')+'</span></div><div style=display:flex;align-items:center;gap:8px><span class=q-source>'+esc(p.source||'未知')+'</span><span class=del-btn onclick="event.stopPropagation();deleteQuestion(\\''+esc(p.id)+'\\')" title="删除此问题">🗑️</span></div></div><div class=q-text>'+esc(p.customer_question)+'</div>'+(ans.length?'<div class=answers>'+ans.map(function(a,i){return '<div class="answer'+(best===i?' best':'')+'"><div class=answer-label>'+(best===i?'⭐ ':'')+'话术 '+(i+1)+(best===i?' (最优)':'')+'</div>'+esc(a.text)+'</div>'}).join('')+'</div>':'<div style=color:#64748b;font-size:13px;font-style:italic>点击此卡片开始培训...</div>')+'</div>';
  }).join('');
  if(selectedId)highlightQuestion(selectedId);
}
function selectQuestion(id){
  selectedId=id;
  var q=null;
  for(var p of allData){if(p.id===id){q=p;break}}
  if(!q)return;
  var cards=document.querySelectorAll('.card');
  cards.forEach(function(c){c.classList.remove('active')});
  setTimeout(function(){
    var target=document.querySelector('[onclick*="'+id+'"]');
    if(target)target.classList.add('active');
  },10);
  document.getElementById('formPanel').querySelector('h3').textContent='✍️ 培训: #'+q.id;
  var pre=document.getElementById('qPreview');
  pre.textContent=q.customer_question;
  pre.style.display='block';
  document.getElementById('answerFields').style.display='block';
  if(q.answers&&q.answers.length){
    document.getElementById('a1').value=q.answers[0]?q.answers[0].text:'';
    document.getElementById('a2').value=q.answers[1]?q.answers[1].text:'';
    document.getElementById('a3').value=q.answers[2]?q.answers[2].text:'';
    document.getElementById('a4').value=q.answers[3]?q.answers[3].text:'';
    document.getElementById('a5').value=q.answers[4]?q.answers[4].text:'';
  }else{
    document.getElementById('a1').value='';
    document.getElementById('a2').value='';
    document.getElementById('a3').value='';
    document.getElementById('a4').value='';
    document.getElementById('a5').value='';
  }
  document.getElementById('formMsg').style.display='none';
}
function highlightQuestion(id){
  var cards=document.querySelectorAll('.card');
  cards.forEach(function(c){c.classList.remove('active')});
  var target=document.querySelector('[onclick*="'+id+'"]');
  if(target)target.classList.add('active');
}
async function deleteQuestion(id){if(!confirm('确定删除此问题？'))return;try{var r=await fetch('/api/training/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:id})});if(!r.ok)throw Error('删除失败');loadData();if(selectedId===id){selectedId=null;document.getElementById('answerFields').style.display='none';document.getElementById('qPreview').style.display='none'}}catch(e){alert('删除失败: '+e.message)}}

async function submitTraining(){
  if(!selectedId)return;
  var a1=document.getElementById('a1').value.trim();
  var a2=document.getElementById('a2').value.trim();
  var a3=document.getElementById('a3').value.trim();
  var a4=document.getElementById('a4').value.trim();
  var a5=document.getElementById('a5').value.trim();
  if(!a1&&!a2&&!a3&&!a4&&!a5){showMsg('请至少填写一条话术','err');return}
  var btn=document.getElementById('btnSubmit');
  btn.disabled=true;btn.textContent='⏳ 提交中...';
  try{
    var resp=await fetch('/api/training/train'+qp(),{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({id:selectedId,answers:[a1,a2,a3,a4,a5].filter(function(a){return a}),best_index:0})
    });
    if(!resp.ok)throw Error('提交失败');
    showMsg('✅ 培训完成！问题 #'+selectedId+' 已标记为已培训','ok');
    loadData();
  }catch(e){
    showMsg('❌ 提交失败: '+e.message,'err');
  }finally{
    btn.disabled=false;btn.textContent='💾 提交培训';
  }
}
function showMsg(t,type){
  var m=document.getElementById('formMsg');
  m.textContent=t;m.className='msg '+(type==='ok'?'msg-ok':'msg-err');m.style.display='block';
}
function esc(t){var d=document.createElement('div');d.textContent=t||'';return d.innerHTML}
loadData();
setInterval(loadData,10000);
</script>
<div id="testBtn" class="test-btn" onclick="openTest()">🧪 测试话术</div>
<div id="testModal" class="test-modal" onclick="if(event.target===this)closeTest()">
<div class="test-panel">
<h2>🧪 话术测试 <span>输入客户问题，验证培训效果</span><button class="close-btn" onclick="closeTest()">✕ 关闭</button></h2>
<textarea class="test-input" id="testInput" placeholder="例如：种一颗牙多少钱？&#10;智齿痛怎么办？&#10;Harga berapa?&#10;..."></textarea>
<button class="test-gen-btn" id="testGenBtn" onclick="generateTest()">🚀 生成建议话术</button>
<div class="test-result" id="testResult"></div>
</div>
</div>
<script>
function openTest(){document.getElementById('testModal').classList.add('show');document.getElementById('testResult').innerHTML='';document.getElementById('testResult').classList.remove('show')}
function closeTest(){document.getElementById('testModal').classList.remove('show')}
async function generateTest(){
  var input=document.getElementById('testInput').value.trim();
  if(!input){alert('请输入客户问题');return}
  var btn=document.getElementById('testGenBtn');btn.disabled=true;btn.textContent='⏳ 生成中...';
  var result=document.getElementById('testResult');result.classList.add('show');
  result.innerHTML='<div class=loading>🧠 Finger 正在思考话术...</div>';
  try{
    var resp=await fetch('/api/training/generate',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text:input})});
    var data=await resp.json();
    if(!data.ok)throw new Error(data.error||'生成失败');
    var scripts=data.scripts||[];
    result.innerHTML='<div style="font-size:13px;color:#94a3b8;margin-bottom:10px">💡 基于培训知识库生成的话术建议：</div>'+
      scripts.map(function(s,i){return '<div class=result-card><div class=num>话术 '+(i+1)+'</div><div class=txt>'+esc(s).replace(/\\n/g,'<br>')+'</div></div>'}).join('');
  }catch(e){
    result.innerHTML='<div class=result-card style="border-color:#7f1d1d"><div class=num style="color:#fca5a5">❌ 生成失败</div><div class=txt style="color:#fca5a5">'+esc(e.message)+'</div></div>';
  }finally{btn.disabled=false;btn.textContent='🚀 生成建议话术'}
}
</script>
</body></html>"""


@app.route("/training")
def training():
    return TRAINING_HTML, 200, {"Content-Type":"text/html; charset=utf-8"}


@app.route("/api/training")
def training_api():
    return jsonify(load_training_data())


@app.route("/api/training/add", methods=["POST"])
def training_add():
    d = request.get_json(silent=True) or {}
    q = d.get("question", "").strip()
    if not q:
        return jsonify({"error": "question required"}), 400
    data = load_training_data()
    nid = f"q_{len(data['training_pairs']) + 1:03d}"
    data["training_pairs"].append({
        "id": nid,
        "customer_question": q,
        "source": d.get("source", "web"),
        "status": "pending",
        "answers": [],
        "best_answer_index": None,
        "created_at": bj_now().isoformat(),
        "notes": d.get("notes", "")
    })
    save_training_data(data)
    return jsonify({"ok": True, "id": nid})


@app.route("/api/training/train", methods=["POST"])
def training_train():
    d = request.get_json(silent=True) or {}
    qid = d.get("id", "")
    answers = d.get("answers", [])
    best_idx = d.get("best_index", None)
    if not qid:
        return jsonify({"error": "id required"}), 400
    data = load_training_data()
    found = None
    for p in data["training_pairs"]:
        if p["id"] == qid:
            found = p
            break
    if not found:
        return jsonify({"error": "not found"}), 404
    found["answers"] = [{"text": a} for a in answers[:5]]
    found["best_answer_index"] = best_idx if best_idx is not None else (0 if answers else None)
    found["status"] = "trained"
    save_training_data(data)
    return jsonify({"ok": True})


@app.route("/api/training/delete", methods=["POST"])
def training_delete():
    d = request.get_json(silent=True) or {}
    qid = d.get("id", "")
    if not qid:
        return jsonify({"error": "id required"}), 400
    data = load_training_data()
    before = len(data["training_pairs"])
    data["training_pairs"] = [p for p in data["training_pairs"] if p["id"] != qid]
    after = len(data["training_pairs"])
    if before == after:
        return jsonify({"error": "not found"}), 404
    save_training_data(data)
    return jsonify({"ok": True, "deleted": qid})


@app.route("/api/training/generate", methods=["POST"])
def training_generate():
    """生成测试话术"""
    d = request.get_json(silent=True) or {}
    text = d.get("text", "").strip()
    if not text:
        return jsonify({"ok": False, "error": "请输入客户问题"})
    from random import sample
    # Use the existing generate_training_scripts
    scripts = generate_training_scripts(text)
    if not scripts:
        scripts = ["建议来院免费检查，医生面诊后给出方案", "我们可以先安排免费CBCT拍片看看情况", "方便的话预约时间来了解详情"]
    return jsonify({"ok": True, "scripts": scripts})


if __name__ == "__main__":
    rp = os.environ.get("PORT", str(PORT))
    print(f"🖐️ Finger QC | 端口 {rp}")
    app.run(host="0.0.0.0" if os.environ.get("RENDER") else "127.0.0.1", port=int(rp), debug=False, threaded=True)

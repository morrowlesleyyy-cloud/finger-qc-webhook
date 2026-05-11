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
def identify_employee(text, phone=""):
    if phone and phone in _emp_cache: return _emp_cache[phone]
    t = text.lower().strip()
    name = '员工'
    m = re.search(r'(?:我.?是|i.?m|i am|this is|name is)\s*(?:医生助理|种植牙客服|美芽|me(?:ya)?.?dental.?的)?\s*(mia|andy|cici|jestina|sophia|小枫|may|john|fong)', t, re.IGNORECASE)
    if m: name = m.group(1).capitalize()
    elif "i'm john" in t or "john, consultation" in t: name = 'John'
    elif "i'm may" in t: name = 'May'
    elif 'jestina' in t: name = 'Jestina'
    elif 'sophia' in t: name = 'Sophia'
    elif '小枫' in t: name = '小枫'
    elif re.search(r'😉|🥰|🤗', t): name = 'Mia'
    elif re.search(r'consultation assistant|dental technology', t): name = 'John'
    elif '我是 meya dental 的专业' in t or '免费为你提供' in t: name = '🤖 Bot'
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
    save_session(p,session,data)
    tag = n or p[-4:]
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
    session["messages"].append({"type":"emp","content":c,"time":bj_now().isoformat()})
    session["score_history"].append({"score":analysis["score"],"time":bj_now().isoformat()})
    data["all_scores"].append(analysis["score"])
    data["total_messages"] = data.get("total_messages",0)+1
    save_session(p,session,data)
    tag = session.get("contact_name","") or p[-4:]
    emp = identify_employee(c, p)
    if emp not in ('员工','🤖 Bot'):
        session["employee"] = emp
        save_session(p,session,data)  # 重新保存以更新员工名字
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
            msgs.append({"t":m["type"], "c":m["content"][:300], "tm":m.get("time","")[-8:-3] if isinstance(m.get("time"),str) and len(m.get("time",""))>8 else "", "e":emp_name if m["type"]=="emp" else ""})
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
::-webkit-scrollbar{width:4px}::-webkit-scrollbar-track{background:#1e293b}::-webkit-scrollbar-thumb{background:#334155;border-radius:2px}
</style></head><body>
<div class=header><h1>Finger 实时看板</h1>
<div class=stats><span>消息 <b id=msgC>0</b></span><span>客户 <b id=custC>0</b></span><span>均分 <b id=avgS>-</b></span></div></div>
<div class=main>
<div class=feed-col><div id=feed></div></div>
<div class=suggest-col><div class=sug-hd>💡 建议回复</div><div id=sugFeed><div class=sug-empty>等待客户消息...<br>将根据客户问题给出建议</div></div></div>
</div>
<script>
const feed=document.getElementById('feed'), sf=document.getElementById('sugFeed'), mc=document.getElementById('msgC'), cc=document.getElementById('custC'), av=document.getElementById('avgS');
let c=0, cs=new Set();
const es=new EventSource('/sse-stream');
es.onmessage=function(e){
  const d=JSON.parse(e.data);
  if(d.type=='ready')return;
  c++; mc.textContent=c;
  const div=document.createElement('div');
  if(d.type=='customer'){
    div.className='msg in';
    cs.add(d.name||d.phone); cc.textContent=cs.size;
    let h='<div class=meta>'+(d.time||'')+' <b>'+(d.name||'客户')+'</b>'+(d.source?' '+d.source:'')+'</div><div class=text>'+esc(d.content)+'</div>';
    if(d.cn)h+='<div style="color:#94a3b8;font-size:11px;margin-top:3px;border-top:1px solid #334155;padding-top:3px">🌐 '+esc(d.cn)+'</div>';
    div.innerHTML=h; feed.appendChild(div); feed.scrollTop=feed.scrollHeight;
    if(d.suggestions&&d.suggestions.length){
    // 保留历史建议，追加新建议
      d.suggestions.forEach(function(s,i){var card=document.createElement('div');card.className='sug-card';card.innerHTML='<div class=num>💡 建议'+(i+1)+'</div><div class=txt>'+esc(s).replace(/\\n/g,'<br>')+'</div>';sf.appendChild(card)});
      sf.scrollTop=0;
    }
  }else if(d.type=='employee'){
    div.className='msg out';
    var sc=d.score||0,cl=sc>=80?'s-good':(sc>=60?'s-ok':'s-bad');av.textContent=sc+'%';
    var is=d.issues&&d.issues.length?'<div class=issues>'+d.issues.map(function(i){return '⚠'+i.label}).join(' · ')+'</div>':'';
    var eh='<div class=meta>'+(d.time||'')+' <b>'+(d.employee||'员工')+'</b></div><div class=text>'+esc(d.content)+'</div><span class="score '+cl+'">'+sc+'/100</span>'+is;
    if(d.cn)eh+='<div style="color:#94a3b8;font-size:11px;margin-top:3px;border-top:1px solid #334155;padding-top:3px">🌐 '+esc(d.cn)+'</div>';
    div.innerHTML=eh; feed.appendChild(div); feed.scrollTop=feed.scrollHeight;
  }else if(d.type=='alert'){
    div.style.cssText='background:#3b0a0a;align-self:center;border:2px solid #ef4444;text-align:center;padding:8px';
    div.innerHTML='<b style=color:#fca5a5>🚨 告警</b><div class=text>'+esc(d.content)+'</div><div style=color:#fca5a5;font-size:11px>'+(d.time||'')+' '+d.score+'/100</div>';
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
::-webkit-scrollbar{width:4px}::-webkit-scrollbar-track{background:#1e293b}::-webkit-scrollbar-thumb{background:#334155;border-radius:2px}
.no-data{text-align:center;color:#475569;font-size:13px;padding:60px 20px;line-height:2}
</style></head><body>
<div class=header><h1>📋 沟通记录</h1><nav>
<a href=/live>实时看板</a>
<a href=/records>沟通记录</a>
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


if __name__ == "__main__":
    rp = os.environ.get("PORT", str(PORT))
    print(f"🖐️ Finger QC | 端口 {rp}")
    app.run(host="0.0.0.0" if os.environ.get("RENDER") else "127.0.0.1", port=int(rp), debug=False, threaded=True)

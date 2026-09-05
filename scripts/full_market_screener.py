#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""喵喵波段选股系统 v3.6 — 全市场扫描器（v3.3 E1放宽：底部/零上金叉均认可；v3.4 新增B+鱼头启动：60分钟级别确认；v3.5 B+明细增加60分钟E5放量阳线+红柱状态标注；v3.6 新增喵喵六联上车信号：L0前置MA60不逆势 + T领先CCI触底拐头 + R1~R5共振确认(KDJ低位金叉/SAR转红/站上中轨/MACD金叉/放量)，加成进score排序）

【新框架：基本面定资格 → 技术面定阶段；个股与ETF彻底分池】
  第一层 账户权限：开户满3年 + 资产10万以内 → 无创业板(10万门槛)/科创板(50万门槛)权限
         → 只扫沪深主板 00/60 开头（排除30创业板/68科创板/8·4北交所）
  第二层 行业聚焦：仅科技（电子/计算机/通信/传媒）+ 有色金属 两个板块
  第三层 硬性预筛：00/60开头 · 价格≤20元 · 非ST · 当日成交额≥8000万 → 按成交额取头部
  第四层 基本面资格赛（v3.0 前置，仅个股）：三维度评分（盈利质量/估值水平/市值规模，满分30）
         → 只有 A/B 档且无排雷红旗的个股才有入池资格；C档或红旗直接淘汰（名单留档可查）
  第五层 技术面阶段分类（v3.0 核心输出）：上车临近(B) / 底部(A) / 趋势(C) / 顶部(D) / 冷却(E)
  ETF 板块：独立候选池（16只行业ETF），不做基本面筛选，单独技术面阶段分类

数据源：腾讯行情接口（web.ifzq.gtimg.cn K线 + proxy.finance.qq.com 板块成分 + qt.gtimg.cn 基本面快照）
输出：控制台排名 + HTML报告（screen_full_YYYYMMDD.html）+ 候选池JSON（pool_YYYYMMDD.json，
      结构：{date, criteria, stocks:[...], etfs:[...], rejected:{...}}，供波段作战台 v4 导入）
用法：python full_market_screener.py
"""
import urllib.request, json, ssl, statistics, sys, io, os, datetime, time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
except Exception:
    pass

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
           'Referer': 'https://gu.qq.com/'}

# ===== 用户选股标准（账户权限边界）=====
# 开户满3年（2023年开户，2020注册制改革后新账户，无豁免）+ 资产10万以内（本金7.5万，
# 达不到创业板10万日均资产门槛，更达不到科创板50万）→ 账户等级只能买：沪深主板 00/60 开头
PRICE_MAX = 20.0            # 个股价格上限（元）
CODE_PREFIX_OK = ('00', '60')  # 只扫深主板(00)/沪主板(60)——账户等级可买范围
PRE_AMT_MIN = 8000e4        # 预筛：当日成交额下限 8000万
CAP_TECH = 100              # 科技行业取成交额头部数量
CAP_METAL = 60              # 有色行业取成交额头部数量
FUND_QUALIFIED = ('A', 'B') # 基本面入池资格：A/B档 且无红旗（C档与红旗一律淘汰）
REC_TOTAL_TARGET = 20      # 收盘版今日推荐总数目标（trend+breakout+picks 三组严格去重，不足时由 B/A 阶段强标的补足）
REC_MIDDAY_TOTAL = 10      # 午间快照版（--midday，11:40 上午收盘后跑）今日推荐总数目标
REC_MIDDAY_PICKS = 1       # 午间快照版精选固定 1 只（精选=两组之外最强信号；午间不用 B/A 硬凑数量）

# ===== 行业板块（腾讯申万一级）=====
BOARDS = [
    ('pt01801080', '电子', '科技'),
    ('pt01801750', '计算机', '科技'),
    ('pt01801770', '通信', '科技'),
    ('pt01801760', '传媒', '科技'),
    ('pt01801050', '有色金属', '有色'),
]

# ===== ETF候选池 =====
ETF_UNIVERSE = [
    ('sh588710', '588710', 'UC科创半导体设备ETF'),
    ('sz159381', '159381', '创业板人工智能ETF华夏'),
    ('sz159819', '159819', '人工智能ETF易方达'),
    ('sh518880', '518880', '华安黄金ETF'),
    ('sh512480', '512480', '半导体ETF国联安'),
    ('sh512760', '512760', '芯片ETF华夏'),
    ('sh588000', '588000', '科创50ETF华夏'),
    ('sz159915', '159915', '创业板ETF易方达'),
    ('sh510300', '510300', '沪深300ETF华泰柏瑞'),
    ('sh512690', '512690', '酒ETF鹏华'),
    ('sh515050', '515050', '5G通信ETF'),
    ('sz159611', '159611', '电力ETF广发'),
    ('sh512880', '512880', '证券ETF国泰'),
    ('sh510880', '510880', '红利ETF华泰柏瑞'),
    ('sh512010', '512010', '医药ETF易方达'),
    ('sh516160', '516160', '新能源车ETF'),
]

# ===== 准入闸门阈值（个股与ETF分档）=====
GATE_AMT_ETF = 3000e4       # ETF 20日均成交额 ≥ 3000万
GATE_AMT_STOCK = 2e8        # 个股 20日均成交额 ≥ 2亿
GATE_SWING_MIN = 0.35       # 250日区间振幅 ≥ 35%
GATE_VOL_LO, GATE_VOL_HI = 0.010, 0.055

# ===== 基本面考察阈值（v2.2 新增，仅个股；ETF 无意义跳过）=====
# 三维度评分（满分30）：盈利质量(隐含ROE=PB/PE) + 估值水平(PE) + 市值规模
# 档位：A≥24 优质 / B 14~23 合格 / C<14 排雷警示
# 红旗：亏损(PE<0) / 隐含ROE<3% / 总市值<50亿 / PB>15


def _get_json(url, tries=5, backoff=1.2):
    last_err = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=20, context=ctx) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except Exception as ex:
            last_err = ex
            time.sleep(backoff * (attempt + 1))
    raise last_err


def fetch_board_members(board_code, board_name):
    """拉取行业板块全部成分股（腾讯排行接口，turnover单位万元）"""
    members = []
    offset = 0
    while True:
        url = ("https://proxy.finance.qq.com/cgi/cgi-bin/rank/hs/getBoardRankList?"
               f"board_code={board_code}&sort_type=Price&direct=down&offset={offset}&count=200")
        data = _get_json(url)
        rl = (data.get('data') or {}).get('rank_list') or []
        if not rl:
            break
        for d in rl:
            try:
                members.append({
                    'symbol': d.get('code', ''),          # 如 sz002371
                    'code': d.get('code', '')[2:],        # 如 002371
                    'name': d.get('name', ''),
                    'price': float(d.get('zxj', 0) or 0),
                    'amount': float(d.get('turnover', 0) or 0) * 1e4,  # 万元→元
                    'industry': board_name,
                })
            except (TypeError, ValueError):
                continue
        if len(rl) < 200:
            break
        offset += 200
    return members


def prefilter(members, sector):
    """硬性预筛：前缀 / 价格 / ST / 成交额"""
    out = []
    for s in members:
        code, name = s['code'], s['name']
        if not code.startswith(CODE_PREFIX_OK):
            continue
        if s['price'] > PRICE_MAX or s['price'] <= 0:
            continue
        if 'ST' in name.upper() or '退' in name:
            continue
        if s['amount'] < PRE_AMT_MIN:
            continue
        s['sector'] = sector
        out.append(s)
    out.sort(key=lambda x: -x['amount'])
    return out


def _get_text(url, tries=4, backoff=1.2):
    last_err = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=20, context=ctx) as resp:
                return resp.read().decode('gbk', errors='replace')
        except Exception as ex:
            last_err = ex
            time.sleep(backoff * (attempt + 1))
    raise last_err


def fetch_fundamentals(symbols):
    """腾讯批量行情接口拉基本面：PE(TTM口径近似)/PB/总市值(亿)。
    隐含ROE = PB/PE×100（数学恒等：PB/PE = E/B）。symbols 形如 ['sh600000', ...]"""
    out = {}
    BATCH = 50
    for i in range(0, len(symbols), BATCH):
        batch = symbols[i:i + BATCH]
        url = 'https://qt.gtimg.cn/q=' + ','.join(batch)
        try:
            text = _get_text(url)
        except Exception:
            time.sleep(1.5)
            try:
                text = _get_text(url)
            except Exception:
                continue
        for line in text.strip().split(';'):
            line = line.strip()
            if not line or '~' not in line:
                continue
            f = line.split('~')
            if len(f) < 47:
                continue
            try:
                sym = f[0].split('=')[0].replace('v_', '').strip()
                if not sym:
                    continue
                pe = float(f[39]) if f[39] not in ('', '-') else None
                mcap = float(f[45]) if f[45] not in ('', '-') else None
                pb = float(f[46]) if f[46] not in ('', '-') else None
            except (ValueError, IndexError):
                continue
            out[sym] = {'pe': pe, 'pb': pb, 'mcap': mcap}
        time.sleep(0.5)
    return out


def grade_fundamentals(f):
    """基本面三维度评分（满分30）+ 档位 + 排雷红旗"""
    if not f:
        return None
    pe, pb, mcap = f.get('pe'), f.get('pb'), f.get('mcap')
    # 隐含 ROE = PB / PE（PE>0 且 PB>0 时才有意义）
    roe = round(pb / pe * 100, 1) if (pe and pe > 0 and pb and pb > 0) else None

    # 1) 盈利质量（10分）
    if roe is None:
        s1 = 0
    elif roe >= 10:
        s1 = 10
    elif roe >= 5:
        s1 = 7
    elif roe >= 3:
        s1 = 4
    elif roe > 0:
        s1 = 1
    else:
        s1 = 0

    # 2) 估值水平 PE（10分）：科技成长容忍较高PE
    if pe is None or pe <= 0:
        s2 = 0
    elif pe <= 30:
        s2 = 10
    elif pe <= 60:
        s2 = 7
    elif pe <= 100:
        s2 = 4
    else:
        s2 = 0

    # 3) 市值规模（10分）：防微市值壳股流动性/操纵风险
    if mcap is None:
        s3 = 0
    elif mcap >= 300:
        s3 = 10
    elif mcap >= 150:
        s3 = 8
    elif mcap >= 80:
        s3 = 6
    elif mcap >= 50:
        s3 = 4
    else:
        s3 = 1

    fscore = s1 + s2 + s3
    fgrade = 'A' if fscore >= 24 else ('B' if fscore >= 14 else 'C')

    flags = []
    if pe is not None and pe <= 0:
        flags.append('亏损')
    if roe is not None and roe < 3:
        flags.append('ROE<3%')
    if mcap is not None and mcap < 50:
        flags.append('微市值<50亿')
    if pb is not None and pb > 15:
        flags.append('PB>15')

    return {'pe': pe, 'pb': pb, 'mcap': mcap, 'roe': roe,
            'fscore': fscore, 'fgrade': fgrade, 'flags': '、'.join(flags)}


def fetch_kline(symbol, n_bars=320):
    """腾讯日线K线，返回 bars；vol 单位：个股=手，ETF=份"""
    url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?"
           f"param={symbol},day,,,{n_bars},qfq")
    data = _get_json(url)
    d = data.get('data', {}).get(symbol, {})
    rows = d.get('qfqday') or d.get('day') or []
    bars = []
    for r in rows:
        # [date, open, close, high, low, volume]
        bars.append({'date': r[0], 'open': float(r[1]), 'close': float(r[2]),
                     'high': float(r[3]), 'low': float(r[4]), 'vol': float(r[5])})
    return bars


def fetch_m60(symbol, n_bars=320):
    """腾讯60分钟K线（mkline 接口，不复权；短窗指标判定足够）
    返回 bars；date 形如 202608281500"""
    url = (f"https://ifzq.gtimg.cn/appstock/app/kline/mkline?"
           f"param={symbol},m60,,{n_bars}")
    data = _get_json(url)
    d = data.get('data', {}).get(symbol, {})
    rows = d.get('m60') or []
    bars = []
    for r in rows:
        bars.append({'date': r[0], 'open': float(r[1]), 'close': float(r[2]),
                     'high': float(r[3]), 'low': float(r[4]), 'vol': float(r[5])})
    return bars


def ema(values, period):
    k = 2 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def calc_macd(closes):
    dif = [f - s for f, s in zip(ema(closes, 12), ema(closes, 26))]
    dea = ema(dif, 9)
    hist = [(d - e) * 2 for d, e in zip(dif, dea)]
    return dif, dea, hist


def calc_kdj(bars, n=9):
    k_list, d_list, j_list = [], [], []
    k, d = 50.0, 50.0
    for i in range(len(bars)):
        w = bars[max(0, i - n + 1):i + 1]
        llv, hhv = min(b['low'] for b in w), max(b['high'] for b in w)
        rsv = (bars[i]['close'] - llv) / (hhv - llv) * 100 if hhv != llv else 50
        k = 2 / 3 * k + 1 / 3 * rsv
        d = 2 / 3 * d + 1 / 3 * k
        k_list.append(k); d_list.append(d); j_list.append(3 * k - 2 * d)
    return k_list, d_list, j_list


def calc_cci(bars, n=14):
    """CCI(14)：典型价格 TP=(H+L+C)/3，CCI=(TP-SMA)/(0.015×MD)。
    CCI≤-100 视为超卖区；向上拐头 = 超卖后首次回升（领先信号）"""
    tps = [(b['high'] + b['low'] + b['close']) / 3 for b in bars]
    out = []
    for i in range(len(bars)):
        w = tps[max(0, i - n + 1):i + 1]
        sma = sum(w) / len(w)
        md = sum(abs(t - sma) for t in w) / len(w)
        out.append((tps[i] - sma) / (0.015 * md) if md else 0.0)
    return out


def calc_sar(bars, step=0.02, max_step=0.2):
    """抛物线SAR（Wilder）：返回 (sar_list, up_list)；up=True 表示多头（红点，价格在SAR上方）。
    step=0.02 加速因子，每次创新高/新低递增0.02，上限0.2"""
    n = len(bars)
    sar_l, up_l = [None] * n, [None] * n
    if n < 3:
        return sar_l, up_l
    up = bars[1]['close'] >= bars[0]['close']
    ep = bars[0]['high'] if up else bars[0]['low']
    af = step
    for i in range(1, n):
        prev_sar = sar_l[i - 1] if sar_l[i - 1] is not None else bars[0]['close']
        sar_i = prev_sar + af * (ep - prev_sar)
        if up:
            prev_lo = bars[i - 1]['low'] if i == 1 else min(bars[i - 1]['low'], bars[i - 2]['low'])
            sar_i = min(sar_i, prev_lo)
            if bars[i]['low'] < sar_i:
                up = False
                sar_i = ep
                ep = bars[i]['low']
                af = step
            else:
                if bars[i]['high'] > ep:
                    ep = bars[i]['high']
                    af = min(af + step, max_step)
        else:
            prev_hi = bars[i - 1]['high'] if i == 1 else max(bars[i - 1]['high'], bars[i - 2]['high'])
            sar_i = max(sar_i, prev_hi)
            if bars[i]['high'] > sar_i:
                up = True
                sar_i = ep
                ep = bars[i]['high']
                af = step
            else:
                if bars[i]['low'] < ep:
                    ep = bars[i]['low']
                    af = min(af + step, max_step)
        sar_l[i] = sar_i
        up_l[i] = up
    return sar_l, up_l


def analyze(symbol, code, name, is_etf, sector=''):
    bars = fetch_kline(symbol)
    if len(bars) < 70:
        return None
    closes = [b['close'] for b in bars]
    vols = [b['vol'] for b in bars]
    n = len(bars)
    last, prev = bars[-1], bars[-2]

    # 成交额估算：个股 vol为手(×100股)，ETF vol为份
    unit = 1.0 if is_etf else 100.0
    amts = [v * unit * c for v, c in zip(vols, closes)]

    dif, dea, hist = calc_macd(closes)
    k_l, d_l, j_l = calc_kdj(bars)
    mid20 = sum(closes[-20:]) / 20
    std20 = statistics.stdev(closes[-20:])
    upper, lower = mid20 + 2 * std20, mid20 - 2 * std20
    ma10 = sum(closes[-10:]) / 10
    ma20 = mid20
    ma20_prev5 = sum(closes[-25:-5]) / 20

    vol_ma5 = sum(vols[-6:-1]) / 5
    amt20 = sum(amts[-20:]) / 20 if len(amts) >= 20 else sum(amts) / len(amts)

    gate_amt_min = GATE_AMT_ETF if is_etf else GATE_AMT_STOCK
    gate_amt = amt20 >= gate_amt_min
    seg = bars[-250:] if n >= 250 else bars
    rng = (max(b['high'] for b in seg) / min(b['low'] for b in seg)) - 1
    gate_swing = rng >= GATE_SWING_MIN
    avg_amp = sum((b['high'] - b['low']) / b['close'] for b in bars[-20:]) / 20
    gate_vol = GATE_VOL_LO <= avg_amp <= GATE_VOL_HI
    above_ma20 = last['close'] > ma20
    ma20_up = ma20 > ma20_prev5

    # ---- 上车5条（含数据依据，供作战台自动勾选）----
    e = {}
    ed = {}
    e1 = False
    e1_cross = []
    for i in range(n - 5, n):
        if i > 0 and dif[i] > dea[i] and dif[i - 1] <= dea[i - 1]:
            e1_cross.append((i, dif[i]))
            e1 = True   # v3.3 放宽：底部金叉与零上金叉均认可（原口径：仅金叉时 DIF 贴零轴才算）
    e['E1_MACD金叉'] = e1 and dif[-1] > dea[-1]
    if e1_cross:
        ed['E1_MACD金叉'] = '；'.join(
            f"{bars[i]['date'][5:]}金叉 DIF {dv:+.3f}（零轴{'下方·底部' if dv < 0 else '上方·中继'}，均认可）"
            for i, dv in e1_cross) + f"，当前 DIF {dif[-1]:+.3f} {'>' if dif[-1] > dea[-1] else '<'} DEA {dea[-1]:+.3f}"
    else:
        ed['E1_MACD金叉'] = f"近5日无金叉（DIF {dif[-1]:+.3f} vs DEA {dea[-1]:+.3f}）"
    e['E2_站上中轨'] = above_ma20
    ed['E2_站上中轨'] = f"收盘 {last['close']} vs MA20 {ma20:.3f}（中轨{'上行' if ma20_up else '走平/下行'}）"
    e3 = False
    e3_i = None
    for i in range(n - 5, n):
        if i > 0 and k_l[i] > d_l[i] and k_l[i - 1] <= d_l[i - 1] and k_l[i - 1] < 35:
            e3 = True
            e3_i = i
    e['E3_KDJ低位金叉'] = e3 and k_l[-1] > d_l[-1]
    if e3_i is not None:
        ed['E3_KDJ低位金叉'] = f"{bars[e3_i]['date'][5:]}金叉（前日K {k_l[e3_i - 1]:.0f}<35），当前 K {k_l[-1]:.0f} D {d_l[-1]:.0f} J {j_l[-1]:.0f}"
    else:
        ed['E3_KDJ低位金叉'] = (f"近5日无低位金叉，当前 K {k_l[-1]:.0f} D {d_l[-1]:.0f} J {j_l[-1]:.0f}"
                              + ('（K>D 但金叉超5日或非低位）' if k_l[-1] > d_l[-1] else ''))
    low20 = min(b['low'] for b in bars[-20:])
    low_prev40 = min(b['low'] for b in bars[-60:-20]) if n >= 60 else low20
    e['E4_higher_low'] = low20 > low_prev40
    ed['E4_higher_low'] = f"近20日低点 {low20} vs 前40日低点 {low_prev40}"
    e5 = False
    e5_i = None
    e5_ratio = 0
    for idx in range(n - 2, n):
        vma5 = sum(vols[max(0, idx - 5):idx]) / max(1, idx)
        if bars[idx]['close'] > bars[idx]['open'] and bars[idx]['vol'] > vma5:
            e5 = True
            e5_i = idx
            e5_ratio = bars[idx]['vol'] / vma5 if vma5 else 0
    e['E5_放量阳线'] = e5
    ed['E5_放量阳线'] = (f"{bars[e5_i]['date'][5:]}放量阳线（量比 {e5_ratio:.1f}）" if e5_i is not None else '近2日无放量阳线')

    # ---- 喵喵六联上车信号 v3.6（用户自研逻辑 · 优化落地版）----
    # 分层：L0 前置过滤（不逆势）→ T 领先触发（CCI超卖拐头=计时起点）
    #       → R1~R5 共振确认（10个交易日内全部成立，不强制先后）
    #   R1 KDJ 低位金叉（金叉前 K<35）  R2 SAR 转红且连续2根维持
    #   R3 收盘站上 BOLL 中轨（中轨走平/向上）  R4 MACD 金叉（近10日+当前DIF>DEA）
    #   R5 放量（近3日阳线且量 ≥ 前5日均量×1.2）
    # 输出：six_cnt(0~5) + six_grade（≥4强 / 5极强）；加成进 score → B 阶段排序置顶
    ma60 = sum(closes[-60:]) / 60 if n >= 60 else ma20
    ma60_prev5 = sum(closes[-65:-5]) / 60 if n >= 65 else ma60
    six = {}
    six['L0_MA60不逆势'] = ma60 >= ma60_prev5 * 0.995  # 走平（容忍0.5%下行）或向上
    cci = calc_cci(bars, 14)
    t_ok, t_i = False, None
    for i in range(max(1, n - 10), n):
        if cci[i] <= -100 and cci[i + 1] > cci[i]:
            t_ok, t_i = True, i
            break
    six['T_CCI触底拐头'] = t_ok
    r1, r1_i = False, None
    for i in range(max(1, n - 10), n):
        if k_l[i] > d_l[i] and k_l[i - 1] <= d_l[i - 1] and k_l[i - 1] < 35:
            r1, r1_i = True, i
            break
    six['R1_KDJ低位金叉'] = r1 and k_l[-1] > d_l[-1]
    sar_l, up_l = calc_sar(bars)
    r2, r2_i = False, None
    for i in range(max(1, n - 10), n):
        if up_l[i] and not up_l[i - 1]:
            r2, r2_i = True, i
            break
    six['R2_SAR转红维持'] = r2 and up_l[-1] and up_l[-2]
    six['R3_站上中轨'] = above_ma20 and ma20 >= ma20_prev5 * 0.995
    r4, r4_i = False, None
    for i in range(max(1, n - 10), n):
        if dif[i] > dea[i] and dif[i - 1] <= dea[i - 1]:
            r4, r4_i = True, i
            break
    six['R4_MACD金叉'] = r4 and dif[-1] > dea[-1]
    r5, r5_i, r5_ratio = False, None, 0
    for i in range(max(1, n - 3), n):
        vma5 = sum(vols[max(0, i - 5):i]) / 5
        if vma5 and bars[i]['close'] > bars[i]['open'] and bars[i]['vol'] >= vma5 * 1.2:
            r5, r5_i, r5_ratio = True, i, bars[i]['vol'] / vma5
            break
    six['R5_放量确认'] = r5
    six_cnt = sum(six[k] for k in ('R1_KDJ低位金叉', 'R2_SAR转红维持', 'R3_站上中轨',
                                   'R4_MACD金叉', 'R5_放量确认'))
    six_grade = '极强' if six_cnt == 5 else ('强' if six_cnt >= 4 else ('接近' if six_cnt == 3 else '不足'))
    six_detail = (f"前置{'✓' if six['L0_MA60不逆势'] else '✗'}MA60 {ma60:.3f}"
                  f"｜触发{'✓' if t_ok else '✗'}CCI {cci[-1]:.0f}"
                  f"｜" + ' '.join(
                      f"{k.split('_')[0]}{'✓' if six[k] else '✗'}"
                      for k in ('R1_KDJ低位金叉', 'R2_SAR转红维持', 'R3_站上中轨',
                                'R4_MACD金叉', 'R5_放量确认'))
                  + (f"（量比{r5_ratio:.1f}）" if r5_ratio else ''))
    entry_cnt = sum(e.values())
    six_bonus = six_cnt * 5  # 六联加成（0~25分）：B 阶段排序时命中数高的置顶
    score = entry_cnt * 20 + six_bonus

    # ---- 下车信号（含数据依据）----
    x = {}
    xd = {}
    x['X1_MACD高位死叉或红柱连缩'] = (
        (dif[-1] < dea[-1] and dif[-2] >= dea[-2] and max(dif[-3:]) > 0) or
        (hist[-1] > 0 and hist[-1] < hist[-2] < hist[-3] and hist[-3] < hist[-4] if n > 4 else False))
    xd['X1_MACD高位死叉或红柱连缩'] = f"DIF {dif[-1]:+.3f} DEA {dea[-1]:+.3f}，柱 {hist[-1]:+.3f}（前3柱 {hist[-2]:+.3f}/{hist[-3]:+.3f}/{hist[-4]:+.3f}）"
    x['X2_J值超买拐头'] = j_l[-2] > 90 and j_l[-1] < j_l[-2]
    xd['X2_J值超买拐头'] = f"J 前日 {j_l[-2]:.0f} → 今 {j_l[-1]:.0f}（{'超买后拐头' if j_l[-2] > 90 else '未超买'}）"
    body = abs(last['close'] - last['open'])
    upshadow = last['high'] - max(last['close'], last['open'])
    x['X3_放量长上影或吞没'] = (upshadow > body * 1.5 and last['vol'] > vol_ma5 * 1.2) or \
        (last['close'] < prev['open'] and last['open'] > prev['close'] and prev['close'] > prev['open'] and last['vol'] > vol_ma5)
    xd['X3_放量长上影或吞没'] = f"上影 {upshadow:.3f} vs 实体 {body:.3f}，量比 {last['vol'] / vol_ma5:.1f}" if vol_ma5 else '量能数据不足'
    x['X4_破MA10'] = last['close'] < ma10
    xd['X4_破MA10'] = f"收盘 {last['close']} vs MA10 {ma10:.3f}"
    hi20 = max(b['high'] for b in bars[-20:])
    x['X5_高点回撤过半'] = (hi20 - last['close']) / (hi20 - low20) > 0.5 if hi20 > low20 else False
    xd['X5_高点回撤过半'] = f"20日高 {hi20} 低 {low20}，现回撤 {(hi20 - last['close']) / (hi20 - low20) * 100:.0f}%" if hi20 > low20 else '区间无波动'

    # ---- v3.7 今日推荐辅助字段：放量突破20日新高（"启动：刚突破确认"判定）----
    hi20_prev = max(b['high'] for b in bars[-21:-1]) if n >= 21 else hi20
    breakout20 = bool(last['close'] > hi20_prev and last['vol'] > vol_ma5 * 1.2)
    breakout_note = (f"收盘 {last['close']} > 前20日高 {hi20_prev}（量比 {last['vol'] / vol_ma5:.1f}）"
                     if breakout20 else '')
    exit_cnt = sum(x.values())
    x_ma20_break = last['close'] < ma20

    # ---- 周期定位 ----
    if x_ma20_break:
        stage = 'E_下跌中继' if (not ma20_up or dif[-1] < dea[-1]) else 'A_底部观察区'
        if entry_cnt >= 3 and ma20_up:
            stage = 'A_底部观察区'
    elif exit_cnt >= 2 or (j_l[-2] > 90 and j_l[-1] < j_l[-2]):
        stage = 'D_顶部区'
    elif entry_cnt == 5:
        stage = 'C_趋势中'
    elif entry_cnt >= 3:
        stage = 'B_上车临近'
    else:
        stage = 'C_趋势中'

    stage_brief = stage.split('_')[0]

    # ---- B+ 鱼头启动：60分钟级别确认（仅日线 B 阶段标的做二次判定）----
    # 条件：①现价站上60分钟MA20（=5日均线近似）②MACD(12,26,9)金叉发生在近12根（3个交易日）内
    #       ③金叉时 DIF 贴零轴或下方（<现价×0.5%，鱼头而非鱼身）④金叉后未死叉（当前 DIF>DEA）
    # v3.5 辅助标注（不影响判定）：E5放量阳线（60分钟）· 红柱扩张/收窄
    m60_note = ''
    if stage == 'B_上车临近':
        try:
            b60 = fetch_m60(symbol)
            if len(b60) >= 80:
                c60 = [b['close'] for b in b60]
                d60, e60, h60 = calc_macd(c60)
                ma20_60 = sum(c60[-20:]) / 20
                cross60 = None
                for i in range(max(1, len(d60) - 12), len(d60)):
                    if d60[i] > e60[i] and d60[i - 1] <= e60[i - 1]:
                        cross60 = (i, d60[i])
                low_cross = bool(cross60) and cross60[1] < c60[-1] * 0.005
                if c60[-1] > ma20_60 and cross60 and d60[-1] > e60[-1] and low_cross:
                    pos = '零轴下方·底部' if cross60[1] < 0 else '贴零轴'
                    # ---- v3.5：60分钟辅助标注（不作硬性判定条件）----
                    # ① E5_60 放量阳线：金叉当根起至最新，阳线且量 ≥ 前20根均量×1.2
                    # ② 红柱状态：金叉后 MACD 红柱扩张=动能延续 / 收窄=可能刚点火就熄
                    vols60 = [b['vol'] for b in b60]
                    e5_60_i, e5_60_ratio = None, 0
                    for idx in range(cross60[0], len(b60)):
                        if idx < 20:
                            continue
                        vma60 = sum(vols60[idx - 20:idx]) / 20
                        if vma60 and b60[idx]['close'] > b60[idx]['open'] and b60[idx]['vol'] >= vma60 * 1.2:
                            e5_60_i, e5_60_ratio = idx, b60[idx]['vol'] / vma60
                            break
                    vol_note = (f"E5放量阳线✅ {b60[e5_60_i]['date'][4:6]}-{b60[e5_60_i]['date'][6:8]} "
                                f"{b60[e5_60_i]['date'][8:10]}:{b60[e5_60_i]['date'][10:12]} 量比{e5_60_ratio:.1f}"
                                if e5_60_i is not None else '⚠️缩量待确认（金叉后无放量阳线）')
                    hist_note = '红柱扩张✅' if h60[-1] > h60[-2] else '红柱收窄⚠️'
                    stage = 'B+_鱼头启动'
                    stage_brief = 'B+'
                    m60_note = (f"60分钟{b60[cross60[0]]['date'][4:6]}-{b60[cross60[0]]['date'][6:8]} "
                                f"{b60[cross60[0]]['date'][8:10]}:{b60[cross60[0]]['date'][10:12]}金叉 "
                                f"DIF {cross60[1]:+.3f}（{pos}），现价 {c60[-1]:.3f} > 60分钟MA20 {ma20_60:.3f}"
                                f"｜{vol_note}｜{hist_note}")
                else:
                    why = ('无新鲜金叉（近3个交易日）' if not cross60
                           else ('金叉位置偏高（鱼身非鱼头）' if not low_cross
                                 else '金叉后动能转弱'))
                    m60_note = f"60分钟未确认鱼头：{why}"
        except Exception:
            pass  # 60分钟数据异常不影响日线判定，保持 B

    actions = {
        'A': '观察池：不动作，等信号凑齐',
        'B': '⭐ 观察池置顶：每日核对',
        'B+': '🚀 鱼头启动：60分钟趋势已起，可贴60分钟MA20试探首仓（半仓内）',
        'C': '持有者拿住，未上车不追',
        'D': '执行下车清单',
        'E': '冷却池：禁止抄底',
    }

    return {
        'code': code, 'name': name, 'is_etf': is_etf, 'sector': sector,
        'date': last['date'],
        'close': last['close'], 'chg': (last['close'] / prev['close'] - 1) * 100,
        'gate_amt': gate_amt, 'gate_swing': gate_swing, 'gate_vol': gate_vol,
        'amt20': amt20, 'rng': rng * 100, 'avg_amp': avg_amp * 100,
        'stage': stage, 'stage_brief': stage_brief, 'score': score,
        'entry_cnt': entry_cnt, 'entry': e, 'entry_detail': ed, 'exit_cnt': exit_cnt,
        'exit': x, 'exit_detail': xd,
        'six_cnt': six_cnt, 'six_grade': six_grade, 'six': six, 'six_detail': six_detail,
        'cci': cci[-1], 'sar_up': bool(up_l[-1]), 'ma60': ma60,
        'above_ma20': above_ma20, 'ma20': ma20, 'ma10': ma10,
        'j': j_l[-1], 'k': k_l[-1], 'dif': dif[-1], 'dea': dea[-1], 'hist': hist[-1],
        'upper': upper, 'lower': lower, 'action': actions[stage_brief],
        'm60': m60_note,
        # v3.7：放量突破20日新高（今日推荐"启动刚突破"判定用）
        'breakout20': breakout20, 'breakout_note': breakout_note, 'hi20': hi20_prev,
    }


def main():
    t0 = time.time()
    session = 'midday' if '--midday' in sys.argv else 'close'
    sess_tag = ' · 午间快照版（今日推荐10只·精选1只）' if session == 'midday' else ''
    print("=" * 90, flush=True)
    print(f"喵喵全市场波段选股扫描 v3.11 · {datetime.date.today().strftime('%Y-%m-%d')}{sess_tag}")
    print(f"框架：账户可买(主板00/60) · 科技+有色 · ≤{PRICE_MAX:.0f}元 · 基本面资格赛(A/B档无红旗) → 技术面5阶段 | ETF独立池")
    print("=" * 90, flush=True)

    # 第一步：拉板块成分
    print("\n[1/5] 拉取行业板块成分...", flush=True)
    techs, metals = [], []
    for board_code, board_name, sector in BOARDS:
        try:
            members = fetch_board_members(board_code, board_name)
            passed = prefilter(members, sector)
            print(f"  {board_name}: 成分{len(members)}只 → 预筛通过{len(passed)}只", flush=True)
            (techs if sector == '科技' else metals).extend(passed)
        except Exception as ex:
            print(f"  [警告] {board_name}板块拉取失败: {ex}", flush=True)

    # 去重（板块间可能有重复）后按成交额取头部
    seen = set()
    techs = [s for s in techs if not (s['code'] in seen or seen.add(s['code']))]
    seen = set()
    metals = [s for s in metals if not (s['code'] in seen or seen.add(s['code']))]
    techs.sort(key=lambda x: -x['amount'])
    metals.sort(key=lambda x: -x['amount'])
    techs, metals = techs[:CAP_TECH], metals[:CAP_METAL]
    print(f"[2/4] 预筛完成：科技取头部{len(techs)}只 / 有色取头部{len(metals)}只", flush=True)

    # 第三步：基本面前置资格赛（v3.0 核心重构——基本面合格才有入池资格）
    stock_heads = techs + metals
    print(f"[2/5] 基本面资格赛：拉取 {len(stock_heads)} 只头部个股 PE/PB/市值...", flush=True)
    fund_map = {}
    try:
        fund_map = fetch_fundamentals([s['symbol'] for s in stock_heads])
    except Exception as ex:
        print(f"  [警告] 基本面批量拉取失败: {ex}", flush=True)
    qualified, rejected_c, rejected_flag = [], [], []
    for s in stock_heads:
        g = grade_fundamentals(fund_map.get(s['symbol'], None))
        s['fund'] = g
        if g and g['fgrade'] in FUND_QUALIFIED and not g['flags']:
            qualified.append(s)
        elif g and g['flags']:
            rejected_flag.append(f"{s['name']}（{g['flags']}）")
        else:
            rejected_c.append(f"{s['name']}（{'C档%d分' % g['fscore'] if g else '无数据'}）")
    print(f"  ✅ 基本面合格（A/B档且无红旗）：{len(qualified)} 只 | "
          f"❌ C档淘汰 {len(rejected_c)} 只 · 红旗淘汰 {len(rejected_flag)} 只", flush=True)

    # 第四步：技术扫描队列 = 合格个股 + 全部ETF（ETF独立成池，不做基本面筛选）
    targets = [(s['symbol'], s['code'], s['name'], False, s['sector']) for s in qualified]
    targets += [(sym, code, name, True, 'ETF') for sym, code, name in ETF_UNIVERSE]
    print(f"[3/5] 技术扫描：合格个股 {len(qualified)} 只 + ETF {len(ETF_UNIVERSE)} 只，并发拉取日线并计算...", flush=True)

    results, failed = [], []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futs = {pool.submit(analyze, *t): t for t in targets}
        done = 0
        for fut in as_completed(futs):
            t = futs[fut]
            done += 1
            if done % 30 == 0:
                print(f"  进度 {done}/{len(targets)}...", flush=True)
            try:
                r = fut.result()
                if r:
                    results.append(r)
                else:
                    failed.append((t[1], '(数据不足)'))
            except Exception as ex:
                failed.append((t[1], f"({type(ex).__name__})"))
        # 失败标的串行补扫一轮
        failed_codes = {c for c, _ in failed}
        retry_targets = [t for t in targets if t[1] in failed_codes]
        if retry_targets:
            print(f"  对 {len(retry_targets)} 只失败标的补扫一轮...", flush=True)
            still_failed = []
            for t in retry_targets:
                time.sleep(0.8)
                try:
                    r = analyze(*t)
                    if r:
                        results.append(r)
                    else:
                        still_failed.append((t[1], '(数据不足)'))
                except Exception:
                    still_failed.append((t[1], '(重试仍失败)'))
            failed = still_failed

    print(f"[4/5] 技术扫描完成：成功 {len(results)} / 失败 {len(failed)}，耗时 {time.time()-t0:.0f}秒", flush=True)

    # 回填基本面到扫描结果（只有合格个股才有 fund 字段——C档/红旗从未进入技术扫描）
    fund_by_code = {s['code']: s['fund'] for s in qualified}
    for r in results:
        if not r['is_etf']:
            r['fund'] = fund_by_code.get(r['code'])

    order = {'B+': 0, 'B': 1, 'A': 2, 'C': 3, 'D': 4, 'E': 5}
    results.sort(key=lambda r: (r['is_etf'], order[r['stage_brief']], -r['score'],
                                -(r['fund']['fscore'] if r.get('fund') else 0), -r['amt20']))

    print(f"\n{'='*90}")
    print("📈 个股候选池（基本面合格）· A/B 阶段标的")
    print('=' * 90)
    for r in results:
        if r['is_etf'] or r['stage_brief'] not in ('B+', 'B', 'A'):
            continue
        gates = ('✓' if r['gate_amt'] else '✗') + ('✓' if r['gate_swing'] else '✗') + ('✓' if r['gate_vol'] else '✗')
        f = r.get('fund')
        ftxt = f"{f['fgrade']}档{f['fscore']}分" if f else '—'
        print(f"[{r['sector']}] {r['code']} {r['name'][:10]:<10} 收{r['close']:>7.3f} {r['stage']:<6} "
              f"分{r['score']:>3} 六联{r['six_cnt']}/5{r['six_grade']} 闸门{gates} 基本面{ftxt} J={r['j']:.0f} 20日均额{r['amt20']/1e8:.2f}亿")

    print(f"\n{'='*90}")
    print("📊 ETF候选池（独立）· A/B 阶段标的")
    print('=' * 90)
    for r in results:
        if (not r['is_etf']) or r['stage_brief'] not in ('B+', 'B', 'A'):
            continue
        gates = ('✓' if r['gate_amt'] else '✗') + ('✓' if r['gate_swing'] else '✗') + ('✓' if r['gate_vol'] else '✗')
        print(f"[ETF] {r['code']} {r['name'][:12]:<12} 收{r['close']:>7.3f} {r['stage']:<6} "
              f"分{r['score']:>3} 六联{r['six_cnt']}/5{r['six_grade']} 闸门{gates} J={r['j']:.0f} 20日均额{r['amt20']/1e8:.2f}亿")

    # v3.11：数据日 = 全部标的最新K线日期的最大值。周末/节假日/隔天补跑时，
    # 候选池正确落为最近交易日（如周六补跑落为 pool_20260904.json），不再错标运行日
    bar_days = [r.get('date', '') for r in results if r.get('date')]
    data_day = max(bar_days).replace('-', '') if bar_days else today_str()
    if data_day != today_str():
        print(f"ℹ️ 数据日 {data_day} ≠ 运行日 {today_str()}（非交易日补跑），候选池按数据日落盘", flush=True)
    html_path = generate_html(results, failed, techs, metals, qualified, rejected_c, rejected_flag, data_day)
    print(f"\n报告已保存: {html_path}")
    json_path = generate_json(results, data_day, session)
    print(f"候选池JSON已保存: {json_path}")
    return results, html_path


def today_str():
    return datetime.date.today().strftime('%Y%m%d')


def _item(r):
    """候选池条目（个股/ETF 通用；个股额外带 fund 基本面字段）"""
    gates = ('✓' if r['gate_amt'] else '✗') + ('✓' if r['gate_swing'] else '✗') + ('✓' if r['gate_vol'] else '✗')
    item = {
        'code': r['code'], 'name': r['name'], 'sector': r['sector'],
        'close': round(r['close'], 3), 'chg': round(r['chg'], 2),
        'stage': r['stage_brief'], 'stageName': r['stage'],
        'score': r['score'], 'entry': f"{r['entry_cnt']}/5",
        'j': round(r['j'], 1), 'amt20': f"{r['amt20']/1e8:.2f}亿",
        'gates': gates, 'detail': ('、'.join(k.split('_', 1)[1] for k, v in r['entry'].items() if v) or '暂无') + (('；' + r['m60']) if r.get('m60') and r['stage_brief'] == 'B+' else ''),
        'action': r['action'], 'm60': r.get('m60', ''),
        # MA20/MA10 显式字段，供作战台仓位计算器「填入MA20止损参考」按钮直接使用
        'ma20': round(r['ma20'], 3), 'ma10': round(r['ma10'], 3),
        # E1~E5 / X1~X5 明细（含数据依据），供作战台「判定」按钮自动勾选清单
        'entryFlags': [{'key': k.split('_', 1)[1], 'on': bool(v), 'detail': r.get('entry_detail', {}).get(k, '')}
                       for k, v in r['entry'].items()],
        'exitFlags': [{'key': k.split('_', 1)[1], 'on': bool(v), 'detail': r.get('exit_detail', {}).get(k, '')}
                      for k, v in r.get('exit', {}).items()],
        # 喵喵六联上车信号（v3.6）：X/5 + 等级 + 明细（供作战台「六联」列/排序使用）
        'six': f"{r['six_cnt']}/5", 'sixGrade': r['six_grade'], 'sixDetail': r['six_detail'],
        'sixFlags': [{'key': k.split('_', 1)[1], 'on': bool(v)} for k, v in r['six'].items()],
    }
    f = r.get('fund')
    if f:
        item['fund'] = {
            'pe': f['pe'], 'pb': f['pb'], 'mcap': f['mcap'], 'roe': f['roe'],
            'fscore': f['fscore'], 'fgrade': f['fgrade'], 'flags': f['flags'],
        }
    return item


def pick_rank(r):
    """v3.10 精选综合评分：趋势强 + 弹性好 + 贴近上车（满分约 27.5）
    ① 趋势强（满分17）：六联指标数×3（0-15）+ C阶段五信号共振+2
    ② 弹性好（满分5）：20日均振幅 avg_amp，≥6% 得满分（avg_amp/3 封顶 2 再 ×2.5）
    ③ 贴近上车（满分约8）：B+鱼头5 / B临近3 / C趋势1 + 入场信号数×0.5 − 乖离惩罚
       （现价高于中轨 MA20 超 4% 开始线性扣分——离买点太远=追高，不算贴近上车）"""
    trend = r['six_cnt'] * 3 + (2 if r['stage_brief'] == 'C' else 0)
    elastic = min(r.get('avg_amp', 0) / 3.0, 2.0) * 2.5
    near = {'B+': 5.0, 'B': 3.0, 'C': 1.0, 'A': 0.0}.get(r['stage_brief'], 0.0) + r['entry_cnt'] * 0.5
    dev = (r['close'] / r['ma20'] - 1) * 100 if r.get('ma20') else 0.0
    near -= max(0.0, dev - 4) * 0.5
    return round(trend + elastic + near, 2)


def pick_note(r):
    """精选推荐理由前缀（三维评分明细，供前端/飞书展示）"""
    dev = (r['close'] / r['ma20'] - 1) * 100 if r.get('ma20') else 0.0
    return (f"🎯综合{pick_rank(r)}=趋势{r['six_cnt']}/5"
            f"{'·C共振' if r['stage_brief'] == 'C' else ''}+弹性{r.get('avg_amp', 0):.1f}%"
            f"+{r['stage_brief']}贴近上车·乖离{dev:+.1f}%")


def build_recommendations(results, today, session='close'):
    """v3.10 今日推荐：①趋势中（C阶段·五信号共振确认）②启动刚突破确认（B+鱼头启动 或 当日放量突破20日新高）
    输出 trend / breakout 两清单 + picks 精选，均带可读推荐理由，供作战台「🔥今日推荐」页签与飞书推送使用。
    精选排序（v3.10）：pick_rank 综合评分 = 趋势强(六联/C共振) + 弹性好(20日均振幅) + 贴近上车(B+/B阶段·入场信号·乖离小)。
    session='close'（默认，15:05 收盘版）：目标共 20 只，不足由 B/A 阶段强标的补足；
    session='midday'（午间快照版，11:40 上午收盘后跑）：目标共 10 只、精选固定 1 只，
    trend/breakout 各≤5 且互斥，精选取两组之外综合评分最高的 1 只，候选不足时宁缺毋滥（不用 B/A 硬凑）。"""
    now_str = datetime.datetime.now().strftime('%H:%M')
    midday = (session == 'midday')
    total_target = REC_MIDDAY_TOTAL if midday else REC_TOTAL_TARGET

    def rec_item(r):
        reasons = []
        if r['stage_brief'] == 'B+':
            reasons.append('60分钟鱼头刚启动')
        if r.get('breakout20'):
            reasons.append('放量突破20日新高')
        if r['stage_brief'] == 'C':
            reasons.append('五信号共振·趋势确认')
        if r['stage_brief'] == 'B':
            reasons.append('⭐B阶段·上车临近')
        if r['stage_brief'] == 'A':
            reasons.append('A底部·观察区')
        if r['six_cnt'] >= 4:
            reasons.append(f"六联{r['six_cnt']}/5 {r['six_grade']}")
        if r['stage_brief'] == 'D':
            reasons.append('⚠️顶部区，仅观察不追')
        reason = '、'.join(reasons) if reasons else (r.get('detail') or '暂无')
        m60_short = ''
        if r['stage_brief'] == 'B+' and r.get('m60'):
            m60_short = r['m60'][:40]
        item = {
            'code': r['code'], 'name': r['name'], 'sector': r['sector'],
            'is_etf': r['is_etf'],
            'close': round(r['close'], 3), 'chg': round(r['chg'], 2),
            'stage': r['stage_brief'], 'stageName': r['stage'],
            'score': r['score'], 'entry': f"{r['entry_cnt']}/5",
            'six': f"{r['six_cnt']}/5", 'sixGrade': r['six_grade'],
            'j': round(r['j'], 1), 'amt20': f"{r['amt20']/1e8:.2f}亿",
            'ma20': round(r['ma20'], 3), 'ma10': round(r['ma10'], 3),
            'detail': ('、'.join(k.split('_', 1)[1] for k, v in r['entry'].items() if v) or '暂无'),
            'reason': reason, 'action': r['action'], 'm60': m60_short,
        }
        return item

    # ① 趋势中 = C 阶段（按 code 去重，再按 score + 涨幅排序；先保留全量列表，收盘/午间按各自名额截取）
    seen_t, trend_all = set(), []
    for r in (x for x in results if x['stage_brief'] == 'C'):
        if r['code'] not in seen_t:
            seen_t.add(r['code'])
            trend_all.append(r)
    trend_all = [rec_item(r) for r in trend_all]
    trend_all.sort(key=lambda x: (-x['score'], -x['chg']))

    # ② 启动刚突破确认 = B+（60分钟鱼头）或 当日放量突破20日新高（排除 D/E 顶部与冷却）；
    #    与 trend 互斥——已是 C 趋势的票（如 C 阶段+放量突破）只进 trend，不重复展示
    used_trend = {x['code'] for x in trend_all}
    breakout_raw = [r for r in results
                    if r['stage_brief'] == 'B+' or (r.get('breakout20') and r['stage_brief'] not in ('D', 'E'))]
    seen, breakout_all = set(), []
    for r in breakout_raw:
        if r['code'] not in seen and r['code'] not in used_trend:
            seen.add(r['code'])
            breakout_all.append(r)
    breakout_all = [rec_item(r) for r in breakout_all]
    breakout_all.sort(key=lambda x: (-(1 if x['stage'] == 'B+' else 0), -x['score'], -x['chg']))

    if midday:
        # 午间快照版：先按名义名额（trend/breakout 各前5）圈定展示集，
        # 精选 = 未被两组展示的候选（C/B+/放量突破）中 pick_rank 综合评分最高 1 只
        # （v3.10：趋势强·六联/C共振 + 弹性好·20日均振幅 + 贴近上车·B+/B阶段·乖离小）
        t0 = min(len(trend_all), 5)
        b0 = min(len(breakout_all), 5)
        displayed0 = {x['code'] for x in trend_all[:t0]} | {x['code'] for x in breakout_all[:b0]}
        all_raw = [r for r in results
                   if r['stage_brief'] in ('C', 'B+') or (r.get('breakout20') and r['stage_brief'] not in ('D', 'E'))]
        seen2, cand = set(), []
        for r in all_raw:
            if r['code'] in seen2:
                continue
            seen2.add(r['code'])
            if r['code'] not in displayed0:
                cand.append(r)
        ranked = sorted(cand, key=lambda r: (pick_rank(r), r['score']), reverse=True)
        picks = [rec_item(r) for r in ranked[:REC_MIDDAY_PICKS]]
        for r, p in zip(ranked[:REC_MIDDAY_PICKS], picks):
            p['reason'] = pick_note(r) + '｜' + p['reason']
        pick_codes = {x['code'] for x in picks}
        # 名额分配：精选固定后，剩余名额给 trend（≤5 优先）→ breakout（≤5）→ 相互让位扩展；
        # 扩展池避开已定为精选的票（防止精选被吸收进两组导致总数缩水）
        trend_pool = trend_all[:5] + [x for x in trend_all[5:] if x['code'] not in pick_codes]
        breakout_pool = breakout_all[:5] + [x for x in breakout_all[5:] if x['code'] not in pick_codes]
        budget = total_target - len(picks)
        t_take = min(5, len(trend_pool), budget)
        b_take = min(5, len(breakout_pool), budget - t_take)
        if t_take + b_take < budget:                       # trend 候选不足 → breakout 扩展（可超5）
            b_take = min(len(breakout_pool), budget - t_take)
        if t_take + b_take < budget:                       # breakout 也不足 → trend 扩展（可超5）
            t_take = min(len(trend_pool), budget - b_take)
        trend, breakout = trend_pool[:t_take], breakout_pool[:b_take]
        note = ('午间快照（上午收盘后自动生成）：趋势中=C阶段；启动刚突破=B+鱼头或放量突破20日新高；'
                '精选=两组未展示候选中「趋势强·弹性好·贴近上车」综合评分最高的1只，目标共10只（候选不足宁缺毋滥）。'
                '收盘后15:05更新为20只完整版。仅供个人研究，不构成投资建议。')
    else:
        # ③ 精选（v3.10 收盘版）：与已展示的 trend/breakout 严格去重——精选只放两组未展示的标的，
        #    按 pick_rank 综合评分排序（趋势强 + 弹性好 + 贴近上车）；
        #    合计不足 REC_TOTAL_TARGET(20) 时，继续从 B/A 阶段强标的按序补足（排除 D/E 顶部与冷却，绝不重复）
        trend, breakout = trend_all[:10], breakout_all[:10]
        used = {x['code'] for x in trend} | {x['code'] for x in breakout}
        all_raw = [r for r in results
                   if r['stage_brief'] in ('C', 'B+') or (r.get('breakout20') and r['stage_brief'] not in ('D', 'E'))]
        seen2, cand = set(), []
        for r in all_raw:
            if r['code'] in seen2:
                continue
            seen2.add(r['code'])
            if r['code'] not in used:
                cand.append(r)
        ranked = sorted(cand, key=lambda r: (pick_rank(r), r['score']), reverse=True)
        pick_src = ranked[:REC_TOTAL_TARGET]
        used |= {r['code'] for r in pick_src}
        if len(trend) + len(breakout) + len(pick_src) < REC_TOTAL_TARGET:
            rest = [r for r in results
                    if r['code'] not in used and r['stage_brief'] not in ('D', 'E')]
            rest.sort(key=lambda r: (r['stage_brief'] in ('B+', 'B'), r['six_cnt'], r['score']), reverse=True)
            for r in rest:
                if len(trend) + len(breakout) + len(pick_src) >= REC_TOTAL_TARGET:
                    break
                pick_src.append(r)
                used.add(r['code'])
        # 精选最终排序：强信号与 B/A 补足统一按三维综合评分降序（趋势强·弹性好·贴近上车）
        pick_src.sort(key=lambda r: (pick_rank(r), r['score']), reverse=True)
        picks = []
        for r in pick_src:
            item = rec_item(r)
            item['reason'] = pick_note(r) + '｜' + item['reason']
            picks.append(item)
        note = ('每日收盘后自动生成：趋势中=五信号共振确认的C阶段；启动刚突破=60分钟鱼头B+或当日放量突破20日新高；'
                '精选=两组之外的强信号，按「趋势强·弹性好·贴近上车」综合评分排序；不足20只由B/A阶段观察补足。'
                '三组严格去重，目标共20只。仅供个人研究，不构成投资建议。')

    # 最终防线：三组整体去重（保序先到先得，杜绝任何路径下的重复展示）
    _seen_final = set()
    for _lst in (trend, breakout, picks):
        _keep = []
        for _x in _lst:
            if _x['code'] not in _seen_final:
                _seen_final.add(_x['code'])
                _keep.append(_x)
        _lst[:] = _keep

    return {
        'date': today, 'generated': now_str, 'session': session,
        'note': note,
        'trend': trend, 'breakout': breakout, 'picks': picks,
    }


def generate_json(results, today, session='close'):
    """输出候选池 JSON v2（个股池与ETF池分离，供波段作战台 v4 导入）；session 决定今日推荐形态（收盘20只 / 午间10只精选1）"""
    stocks = [_item(r) for r in results if not r['is_etf']]
    etfs = [_item(r) for r in results if r['is_etf']]
    out = {
        'date': today, 'session': session,
        'criteria': (f'账户可买：沪深主板00/60（开户满3年·资产10万以内）· 价格≤{PRICE_MAX:.0f}元 · '
                     f'科技(电子/计算机/通信/传媒)+有色 · 非ST · 成交额≥8000万 · 基本面A/B档且无红旗（资格赛）'),
        'stocks': stocks,
        'etfs': etfs,
        # v3.10 今日推荐：趋势中 + 启动刚突破确认 + 精选（收盘版20只 / 午间快照版10只·精选1只，精选按三维综合评分）
        'recommendations': build_recommendations(results, today, session),
        'count': len(stocks) + len(etfs),
    }
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'reports')
    out_dir = os.path.normpath(out_dir)
    path = os.path.join(out_dir, f'pool_{today}.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    return path


def generate_html(results, failed, techs, metals, qualified, rejected_c, rejected_flag, data_day=None):
    today = data_day or datetime.date.today().strftime('%Y%m%d')
    stage_color = {'A': '#8a6d1a', 'B': '#c0392b', 'B+': '#0e8a6d', 'C': '#2c5f8a', 'D': '#a04000', 'E': '#555'}
    grade_color = {'A': '#1d7a52', 'B': '#8a6d1a', 'C': '#a32d2d'}

    stocks = [r for r in results if not r['is_etf']]
    etfs = [r for r in results if r['is_etf']]

    def gates_html(r):
        return ''.join(f"<span style='color:{'#27500a' if g else '#a32d2d'}'>{'✓' if g else '✗'}</span> "
                       for g in (r['gate_amt'], r['gate_swing'], r['gate_vol']))

    def fund_cell(f, brief=False):
        if not f:
            return '<span style="color:#999">—</span>'
        if brief:
            return (f"<span style='color:{grade_color[f['fgrade']]}'>{f['fgrade']}</span>"
                    f"<span style='font-size:11px;color:#666'> · PE{f['pe']:.0f} · {f['mcap']:.0f}亿</span>")
        html = (f"<span style='color:{grade_color[f['fgrade']]};font-weight:700'>{f['fgrade']}档 {f['fscore']}分</span>"
                f"<br><span style='font-size:11px;color:#666'>PE {f['pe']:.0f} · PB {f['pb']:.1f} · {f['mcap']:.0f}亿"
                f"{(' · ROE ' + format(f['roe'], '.1f') + '%') if f['roe'] is not None else ''}</span>")
        if f['flags']:
            html += f"<br><span style='font-size:11px;color:#a32d2d'>⚠ {f['flags']}</span>"
        return html

    def row_ab(r, with_fund):
        sc = stage_color[r['stage_brief']]
        chg_cls = 'up' if r['chg'] > 0 else ('down' if r['chg'] < 0 else '')
        detail = '、'.join(k.split('_', 1)[1] for k, v in r['entry'].items() if v) or '暂无'
        fcell = fund_cell(r.get('fund')) if with_fund else '<span style="color:#999">不适用</span>'
        s6 = r['six_cnt']
        s6_color = '#0e8a6d' if s6 >= 4 else ('#8a6d1a' if s6 == 3 else '#999')
        six_cell = (f"<b style='color:{s6_color}'>{s6}/5</b>"
                    f"<span style='color:{s6_color}'>{r['six_grade']}</span>"
                    f"<div style='font-size:10px;color:#888' title=\"{r['six_detail']}\">{r['six_detail'][:28]}…</div>")
        return (f"<tr><td>{r['code']}</td><td>{r['name']}</td><td>{r['sector']}</td>"
                f"<td class=\"{chg_cls}\">{r['close']:.3f}</td><td class=\"{chg_cls}\">{r['chg']:+.2f}%</td>"
                f"<td style=\"color:{sc};font-weight:700\">{r['stage']}</td>"
                f"<td><b>{r['score']}</b></td><td>{r['entry_cnt']}/5</td>"
                f"<td>{six_cell}</td><td>{fcell}</td>"
                f"<td>{r['j']:.0f}</td><td>{r['amt20']/1e8:.2f}亿</td><td>{gates_html(r)}</td>"
                f"<td style=\"font-size:12px\">{detail}</td></tr>")

    def row_all(r, with_fund):
        sc = stage_color[r['stage_brief']]
        chg_cls = 'up' if r['chg'] > 0 else ('down' if r['chg'] < 0 else '')
        fcell = fund_cell(r.get('fund'), brief=True) if with_fund else '—'
        return (f"<tr><td>{r['code']}</td><td>{r['name']}</td><td>{r['sector']}</td>"
                f"<td class=\"{chg_cls}\">{r['close']:.3f}</td><td class=\"{chg_cls}\">{r['chg']:+.2f}%</td>"
                f"<td style=\"color:{sc};font-weight:700\">{r['stage']}</td><td>{r['score']}</td>"
                f"<td>{r['entry_cnt']}/5</td><td>{fcell}</td><td>{r['j']:.0f}</td><td>{r['amt20']/1e8:.2f}亿</td>"
                f"<td style=\"font-size:12px\">{r['action']}</td></tr>")

    th_ab = ('<tr><th>代码</th><th>名称</th><th>板块</th><th>收盘</th><th>涨跌</th><th>周期阶段</th>'
             '<th>信号分</th><th>上车条</th><th>六联</th><th>基本面</th><th>J值</th><th>20日均额</th><th>闸门</th><th>已满足条件</th></tr>')
    th_all = ('<tr><th>代码</th><th>名称</th><th>板块</th><th>收盘</th><th>涨跌</th><th>周期阶段</th>'
              '<th>信号分</th><th>上车条</th><th>基本面</th><th>J值</th><th>20日均额</th><th>系统动作</th></tr>')

    stock_ab = [r for r in stocks if r['stage_brief'] in ('A', 'B', 'B+')]
    etf_ab = [r for r in etfs if r['stage_brief'] in ('A', 'B', 'B+')]
    stock_all = [r for r in stocks if r['gate_amt'] and r['gate_swing'] and r['gate_vol']]
    etf_all = [r for r in etfs if r['gate_amt'] and r['gate_swing'] and r['gate_vol']]

    stock_ab_html = ('<table>' + th_ab + ''.join(row_ab(r, True) for r in stock_ab) + '</table>') if stock_ab else \
        '<p style="color:#999">今日个股池无 A/B 阶段标的——空仓等待也是波段选手的常态。</p>'
    etf_ab_html = ('<table>' + th_ab + ''.join(row_ab(r, False) for r in etf_ab) + '</table>') if etf_ab else \
        '<p style="color:#999">今日 ETF 池无 A/B 阶段标的。</p>'

    ba_detail = ''
    for r in stock_ab[:20]:
        items = ''.join(f"<li style='color:{'#27500a' if v else '#999'}'>[{'✓' if v else '✗'}] {k.split('_',1)[1]}</li>"
                        for k, v in r['entry'].items())
        f = r.get('fund')
        ftxt = f"{f['fgrade']}档{f['fscore']}分" if f else ''
        ba_detail += (f"<div class='card'><b>{r['name']}（{r['code']}）</b> · [{r['sector']}] · {r['stage']} · "
                      f"得分 {r['score']} · 基本面 {ftxt} · J={r['j']:.0f} · MA20={r['ma20']:.3f} · BOLL下轨={r['lower']:.3f}<ul>{items}</ul></div>")

    cnt_s = {s: sum(1 for r in stocks if r['stage_brief'] == s) for s in ('B+', 'B', 'A', 'C', 'D', 'E')}
    cnt_e = {s: sum(1 for r in etfs if r['stage_brief'] == s) for s in ('B+', 'B', 'A', 'C', 'D', 'E')}
    fg = {'A': 0, 'B': 0}
    for s in qualified:
        if s['fund']:
            fg[s['fund']['fgrade']] += 1

    funnel_html = (
        f"<b>五层漏斗</b>：① 账户可买（沪深主板00/60 · 开户满3年·资产10万以内，无创业板/科创板权限）→ "
        f"② 行业聚焦：科技(电子/计算机/通信/传媒)+有色金属 → ③ 硬性预筛（≤{PRICE_MAX:.0f}元 · 非ST · 成交额≥8000万）"
        f"取头部：科技 {len(techs)} 只 / 有色 {len(metals)} 只 → "
        f"④ <b>基本面资格赛</b>：合格 <b>{len(qualified)}</b> 只（🟢A档 {fg['A']} · 🟡B档 {fg['B']}，均无红旗），"
        f"淘汰 C档 {len(rejected_c)} 只 + 红旗 {len(rejected_flag)} 只 → "
        f"⑤ 技术面阶段分类：个股池 <b>{len(stocks)}</b> 只 + ETF池 <b>{len(etfs)}</b> 只（独立）。<br>"
        f"个股池阶段：A底部 {cnt_s['A']} · <b>B上车临近 {cnt_s['B']}</b> · <b style='color:#0e8a6d'>B+鱼头启动 {cnt_s['B+']}</b> · C趋势 {cnt_s['C']} · D顶部 {cnt_s['D']} · E冷却 {cnt_s['E']}"
        f"　|　ETF池阶段：A底部 {cnt_e['A']} · <b>B上车临近 {cnt_e['B']}</b> · <b style='color:#0e8a6d'>B+鱼头启动 {cnt_e['B+']}</b> · C趋势 {cnt_e['C']} · D顶部 {cnt_e['D']} · E冷却 {cnt_e['E']}")

    rej_html = ''
    if rejected_c or rejected_flag:
        rej_html = (f"<h2>🚫 基本面淘汰名单（留档备查 · 不入候选池）</h2><div class='card' style='font-size:12.5px'>"
                    f"<b>红旗淘汰 {len(rejected_flag)} 只</b>（亏损/ROE&lt;3%/微市值&lt;50亿/PB&gt;15）："
                    f"{'、'.join(rejected_flag[:40])}{'…' if len(rejected_flag) > 40 else ''}<br><br>"
                    f"<b>C档淘汰 {len(rejected_c)} 只</b>（三维度评分&lt;14）："
                    f"{'、'.join(rejected_c[:40])}{'…' if len(rejected_c) > 40 else ''}</div>")

    html = f"""<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8">
<title>全市场波段选股扫描 v3.4 · {today}</title><style>
body{{font-family:"PingFang SC","Microsoft YaHei",sans-serif;background:#f7f6f3;color:#2c2c2a;margin:0;padding:24px;line-height:1.6}}
.c{{max-width:1180px;margin:0 auto}}
h1{{font-size:22px;color:#26215c;border-bottom:3px solid #534ab7;padding-bottom:8px}}
h2{{font-size:17px;color:#3c3489;margin-top:28px;padding-left:10px;border-left:4px solid #534ab7}}
table{{width:100%;border-collapse:collapse;background:#fff;margin:12px 0}}
th{{background:#eeedfe;color:#3c3489;padding:8px 8px;font-size:13px;text-align:left;white-space:nowrap}}
td{{padding:7px 8px;border-bottom:1px solid #e8e6df;font-size:13px;white-space:nowrap}}
.up{{color:#d84040;font-weight:600}} .down{{color:#1d7a52;font-weight:600}}
.card{{background:#fff;border-radius:10px;padding:14px 18px;margin:10px 0;box-shadow:0 1px 3px rgba(0,0,0,.06)}}
ul{{margin:6px 0}} li{{font-size:13px}}
.note{{background:#fff8e6;border:1px solid #f0dca8;border-radius:8px;padding:10px 14px;font-size:13px;margin:12px 0}}
.pool-tag{{display:inline-block;background:#3c3489;color:#fff;border-radius:6px;padding:1px 8px;font-size:13px;margin-right:6px}}
.pool-tag.etf{{background:#1d6a52}}
.footer{{text-align:center;color:#888;font-size:12px;margin-top:24px}}
</style></head><body><div class="c">
<h1>全市场波段选股扫描 v3.4 · {today}</h1>
<p style="font-size:13px;color:#666">逻辑框架：<b>账户权限定范围（主板00/60）→ 行业聚焦（科技+有色）→ 硬性预筛 → 基本面资格赛（A/B档且无红旗才有入池资格）→ 技术面定阶段（上车临近/底部/趋势/顶部/冷却）</b>。ETF 不做基本面筛选，独立成池。<br>准入闸门：个股 20日均额≥2亿，ETF≥3000万；250日振幅≥35%；日振幅1%~5.5%</p>
<div class="note">{funnel_html}</div>

<h2><span class="pool-tag">📈 个股候选池</span>基本面合格 · A/B 阶段标的（{len(stock_ab)} 只 · 推送重点）</h2>
{stock_ab_html}

<h2>个股池 · B/A 阶段上车条件明细（Top 20）</h2>
{ba_detail or '<p style="color:#999">无明细</p>'}

<h2>个股池 · 全量排名（闸门全通过 · 按操作优先级）</h2>
<table>{th_all}{''.join(row_all(r, True) for r in stock_all)}</table>

<h2><span class="pool-tag etf">📊 ETF 候选池</span>独立板块 · A/B 阶段标的（{len(etf_ab)} 只）</h2>
{etf_ab_html}

<h2>ETF池 · 全量排名（闸门全通过）</h2>
<table>{th_all}{''.join(row_all(r, False) for r in etf_all)}</table>

{rej_html}

<h2>使用说明（v3.0 新框架）</h2>
<div class="card" style="font-size:13px">
<ol>
<li><b>基本面定资格，技术面定阶段</b>：个股池里的每一只都已通过基本面资格赛（A/B档且无红旗）——亏损股、微市值壳股、高泡沫股从源头剔除，不再出现在候选池。</li>
<li><b>账户权限边界</b>：开户满3年 + 资产10万以内 → 仅沪深主板 00/60 开头。创业板(30)、科创板(68)、北交所已被预筛排除，池内所有标的账户均可买。</li>
<li><b>六阶段操作纪律</b>：🚀<b style='color:#0e8a6d'>B+鱼头启动</b>（日线B阶段 + 60分钟低位金叉且站上60分钟MA20）鱼头刚抬头，可贴60分钟MA20试探首仓；明细中「E5放量阳线✅ / ⚠️缩量待确认」「红柱扩张✅ / 收窄⚠️」为60分钟辅助标注（不影响判定），两项均⚠️的标的宁可再等一根确认；⭐B上车临近（≥3条上车信号）置顶每日核对；A底部观察不动作；C趋势中持有者拿住、未上车不追；D顶部执行下车清单；E冷却禁止抄底（20个交易日）。</li>
<li><b>ETF独立板块</b>：ETF 跟踪指数而非个股基本面，故不做资格赛；仅按流动性与波动闸门 + 技术阶段分类，作为个股之外的仓位工具（单标的≤40%）。</li>
<li><b>个股仓位纪律</b>：个股波动与暴雷风险高于 ETF，单票≤15%，止损距离放宽，首批仓位更轻。</li>
<li>B阶段标的次日 14:45 用《波段作战台》做最终判定；本池每交易日 15:45 自动扫描并飞书推送。</li>
</ol></div>
{f'<p style="font-size:12px;color:#999">拉取失败 {len(failed)} 只：{", ".join(c + r for c, r in failed[:30])}{"..." if len(failed) > 30 else ""}</p>' if failed else ''}
<div class="footer">喵喵波段选股系统 v3.6（基本面前置 · 个股/ETF 分池 · E1金叉放宽 · B+鱼头启动60分钟确认 · B+量能/红柱标注 · <b>六联上车信号</b>：L0不逆势 + T领先CCI触底拐头 + R1~R5共振确认，≥4强/5极强，加成排序）· 仅供个人研究，不构成投资建议</div>
</div></body></html>"""

    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'reports')
    out_dir = os.path.normpath(out_dir)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f'screen_full_{today}.html')
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    return path


if __name__ == '__main__':
    main()

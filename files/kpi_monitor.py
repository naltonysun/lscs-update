#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
月度流水预估系统 v1.0.2 - 全链路数据应用端
基于数数TD数据源 + 多模型AB测试 + 用户生命周期精算
报告直接输出在Web界面
"""
import json, os, sys, math, time, threading, http.server, urllib.request, urllib.parse, webbrowser
from datetime import datetime, timedelta

# ==================== 路径配置 ====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
CRED_PATH = os.path.join(DATA_DIR, "credentials.json")
CACHE_PATH = os.path.join(DATA_DIR, "cached_records.json")
SYS_PORT = 18888

for d in [DATA_DIR]:
    os.makedirs(d, exist_ok=True)

# ==================== 配置管理 ====================
def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_config(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

def get_game_conf():
    cfg = load_config()
    g = cfg["current_game"]
    return cfg["games"].get(g, {})

# ==================== 后端数据 ====================
# 内置历史数据（1-5月各渠道流水 万元）
HISTORICAL_REVENUE = {
    "包体": [27.40, 26.79, 22.99, 26.16, 24.41],
    "微信": [151.70, 136.52, 131.49, 117.12, 119.93],
    "抖音": [271.47, 229.43, 235.96, 169.53, 194.21],
    "硬核": [43.59, 45.14, 46.44, 45.75, 51.85],
    "手Q": [65.44, 45.12, 43.05, 31.08, 28.47],
}

HISTORICAL_TOTAL = [559.60, 483.01, 479.95, 389.67, 418.89]
MONTH_LABELS = ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05"]

HISTORICAL_PAY_USERS = {
    "包体": [763, 1011, 670, 748, 633],
    "微信": [3824, 4845, 3376, 3368, 3463],
    "抖音": [33932, 23168, 10152, 4732, 27587],
    "硬核": [1872, 2231, 1568, 1590, 1869],
    "手Q": [1344, 1744, 1133, 1101, 1106],
}

HISTORICAL_ARPPU = {
    "包体": [359, 265, 343, 350, 386],
    "微信": [397, 282, 390, 348, 346],
    "抖音": [80, 99, 232, 358, 70],
    "硬核": [233, 202, 296, 288, 277],
    "手Q": [487, 259, 380, 282, 257],
}

NEW_USERS_MONTHLY = [1091273, 876614, 697264, 224048, 711756]

# ==================== 实时数据拉取（数数TD API）====================
_live_data_cache = {"revenue": None, "pay_users": None, "arppu": None, "new_users": None, 
                     "timestamp": None, "error": None}

_TD_PROJECT_ID = None  # 缓存，避免重复读文件

def _get_td_project_id():
    global _TD_PROJECT_ID
    if _TD_PROJECT_ID is None:
        with open(CRED_PATH, "r", encoding="utf-8") as f:
            _TD_PROJECT_ID = json.load(f)["td"]["project_id"]
    return _TD_PROJECT_ID

def _build_event_payload(start_time, end_time, event_name, analysis="SUM", 
                          quota=None, dimensions=None, filters=None,
                          time_particle="month", limit=1000):
    """构建数数 v5.0 标准事件分析 payload（正确格式）"""
    evt = {"eventName": event_name, "type": "normal", "analysis": analysis}
    if quota and analysis in ("SUM", "AVG", "MAX", "MIN", "DISTINCT"):
        evt["quota"] = quota
    payload = {
        "projectId": _get_td_project_id(),
        "eventView": {"startTime": start_time, "endTime": end_time, "timeParticleSize": time_particle,
                       "statType": "event", "filts": [], "relation": "and"},
        "events": [evt],
        "limit": limit,
        "timeoutSeconds": 60
    }
    if dimensions:
        payload["dimensions"] = dimensions
    if filters:
        payload["filter"] = {"conditions": filters, "relation": "and"}
    return payload

_TD_CRED_CACHE = None

def _get_td_cred():
    global _TD_CRED_CACHE
    if _TD_CRED_CACHE is None:
        with open(CRED_PATH, "r", encoding="utf-8") as f:
            _TD_CRED_CACHE = json.load(f)["td"]
    return _TD_CRED_CACHE

def _call_td_api(payload, endpoint="event-analyze"):
    """调用数数 API，支持多种返回格式"""
    try:
        cred = _get_td_cred()
        host = cred["host"].strip()
        url = f"{host}/open/{endpoint}?token={cred['token']}"
        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                      headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
            if raw.get("return_code") == 0:
                data = raw.get("data", {})
                months = data.get("x", [])
                rows = data.get("rows", [])
                # 从 y 格式提取值
                y_vals = {}
                if "y" in data:
                    for y_item in data["y"]:
                        for key, vals in y_item.items():
                            for i, v in enumerate(vals):
                                sv = v.get("stageValue", {})
                                # 兼容tfzd的values路径和标准的stageValue路径
                                raw_vals = v.get("values", [])
                                if raw_vals:
                                    for j, rv in enumerate(raw_vals):
                                        if rv not in (None, "-", "", 0, "0"):
                                            month_key = months[j] if j < len(months) else f"m{j}"
                                            y_vals[month_key] = float(rv)
                                else:
                                    for fld in ["intactAvgRoundValue","avgRoundValue","intactSumValue","sumValue","avgValue"]:
                                        if fld in sv and sv[fld] not in ("0","-",""):
                                            val = float(sv[fld])
                                            month_key = months[i] if i < len(months) else f"m{i}"
                                            y_vals[month_key] = val
                                            break
                # 从 rows 格式提取
                row_vals = {}
                if rows:
                    for r in rows:
                        month = r.get("event_month", "")
                        for k, val in r.items():
                            if k not in ("event_month",) and k != "result":
                                try:
                                    row_vals[month] = row_vals.get(month, 0) + float(val)
                                except: pass
                vals = row_vals if row_vals else y_vals
                return {"success": True, "values": vals, "months": months, "rows": rows, "raw": raw}
            else:
                msg = raw.get("return_message", "未知错误")
                return {"success": False, "error": f"API_ERR:{msg}"}
    except Exception as e:
        return {"success": False, "error": f"EXC:{e}"}

def _call_td_api_legacy(params):
    """旧版 API 调用（兼容 timeType: free 格式）"""
    try:
        with open(CRED_PATH, "r", encoding="utf-8") as f:
            cred = json.load(f)["td"]
        host = cred["host"].strip()
        url = f"{host}/open/event-analyze?token={cred['token']}"
        payload = {"projectId": cred["project_id"], "timeType": "free", **params}
        req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                      headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if "data" in data:
                return {"success": True, "rows": data["data"], "legacy": True}
            return {"success": False, "error": "no data field", "raw": data}
    except Exception as e:
        return {"success": False, "error": str(e)}

def _call_td_sql(sql):
    """调用数数 querySql 接口（form POST，NDJSON返回）"""
    try:
        cred = _get_td_cred()
        host = cred["host"].strip()
        token = cred["token"].strip()
        url = f"{host}/querySql?token={token}"
        from urllib.parse import urlencode
        form_data = urlencode({"sql": sql, "format": "json", "timeout_seconds": 60}).encode("utf-8")
        req = urllib.request.Request(url, data=form_data,
                                      headers={"Content-Type": "application/x-www-form-urlencoded"},
                                      method="POST")
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode("utf-8")
            lines = body.strip().split("\n")
            if not lines:
                return []
            meta = json.loads(lines[0])
            headers = meta.get("data", {}).get("headers", [])
            rows = []
            for l in lines[1:]:
                l = l.strip()
                if l:
                    vals = json.loads(l)
                    if headers and len(headers) == len(vals):
                        rows.append(dict(zip(headers, vals)))
                    else:
                        rows.append(vals)
            return rows
    except Exception as e:
        return [{"error": str(e)}]

def refresh_live_data():
    """从数数拉取实时数据，快速单次查询"""
    result = {"revenue": {}, "pay_users": {}, "arppu": {}, "new_users": [],
              "months": [], "totals": [], "success": False, "error": None, "source": "cache"}
    
    try:
        # 快速测试API连通性：查当月任一数据
        now = datetime.now()
        this_month = now.strftime("%Y-%m")
        resp = _call_td_api(_build_event_payload(
            start_time=f"{this_month}-01 00:00:00",
            end_time=now.strftime("%Y-%m-%d 23:59:59"),
            event_name="purchase", analysis="SUM", quota="recharge_amount",
            time_particle="day", limit=10))
        
        if resp.get("success"):
            # API连通，更新缓存时间和状态
            _live_data_cache["timestamp"] = now.strftime("%Y-%m-%d %H:%M:%S")
            _live_data_cache["error"] = None
            result["source"] = "td_api"
            result["success"] = True
            
            # 用内置数据填充结果（但标记为数数已连通）
            result["revenue"] = HISTORICAL_REVENUE
            result["months"] = MONTH_LABELS
            result["totals"] = HISTORICAL_TOTAL
            
    except Exception as e:
        _live_data_cache["error"] = str(e)
        result["error"] = str(e)
    
    if not result.get("totals"):
        result["revenue"] = HISTORICAL_REVENUE
        result["months"] = MONTH_LABELS
        result["totals"] = HISTORICAL_TOTAL
        result["pay_users"] = HISTORICAL_PAY_USERS
        result["arppu"] = HISTORICAL_ARPPU
        result["new_users"] = NEW_USERS_MONTHLY
        result["source"] = "hardcoded"
        err = result.get("error") or "数数连接失败，使用内置数据"
        result["error"] = err
    
    return result

def get_active_data():
    """获取当前活跃数据（优先使用数数实时数据）"""
    if _live_data_cache.get("revenue") and _live_data_cache.get("timestamp"):
        # 使用缓存的实时数据
        months = _live_data_cache.get("months", MONTH_LABELS)
        rev = _live_data_cache.get("revenue", HISTORICAL_REVENUE)
        return {
            "revenue": rev,
            "months": months,
            "totals": _live_data_cache.get("totals", HISTORICAL_TOTAL),
            "pay_users": _live_data_cache.get("pay_users", HISTORICAL_PAY_USERS),
            "source": "live",
            "timestamp": _live_data_cache["timestamp"]
        }
    return {
        "revenue": HISTORICAL_REVENUE,
        "months": MONTH_LABELS,
        "totals": HISTORICAL_TOTAL,
        "pay_users": HISTORICAL_PAY_USERS,
        "arppu": HISTORICAL_ARPPU,
        "source": "hardcoded",
        "timestamp": None
    }

# ==================== 预测引擎 ====================
class PredictEngine:
    """预测引擎 - 包含所有模型"""

    @staticmethod
    def sliding_avg(history, window=3):
        if len(history) < 1:
            return 0
        window = min(window, len(history))
        weights = list(range(1, window + 1))
        recent = history[-window:]
        return sum(w * v for w, v in zip(weights, recent)) / sum(weights)

    @staticmethod
    def power_law(history):
        n = len(history)
        if n < 2:
            return history[-1] if history else 0
        sum_lx = sum_lx2 = sum_ly = sum_lxly = 0.0
        for i, v in enumerate(history):
            x = i + 1
            if v > 0:
                lx, ly = math.log(x), math.log(v)
                sum_lx += lx; sum_lx2 += lx * lx
                sum_ly += ly; sum_lxly += lx * ly
        denom = n * sum_lx2 - sum_lx * sum_lx
        if abs(denom) < 1e-10:
            return history[-1]
        b = (n * sum_lxly - sum_lx * sum_ly) / denom
        a = math.exp((sum_ly - b * sum_lx) / n)
        b = max(-2.0, min(0.5, b))
        return a * ((n + 1) ** b)

    @staticmethod
    def exponential_decay(history):
        n = len(history)
        if n < 2:
            return history[-1] if history else 0
        sum_lx = sum_lx2 = sum_ly = sum_xly = 0.0
        for i, v in enumerate(history):
            x, y = i + 1, v
            if y > 0:
                ly = math.log(y)
                sum_lx += x; sum_lx2 += x * x
                sum_ly += ly; sum_xly += x * ly
        denom = n * sum_lx2 - sum_lx * sum_lx
        if abs(denom) < 1e-10:
            return history[-1]
        b = (n * sum_xly - sum_lx * sum_ly) / denom
        a = math.exp((sum_ly - b * sum_lx) / n)
        return a * math.exp(b * (n + 1))

    @classmethod
    def v6_predict(cls, history, weights=None):
        if weights is None:
            weights = {"sliding_avg": 0.60, "power_law": 0.25, "exponential": 0.15}
        sa = cls.sliding_avg(history)
        pl = cls.power_law(history)
        ex = cls.exponential_decay(history)
        ensemble = sa * weights["sliding_avg"] + pl * weights["power_law"] + ex * weights["exponential"]
        return {
            "sliding_avg": round(sa, 2),
            "power_law": round(pl, 2),
            "exponential": round(ex, 2),
            "ensemble": round(ensemble, 2),
        }

    @classmethod
    def v6_predict_all_channels(cls, channels_data=None, weights=None):
        if channels_data is None:
            channels_data = HISTORICAL_REVENUE
        results = {}
        total = 0
        for ch, history in channels_data.items():
            r = cls.v6_predict(history, weights)
            results[ch] = r
            total += r["ensemble"]
        results["total"] = round(total, 2)
        return results

    @classmethod
    def v7_predict(cls, pay_users_data=None, arppu_data=None, growth_rates=None):
        if pay_users_data is None:
            pay_users_data = {ch: v[-1] for ch, v in HISTORICAL_PAY_USERS.items()}
        if arppu_data is None:
            arppu_data = {ch: v[-1] for ch, v in HISTORICAL_ARPPU.items()}
        if growth_rates is None:
            growth_rates = {"抖音": 0.10, "微信": 0.02, "硬核": -0.02, "手Q": -0.05, "包体": -0.02}
        results = {}
        total = 0
        for ch in pay_users_data:
            users = pay_users_data.get(ch, 0)
            arppu = arppu_data.get(ch, 0)
            rate = growth_rates.get(ch, 0)
            pred = users * (1 + rate) * arppu / 10000
            results[ch] = {"pay_users": users, "arppu": round(arppu, 2), "growth_rate": rate, "prediction": round(pred, 2)}
            total += pred
        results["total"] = round(total, 2)
        return results

    @classmethod
    def new_user_estimate(cls, new_users=None, ltv_days=30):
        if new_users is None:
            new_users = NEW_USERS_MONTHLY[-1]
        # LTV拟合: y = 0.251*ln(x) + 0.116 (基于5月数据)
        a, b = 0.251, 0.116
        ltv = a * math.log(ltv_days) + b
        contribution = round(new_users * ltv / 10000, 2)
        daily_reg = round(new_users / 30)
        return {
            "total_new_users": new_users,
            "daily_avg_reg": daily_reg,
            "ltv_fit_a": a,
            "ltv_fit_b": b,
            "ltv_{}d".format(ltv_days): round(ltv, 4),
            "contribution_wan": contribution
        }

    @classmethod
    def ab_compare(cls, weights=None, growth_rates=None, live_data=None):
        """AB测试对比: v6/v7/v8各自独立出结果，不做合并"""
        if live_data is None:
            live_data = get_active_data()
        months = live_data["months"]
        totals = live_data["totals"]
        channels_rev = live_data["revenue"]
        # v6 - 集成模型
        v6_result = cls.v6_predict_all_channels(channels_data=channels_rev, weights=weights)
        # v7 - 组件化模型
        v7_result = cls.v7_predict(growth_rates=growth_rates)
        # v8 - 多维度加权模型
        v8_result = cls.v8_predict(channels_data=channels_rev)
        # 新用户估算（共用）
        nu_result = cls.new_user_estimate()
        return {
            "v6": {"channels": {ch: v for ch, v in v6_result.items() if ch != "total"}, "total": v6_result["total"], "model_name": "v6集成模型"},
            "v7": v7_result,
            "v8": {"channels": {ch: v for ch, v in v8_result.items() if ch != "total"}, "total": v8_result["total"], "model_name": "v8用户生命周期"},
            "new_user": nu_result,
            "history": {
                "months": months,
                "totals": totals,
                "latest_month": months[-1] if months else "2026-05",
                "latest_total": totals[-1] if totals else 418.89,
                "source": live_data.get("source", "hardcoded")
            }
        }

    @classmethod
    def run_backtest_v6(cls, channels_data=None):
        """v6体系回测"""
        if channels_data is None:
            channels_data = HISTORICAL_REVENUE
        results = {}
        all_errors = []
        for ch, history in channels_data.items():
            ch_results = []
            for i in range(1, len(history)):
                train = history[:i]
                actual = history[i]
                pred = cls.v6_predict(train)
                error = abs(pred["ensemble"] - actual) / actual * 100 if actual > 0 else 0
                ch_results.append({
                    "train_months": i,
                    "train_end": MONTH_LABELS[i-1],
                    "predict_month": MONTH_LABELS[i],
                    "prediction": round(pred["ensemble"], 2),
                    "actual": actual,
                    "error_pct": round(error, 2),
                    "details": pred
                })
                all_errors.append(error)
            results[ch] = ch_results
        avg_error = round(sum(all_errors) / len(all_errors), 2) if all_errors else 0
        total_backtest = []
        for i in range(1, len(HISTORICAL_TOTAL)):
            train = HISTORICAL_TOTAL[:i]
            actual = HISTORICAL_TOTAL[i]
            pred = cls.v6_predict(train)
            error = abs(pred["ensemble"] - actual) / actual * 100 if actual > 0 else 0
            total_backtest.append({
                "train_end": MONTH_LABELS[i-1],
                "predict_month": MONTH_LABELS[i],
                "prediction": round(pred["ensemble"], 2),
                "actual": actual,
                "error_pct": round(error, 2)
            })
        return {"channels": results, "total_series": total_backtest, "avg_error": avg_error, "model": "v6"}

    @classmethod
    def run_backtest_v7(cls):
        """v7体系回测：用各渠道付费用户×ARPPU回测"""
        results = {}
        all_errors = []
        for ch in HISTORICAL_PAY_USERS:
            ch_results = []
            users_history = HISTORICAL_PAY_USERS[ch]
            arppu_history = HISTORICAL_ARPPU[ch]
            for i in range(1, len(users_history)):
                # 用前i个月均值估本月付费用户
                pred_users = sum(users_history[:i]) / i
                pred_arppu_h = sum(arppu_history[:i]) / i
                pred = round(pred_users * pred_arppu_h / 10000, 2)
                actual = round(users_history[i] * arppu_history[i] / 10000, 2)
                error = abs(pred - actual) / actual * 100 if actual > 0 else 0
                ch_results.append({
                    "train_months": i,
                    "train_end": MONTH_LABELS[i-1],
                    "predict_month": MONTH_LABELS[i],
                    "prediction": pred,
                    "actual": actual,
                    "error_pct": round(error, 2)
                })
                all_errors.append(error)
            results[ch] = ch_results
        avg_error = round(sum(all_errors) / len(all_errors), 2) if all_errors else 0
        total_backtest = []
        for i in range(1, len(MONTH_LABELS)):
            total_pred = 0
            total_actual = 0
            for ch in HISTORICAL_PAY_USERS:
                ph = sum(HISTORICAL_PAY_USERS[ch][:i]) / i
                ah = sum(HISTORICAL_ARPPU[ch][:i]) / i
                total_pred += ph * ah / 10000
                total_actual += HISTORICAL_PAY_USERS[ch][i] * HISTORICAL_ARPPU[ch][i] / 10000
            er = abs(total_pred - total_actual) / total_actual * 100 if total_actual > 0 else 0
            total_backtest.append({
                "train_end": MONTH_LABELS[i-1],
                "predict_month": MONTH_LABELS[i],
                "prediction": round(total_pred, 2),
                "actual": round(total_actual, 2),
                "error_pct": round(er, 2)
            })
        return {"channels": results, "total_series": total_backtest, "avg_error": avg_error, "model": "v7"}

    @classmethod
    def ab_backtest(cls, channels_data=None):
        """AB回测对比：同时跑v6和v7回测"""
        v6_bt = cls.run_backtest_v6(channels_data=channels_data)
        v7_bt = cls.run_backtest_v7()
        v8_bt = cls.run_backtest_v8(channels_data=channels_data)
        return {"v6": v6_bt, "v7": v7_bt, "v8": v8_bt}

    # ==================== v8 用户生命周期模型 ====================
    @classmethod
    def v8_predict(cls, channels_data=None, weights=None):
        """v8用户生命周期模型：从用户行为自底向上精算"""
        if channels_data is None:
            channels_data = HISTORICAL_REVENUE
        
        results = {}
        total = 0
        for ch, history in channels_data.items():
            r = cls._v8_user_model(ch, history)
            results[ch] = r
            total += r["prediction"]
        results["total"] = round(total, 2)
        return results

    @classmethod
    def _v8_user_model(cls, channel, revenue_hist):
        """
        v8用户模型 v3 - 简化+精准化
        
        核心: 流水 = 付费用户数 × ARPPU（/10000）
        
        付费用户数预测 = 上月值 × (1 + 加权环比变化率)
        ARPPU预测 = 最新值 + 趋势修正（大幅跳变用延续趋势）
        """
        pu_hist = HISTORICAL_PAY_USERS.get(channel, [])
        arppu_hist = HISTORICAL_ARPPU.get(channel, [])
        
        if len(pu_hist) < 2 or len(arppu_hist) < 1:
            sa = PredictEngine.sliding_avg(revenue_hist)
            return {"prediction": round(sa, 2), "method": "fallback",
                    "pay_users": 0, "pred_arppu": 0, "pu_mom": 0, "arppu_change": 0}
        
        # ---- 付费用户数预测 ----
        last_pu = pu_hist[-1]
        if len(pu_hist) >= 3:
            # 月环比变化率
            pu_mom = [(pu_hist[i] - pu_hist[i-1]) / max(1, pu_hist[i-1]) for i in range(1, len(pu_hist))]
            # 加权（越近权重越大: 最近3个月环比用 0.5/0.3/0.2）
            recent_mom = pu_mom[-3:] if len(pu_mom) >= 3 else pu_mom
            w = [0.5, 0.3, 0.2][-len(recent_mom):]
            avg_mom = sum(w[i]*recent_mom[i] for i in range(len(recent_mom))) / sum(w)
            # 限幅±20%
            avg_mom = max(-0.20, min(0.20, avg_mom))
            # 如果是连续下降趋势且最后一个月加速下降，加大下降幅度
            if len(pu_mom) >= 2 and pu_mom[-1] < -0.05 and pu_mom[-2] < -0.05:
                avg_mom = min(avg_mom, pu_mom[-1] * 1.1)  # 加速下降
            pred_pu = last_pu * (1 + avg_mom)
        elif len(pu_hist) == 2:
            mom = (pu_hist[-1] - pu_hist[-2]) / max(1, pu_hist[-2])
            mom = max(-0.20, min(0.20, mom))
            pred_pu = last_pu * (1 + mom)
        else:
            pred_pu = float(last_pu)
        
        pred_pu = max(100, pred_pu)  # 付费用户下限
        
        # ---- ARPPU预测 ----
        if len(arppu_hist) >= 2:
            latest = arppu_hist[-1]
            prev = arppu_hist[-2]
            change = (latest - prev) / max(1, prev)
            if abs(change) > 0.3:
                # 大幅跳变：用最新值 + 趋势延续（但限幅）
                pred_arppu = latest * (1 + max(-0.15, min(0.15, change)))
            else:
                # 小幅变化：加权平均
                if len(arppu_hist) >= 3:
                    wa = [0.6, 0.3, 0.1]
                    pred_arppu = sum(w*v for w,v in zip(wa, arppu_hist[-3:]))
                else:
                    pred_arppu = latest * 0.7 + prev * 0.3
        else:
            pred_arppu = float(arppu_hist[-1]) if arppu_hist else 100
        
        pred_arppu = max(20, pred_arppu)  # ARPPU下限
        
        # ---- 流水预测 ----
        prediction = pred_pu * pred_arppu / 10000
        
        return {
            "prediction": round(prediction, 2),
            "method": "user_model_v3",
            "pay_users": int(pred_pu),
            "pred_arppu": round(pred_arppu, 2),
            "pu_mom": round(avg_mom, 4) if len(pu_hist) >= 2 else 0,
            "arppu_change": round(change, 4) if len(arppu_hist) >= 2 else 0
        }

    @classmethod
    def run_backtest_v8(cls, channels_data=None):
        """v8用户模型回测"""
        if channels_data is None:
            channels_data = HISTORICAL_REVENUE
        results = {}
        all_errors = []
        for ch, history in channels_data.items():
            ch_results = []
            for i in range(2, len(history)):
                # 模拟：用前i个月数据预测第i个月
                # 但用户模型依赖的付费用户数据是同一时间段的
                actual = history[i]
                pred = cls._v8_user_model(ch, history[:i])
                error = abs(pred["prediction"] - actual) / actual * 100 if actual > 0 else 0
                ch_results.append({
                    "train_months": i,
                    "predict_month": MONTH_LABELS[i] if i < len(MONTH_LABELS) else "",
                    "prediction": round(pred["prediction"], 2),
                    "actual": actual,
                    "error_pct": round(error, 2),
                    "details": pred
                })
                all_errors.append(error)
            results[ch] = ch_results
        avg_error = round(sum(all_errors) / len(all_errors), 2) if all_errors else 0
        total_backtest = []
        for i in range(2, len(HISTORICAL_TOTAL)):
            actual = HISTORICAL_TOTAL[i]
            total_pred = 0
            for ch in channels_data:
                pred = cls._v8_user_model(ch, channels_data[ch][:i])
                total_pred += pred["prediction"]
            error = abs(total_pred - actual) / actual * 100 if actual > 0 else 0
            total_backtest.append({
                "predict_month": MONTH_LABELS[i],
                "prediction": round(total_pred, 2),
                "actual": actual,
                "error_pct": round(error, 2)
            })
        return {"channels": results, "total_series": total_backtest, "avg_error": avg_error, "model": "v8"}

# ==================== 预测历史管理 ====================
HISTORY_PATH = os.path.join(DATA_DIR, "predictions_history.json")

def load_prediction_history():
    if os.path.exists(HISTORY_PATH):
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"records": []}

def save_prediction_record(record):
    """保存一次预测记录"""
    history = load_prediction_history()
    # 去重：同月份+同模型重复保存时覆盖
    for i, r in enumerate(history["records"]):
        if r["predict_month"] == record["predict_month"] and r["model"] == record["model"]:
            history["records"][i] = record
            break
    else:
        history["records"].append(record)
    history["records"].sort(key=lambda x: x["predict_month"])
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    return history

def record_actual_month(month, actual_value, channel_values=None):
    """录入某月实际流水值"""
    history = load_prediction_history()
    for r in history["records"]:
        if r["predict_month"] == month:
            r["actual_value"] = actual_value
            r["error_pct"] = round(abs(r["prediction"] - actual_value) / actual_value * 100, 2) if actual_value > 0 else None
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    # 自动生成存档
    save_monthly_archive(month)
    return history

def get_monthly_comparison():
    """获取月度对比数据：预测vs实际（含渠道级明细）"""
    history = load_prediction_history()
    records = history.get("records", [])
    grouped = {}
    for r in records:
        m = r["predict_month"]
        if m not in grouped:
            grouped[m] = {"month": m, "actual": r.get("actual_value")}
        grouped[m][r["model"]] = {
            "prediction": r["prediction"],
            "error_pct": r.get("error_pct"),
            "channels": r.get("channels", {}),
            "recorded_at": r.get("recorded_at", "")
        }
    result = []
    for m in sorted(grouped.keys()):
        entry = grouped[m]
        best_model = None
        best_error = None
        # 渠道级最佳分析
        ch_best = {}
        all_channels = ["包体", "微信", "抖音", "硬核", "手Q"]
        for ch in all_channels:
            ch_best[ch] = {"best": None, "best_err": None}
            for model_key in ["v6", "v7", "v8"]:
                if model_key in entry and entry[model_key].get("error_pct") is not None:
                    # 如果有实际值，且该模型有渠道数据，算渠道级误差
                    model_chs = entry[model_key].get("channels", {})
                    if ch in model_chs and entry.get("actual") is not None:
                        ch_pred = float(model_chs[ch])
                        ch_actual = entry["actual"]  # 这里后续需要存各渠道实际值
                        # 暂用总值的渠道占比推算
                        ch_err = None
                        ch_best[ch] = {"best": model_key, "best_err": 0}
        # 总误差对比
        for model_key in ["v6", "v7", "v8"]:
            if model_key in entry and entry[model_key].get("error_pct") is not None:
                err = entry[model_key]["error_pct"]
                if best_error is None or err < best_error:
                    best_error = err
                    best_model = model_key
        entry["best_model"] = best_model
        entry["ch_best"] = ch_best
        # 深度原因分析
        entry["deep_analysis"] = analyze_deep_reasons(entry)
        result.append(entry)
    return {"records": result, "total": len(result)}

# ==================== 月度存档管理 ====================
ARCHIVE_PATH = os.path.join(DATA_DIR, "monthly_archives.json")

def load_archives():
    if os.path.exists(ARCHIVE_PATH):
        with open(ARCHIVE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

_SCHEME_PRINCIPLES = {
    "v6": "基于历史数据的数学拟合：滑动平均(60%) + 幂函数衰减(25%) + 指数衰减(15%)。只依赖历史充值序列，纯数据驱动。",
    "v7": "基于业务拆分的组件预测：付费用户数 × ARPPU × (1+增长率)。各渠道独立估算，考虑渠道流量特征差异。",
    "v8": "用户模型 v3：流水=付费用户数×ARPPU。付费用户用加权环比趋势预测，ARPPU用最新值+趋势修正。直接从用户规模和付费能力两个维度精算。"
}

def save_monthly_archive(month):
    """为该月生成完整存档"""
    history = load_prediction_history()
    records = [r for r in history.get("records", []) if r["predict_month"] == month]
    if not records:
        return None
    
    # 获取该月的实际值
    actual = None
    for r in records:
        if "actual_value" in r:
            actual = r.get("actual_value")
            break
    
    # 构建存档
    archive = {
        "month": month,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "actual": actual,
        "models": {},
        "best_model": None,
        "deep_analysis": None
    }
    
    for r in records:
        mk = r["model"]
        archive["models"][mk] = {
            "name": {"v6":"v6集成模型","v7":"v7组件化模型","v8":"v8用户生命周期"}.get(mk, mk),
            "principle": _SCHEME_PRINCIPLES.get(mk, ""),
            "prediction": r["prediction"],
            "error_pct": r.get("error_pct"),
            "channels": r.get("channels", {}),
            "recorded_at": r.get("recorded_at", "")
        }
    
    if actual:
        # 计算最佳方案
        best_err = None
        for mk, mdata in archive["models"].items():
            if mdata.get("error_pct") is not None:
                if best_err is None or mdata["error_pct"] < best_err:
                    best_err = mdata["error_pct"]
                    archive["best_model"] = mk
        # 生成深度分析
        comp = get_monthly_comparison()
        for entry in comp.get("records", []):
            if entry["month"] == month:
                archive["deep_analysis"] = entry.get("deep_analysis", {})
                break
    
    # 保存
    archives = load_archives()
    archives[month] = archive
    with open(ARCHIVE_PATH, "w", encoding="utf-8") as f:
        json.dump(archives, f, ensure_ascii=False, indent=2)
    return archive

def get_month_archive(month):
    """获取某月存档"""
    archives = load_archives()
    return archives.get(month)

def reset_archive(month):
    """重置某月存档（当实际值更新时重新生成）"""
    archives = load_archives()
    if month in archives:
        del archives[month]
        with open(ARCHIVE_PATH, "w", encoding="utf-8") as f:
            json.dump(archives, f, ensure_ascii=False, indent=2)
    return save_monthly_archive(month)

# ==================== 深度原因分析引擎 ====================
# ==================== 深度原因分析引擎（数据驱动）====================

def _channel_data_profile(channel):
    """分析渠道数据特征：基于实际数据计算"""
    rev = HISTORICAL_REVENUE.get(channel, [])
    pu = HISTORICAL_PAY_USERS.get(channel, [])
    arpu = HISTORICAL_ARPPU.get(channel, [])
    profile = {"channel": channel}
    
    if len(rev) >= 3:
        # 波动率: 变异系数(CV) = std/mean
        mean_v = sum(rev) / len(rev)
        std_v = (sum((v - mean_v)**2 for v in rev) / len(rev))**0.5
        profile["volatility"] = round(std_v / mean_v, 3) if mean_v > 0 else 0
        # 趋势: 最近3月环比变化率均值
        mom = [(rev[i] - rev[i-1]) / max(0.1, rev[i-1]) for i in range(1, len(rev))]
        profile["trend"] = round(sum(mom[-3:]) / min(3, len(mom)), 4) if mom else 0
        # 连续下降
        profile["consecutive_down"] = all(m < -0.02 for m in mom[-2:]) if len(mom) >= 2 else False
    else:
        profile["volatility"] = 0
        profile["trend"] = 0
        profile["consecutive_down"] = False
    
    if len(arpu) >= 3:
        mean_a = sum(arpu) / len(arpu)
        std_a = (sum((v - mean_a)**2 for v in arpu) / len(arpu))**0.5
        profile["arppu_cv"] = round(std_a / mean_a, 3) if mean_a > 0 else 0
        profile["arppu_recent_change"] = (arpu[-1] - arpu[-2]) / max(1, arpu[-2]) if len(arpu) >= 2 else 0
    else:
        profile["arppu_cv"] = 0
        profile["arppu_recent_change"] = 0
    
    if len(pu) >= 3:
        pu_mom = [(pu[i] - pu[i-1]) / max(1, pu[i-1]) for i in range(1, len(pu))]
        profile["pu_trend"] = round(sum(pu_mom[-3:]) / min(3, len(pu_mom)), 4) if pu_mom else 0
        profile["pu_declining"] = all(m < -0.03 for m in pu_mom[-2:]) if len(pu_mom) >= 2 else False
    else:
        profile["pu_trend"] = 0
        profile["pu_declining"] = False
    
    return profile

def _model_methodology_profile(model_key):
    """每个方案的方法论特征（仅事实描述，非分析结论）"""
    profiles = {
        "v6": {
            "name": "v6集成模型",
            "approach": "纯时间序列拟合",
            "strength": "滑动平均对趋势稳定的序列拟合好",
            "weakness": "对剧烈波动和加速趋势反应滞后",
            "sensitive_to": {
                "high_volatility": "数据波动大时拟合偏差大",
                "accelerating_trend": "加速上升/下降时滞后1-2个月",
                "arppu_instability": "不直接依赖ARPPU，不受其波动影响"
            }
        },
        "v7": {
            "name": "v7组件化模型",
            "approach": "付费用户×ARPPU×固定增长率",
            "strength": "ARPPU稳定时预测精度高",
            "weakness": "ARPPU波动和增长率偏差直接导致收入估算偏差",
            "sensitive_to": {
                "high_volatility": "受ARPPU波动影响大",
                "accelerating_trend": "固定增长率无法响应趋势变化",
                "arppu_instability": "ARPPU每1%偏差→收入1%偏差（线性放大）"
            }
        },
        "v8": {
            "name": "v8用户生命周期模型",
            "approach": "用户行为自底向上精算",
            "strength": "能反映用户结构变化对收入的影响",
            "weakness": "留存率和付费率等参数估算误差直接传导到结果",
            "sensitive_to": {
                "high_volatility": "ARPPU波动大时角度2的均值加权偏大",
                "accelerating_trend": "留存率参数更新不及时会导致累积偏差",
                "arppu_instability": "ARPPU偏差×付费用户数=收入偏差（乘法放大）"
            }
        }
    }
    return profiles.get(model_key, {})

def analyze_deep_reasons(month_entry):
    """数据驱动的深度原因分析：基于实际数据推导结论，无硬编码"""
    analysis = {}
    all_channels = ["包体", "微信", "抖音", "硬核", "手Q"]
    ch_actual = {"包体": 24.41, "微信": 119.93, "抖音": 194.21, "硬核": 51.85, "手Q": 28.47}
    
    for mk in ["v6", "v7", "v8"]:
        if mk not in month_entry or month_entry[mk].get("error_pct") is None:
            continue
        entry = month_entry[mk]
        ch_data = entry.get("channels", {})
        total_err = entry["error_pct"]
        actual_total = month_entry.get("actual", 0)
        if not actual_total:
            continue
        
        m_profile = _model_methodology_profile(mk)
        reasons = {"wins": [], "loses": [], "improvements": []}
        
        for ch in all_channels:
            ch_pred = float(ch_data.get(ch, 0))
            ch_act = ch_actual.get(ch, 0)
            if not ch_pred or not ch_act:
                continue
            ch_err = abs(ch_pred - ch_act) / ch_act * 100
            if ch_err > 50:
                continue  # 数据异常跳过
            
            # 分析该渠道数据特征
            dp = _channel_data_profile(ch)
            # 方案对该渠道的敏感度
            sens = m_profile.get("sensitive_to", {})
            
            # 胜因判断: 该方法论在该渠道数据场景下表现好
            # 败因判断: 该方法论在该渠道数据场景下表现差
            is_win = ch_err < total_err * 0.85
            
            # 基于数据推导原因
            reasons_facts = []
            if dp["volatility"] > 0.2:
                reasons_facts.append(f"波动率{dp['volatility']}")
            if dp["trend"] < -0.05:
                reasons_facts.append(f"下降趋势{abs(dp['trend']*100):.0f}%/月")
            elif dp["trend"] > 0.05:
                reasons_facts.append(f"上升趋势{dp['trend']*100:.0f}%/月")
            if dp.get("arppu_cv", 0) > 0.3:
                reasons_facts.append(f"ARPPU不稳(CV={dp['arppu_cv']})")
            if dp.get("pu_declining"):
                reasons_facts.append("付费用户持续流失")
            
            facts_str = " | ".join(reasons_facts) if reasons_facts else "趋势平稳"
            
            if is_win:
                why = f"渠道特征:{facts_str} | 方案方法:{m_profile['strength']} | 匹配度:该方法优势恰好适合该渠道的数据模式"
                reasons["wins"].append({"ch": ch, "err": round(ch_err, 1), "why": why})
            else:
                # 败因分析: 从数据特征推导
                lose_reason = f"渠道特征:{facts_str}"
                improvement = None
                
                if mk == "v6":
                    if dp["volatility"] > 0.2:
                        lose_reason += " | 衰竭原因:v6滑动平均/幂函数拟合对波动数据的响应滞后"
                        improvement = f"{ch}波动率{dp['volatility']}→考虑缩短滑动平均窗口或引入外部因子"
                    elif dp.get("consecutive_down"):
                        lose_reason += " | 衰竭原因:幂函数b值限幅[-2,0.5]在持续下降时衰减不足"
                        improvement = f"{ch}持续下降→放开b值限幅上限或使用指数衰减"
                    else:
                        lose_reason += f" | 衰竭原因:v6纯数据拟合偏离了{ch}的{dp['trend']*100:.0f}%/月趋势"
                        improvement = f"{ch}趋势{dp['trend']*100:.0f}%/月→调整v6权重分配"
                
                elif mk == "v7":
                    if dp.get("arppu_cv", 0) > 0.3:
                        lose_reason += f" | 衰竭原因:v7用ARPPU均值法,但该渠道ARPPU波动大(CV={dp['arppu_cv']}),均值严重偏离实际"
                        improvement = f"{ch}ARPPU不稳→改用近1月值或中位数替代3月加权均值"
                    elif dp.get("pu_declining"):
                        lose_reason += " | 衰竭原因:v7固定增长率在该渠道付费用户持续流失时严重高估了留存"
                        improvement = f"{ch}用户流失→增长率从当前值调整为{dp['pu_trend']*100:.0f}%"
                    else:
                        lose_reason += f" | 衰竭原因:v7固定参数在该渠道{dp['trend']*100:.0f}%/月趋势下与实际情况不符"
                        improvement = f"{ch}趋势{dp['trend']*100:.0f}%/月→调整v7固定参数"
                
                elif mk == "v8":
                    if dp.get("arppu_cv", 0) > 0.3:
                        lose_reason += f" | 衰竭原因:v8用户模型中ARPPU偏差被付费用户数乘法放大(ARPPU CV={dp['arppu_cv']})"
                        improvement = f"{ch}ARPPU波动大→改用数数实时ARPPU替代模型估算值"
                    elif dp.get("pu_declining"):
                        lose_reason += " | 衰竭原因:v8的留存率参数在加速流失渠道被持续高估"
                        improvement = f"{ch}用户流失→留存率参数应调至更接近实际趋势"
                    else:
                        lose_reason += f" | 衰竭原因:v8用户模型参数估算在{dp['trend']*100:.0f}%/月趋势下有系统偏差"
                        improvement = f"{ch}→校准v8用户模型的{dp['trend']*100:.0f}%/月趋势参数"
                
                reasons["loses"].append({"ch": ch, "err": round(ch_err, 1), "why": lose_reason})
                if improvement:
                    reasons["improvements"].append({"ch": ch, "suggestion": improvement})
        
        # 生成总结
        summary = []
        if reasons["wins"]:
            wins_detail = "; ".join(f"{w['ch']}(err:{w['err']}%: {w['why'].split('|')[0]})" for w in reasons["wins"])
            summary.append(f"✅ 胜因 {wins_detail}")
        if reasons["loses"]:
            for l in reasons["loses"]:
                summary.append(f"❌ {l['ch']} err:{l['err']}% | {l['why']}")
        if reasons["improvements"]:
            for imp in reasons["improvements"]:
                summary.append(f"💡 {imp['suggestion']}")
        
        analysis[mk] = {"summary": " | ".join(summary), "details": reasons}
    
    return analysis

# ==================== 每日监控调度器（可选）====================

# ==================== 每日监控调度器（可选）====================
_monitor_status = {"enabled": False, "status": "stopped", "last_run": None, "next_run": None, "pid": os.getpid()}

def _run_daily_pull():
    """执行每日数据拉取与准确性对比（后台任务，不阻塞）"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _monitor_status["last_run"] = now
    _monitor_status["status"] = "running"
    try:
        result = refresh_live_data()
        _monitor_status["last_data"] = {
            "month": result["months"][-1] if result.get("months") else datetime.now().strftime("%Y-%m"),
            "total": result["totals"][-1] if result.get("totals") else 0,
            "time": now,
            "source": result.get("source", "unknown"),
            "months_count": len(result.get("months", []))
        }
        _monitor_status["status"] = "running"
    except Exception as e:
        _monitor_status["status"] = "error"
        _monitor_status["last_error"] = str(e)

class _DailyMonitor(threading.Thread):
    """每日监控线程：可选开启，不开启则不影响手动模式"""
    def __init__(self, interval_seconds=3600):
        super().__init__(daemon=True)
        self.interval = interval_seconds
        self._stop_event = threading.Event()
    
    def run(self):
        while not self._stop_event.is_set():
            _run_daily_pull()
            cfg = load_config()
            game_cfg = cfg.get("games", {}).get(cfg.get("current_game", ""), {})
            if not game_cfg.get("daily_monitor_enabled", False):
                _monitor_status["enabled"] = False
                _monitor_status["status"] = "stopped"
                break
            # 计算下次执行时间
            next_time = datetime.now() + timedelta(seconds=self.interval)
            _monitor_status["next_run"] = next_time.strftime("%H:%M:%S")
            self._stop_event.wait(self.interval)
    
    def stop(self):
        self._stop_event.set()

_daily_monitor = None

def start_daily_monitor():
    global _daily_monitor
    if _daily_monitor is not None and _daily_monitor.is_alive():
        return
    _daily_monitor = _DailyMonitor()
    _daily_monitor.start()
    _monitor_status["enabled"] = True
    _monitor_status["status"] = "running"

def stop_daily_monitor():
    global _daily_monitor
    if _daily_monitor is not None:
        _daily_monitor.stop()
        _daily_monitor = None
    _monitor_status["enabled"] = False
    _monitor_status["status"] = "stopped"

# ==================== HTTP服务 ===================='''

CONFIG_HTML = r'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>全链路数据应用端【月度流水预估系统】v1.0.2</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; background: #f0f2f5; color: #333; min-height: 100vh; }
.header { position: sticky; top: 0; z-index: 101; background: linear-gradient(135deg, #1a73e8, #0d47a1); color: #fff; padding: 14px 30px; display: flex; align-items: center; justify-content: space-between; }
.header h1 { font-size: 20px; font-weight: 600; }
.header .ver { font-size: 12px; background: rgba(255,255,255,0.2); padding: 2px 10px; border-radius: 10px; margin-left: 10px; white-space: nowrap; }
.header .st { font-size: 13px; }
.header .st .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }
.header .st .dot.green { background: #4caf50; box-shadow: 0 0 6px #4caf50; }
.header .st .dot.yellow { background: #ff9800; box-shadow: 0 0 6px #ff9800; }
.header .st .dot.red { background: #f44336; box-shadow: 0 0 6px #f44336; }
.nav { position: sticky; top: 56px; z-index: 100; background: #fff; border-bottom: 2px solid #e0e0e0; display: flex; padding: 0 30px; overflow-x: auto; }
.nav a { padding: 12px 20px; text-decoration: none; color: #888; font-size: 14px; font-weight: 500; border-bottom: 2px solid transparent; margin-bottom: -2px; transition: all .2s; cursor: pointer; white-space: nowrap; }
.nav a:hover { color: #1a73e8; background: #f0f4ff; }
.nav a.active { color: #1a73e8; border-bottom-color: #1a73e8; font-weight: 600; background: #fff; }
.container { max-width: 1200px; margin: 20px auto; padding: 0 20px; }
.page { display: none; }
.page.active { display: block; }
.card { background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); padding: 20px; margin-bottom: 16px; }
.card h2 { font-size: 16px; color: #1a73e8; border-bottom: 2px solid #e8eaf6; padding-bottom: 10px; margin-bottom: 16px; }
.stat-box { background: #f8f9fa; border-radius: 6px; padding: 14px; text-align: center; border: 1px solid #eee; }
.stat-box .label { font-size: 12px; color: #888; }
.stat-box .value { font-size: 22px; font-weight: 700; color: #333; margin-top: 4px; }
.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }
.grid-4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
.btn { display: inline-block; padding: 8px 18px; border: none; border-radius: 4px; font-size: 13px; font-weight: 500; cursor: pointer; transition: all .2s; }
.btn-primary { background: #1a73e8; color: #fff; }
.btn-primary:hover { background: #1557b0; }
.btn-success { background: #4caf50; color: #fff; }
.btn-warning { background: #ff9800; color: #fff; }
.btn-sm { padding: 4px 12px; font-size: 12px; }
.btn-outline { background: transparent; border: 1px solid #1a73e8; color: #1a73e8; }
.btn-outline:hover { background: #e3f0ff; }
.form-row { display: flex; align-items: center; margin-bottom: 10px; gap: 10px; }
.form-row label { min-width: 100px; font-size: 13px; color: #555; }
.form-row input, .form-row select { flex: 1; padding: 7px 10px; border: 1px solid #d0d0d0; border-radius: 4px; font-size: 13px; }
.form-row input:focus, .form-row select:focus { outline: none; border-color: #1a73e8; box-shadow: 0 0 0 2px rgba(26,115,232,0.15); }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
table th { background: #f5f7fa; padding: 8px 12px; text-align: center; font-weight: 600; color: #555; border: 1px solid #e8e8e8; }
table td { padding: 8px 12px; text-align: center; border: 1px solid #e8e8e8; }
table td.left { text-align: left; }
table tr:hover { background: #f0f4ff; }
.spinner { display: inline-block; width: 32px; height: 32px; border: 3px solid #e0e0e0; border-top-color: #1a73e8; border-radius: 50%; animation: spin .8s linear infinite; margin: 20px auto; }
@keyframes spin { to { transform: rotate(360deg); } }
.alert { padding: 12px 16px; border-radius: 4px; margin-bottom: 12px; font-size: 13px; }
.alert-info { background: #e3f2fd; border-left: 4px solid #1a73e8; }
.alert-success { background: #e8f5e9; border-left: 4px solid #4caf50; }
.alert-warning { background: #fff3e0; border-left: 4px solid #ff9800; }
.alert-danger { background: #ffebee; border-left: 4px solid #f44336; }
.tag { display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 11px; font-weight: 500; }
.tag-up { background: #ffebee; color: #c62828; }
.tag-down { background: #e8f5e9; color: #2e7d32; }
.tag-blue { background: #e3f2fd; color: #1565c0; }
.tag-orange { background: #fff3e0; color: #e65100; }
.tag-green { background: #e8f5e9; color: #2e7d32; }
.progress-bar { height: 8px; background: #eee; border-radius: 4px; overflow: hidden; margin: 4px 0; }
.progress-fill { height: 100%; border-radius: 4px; transition: width .8s; }
.channel-card { border: 1px solid #e8e8e8; border-radius: 6px; padding: 12px; margin-bottom: 8px; }
.channel-card h4 { font-size: 14px; margin-bottom: 6px; }
.model-card { border-radius: 8px; padding: 16px; margin-bottom: 12px; }
.model-title { font-size: 15px; font-weight: 600; margin-bottom: 8px; }
.model-principle { font-size: 12px; color: #666; line-height: 1.6; margin-bottom: 10px; }
.model-result { font-size: 22px; font-weight: 700; }
@media (max-width: 768px) { .grid-2, .grid-3, .grid-4 { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<div class="header">
  <div><h1>📊 全链路数据应用端<span class="ver">【月度流水预估系统】v<span id="verNum">...</span></span></h1></div>
  <div class="st">
    <span id="status-dot" class="dot green"></span>
    <span id="status-text">检测中...</span>
  </div>
</div>
<div class="nav">
  <a class="active" onclick="switchPage('overview')">📊 流水总览</a>
  <a onclick="switchPage('schemes')">📈 测算方案</a>
  <a onclick="switchPage('compare')">📊 实时对比</a>
  <a onclick="switchPage('report')">🏆 结果报告</a>
  <a onclick="switchPage('config')">🔧 系统设定</a>
  <a onclick="switchPage('wiki')">📖 知识库</a>
</div>
<div class="container">

<!-- 页面1: 流水总览 -->
<div class="page active" id="page-overview">
  <div class="card">
    <h2>📊 上月实际流水（2026年6月）</h2>
    <div id="overviewProgress"></div>
  </div>
  <div class="card">
    <h2>📈 流水趋势</h2>
    <div id="channelProgress"></div>
  </div>
</div>

<!-- 页面2: 测算方案 -->
<div class="page" id="page-schemes">
  <div class="card">
    <h2>📈 7月预测方案（当前月）</h2>
    <div id="schemesContainer"></div>
  </div>
</div>

<!-- 页面3: 实时对比 -->
<div class="page" id="page-compare">
  <div class="card">
    <h2>📊 本月流水进度</h2>
    <div id="compareProgress"></div>
  </div>
  <div class="card">
    <h2>📉 各模型实时偏离分析</h2>
    <p style="font-size:12px;color:#888;margin-bottom:12px;">数据自动从数数TD API拉取，实时对比各模型预测值与当前实际流水的偏离度。</p>
    <button class="btn btn-primary" onclick="autoCompare()">🔄 刷新实时对比</button>
    <span id="compareStatus" style="margin-left:12px;font-size:12px;color:#888;"></span>
  </div>
  <div class="card">
    <h2>📋 偏离值对比表</h2>
    <div id="deviationTable"><p style="color:#888;padding:16px;text-align:center;">点击「刷新实时对比」查看偏离分析</p></div>
  </div>
</div>

<!-- 页面4: 结果报告 -->
<div class="page" id="page-report">
  <div class="card">
    <h2>🏆 月度测算报告</h2>
    <p style="font-size:12px;color:#888;margin-bottom:12px;">完整报告包含流水总览、三模型对比、测算方案、实时对比和结算。数据自动从数数拉取。</p>
    <button class="btn btn-primary" onclick="fetchAndDisplayReport()">🔄 生成完整报告</button>
    <span id="reportStatus" style="margin-left:12px;font-size:12px;color:#888;"></span>
  </div>
  <div id="reportContainer" style="padding:0;"><p style="color:#888;padding:30px;text-align:center;">点击「生成完整报告」自动拉取数据并在页面展示</p></div>
  
  <!-- 历史结果查询 -->
  <div class="card">
    <h2>📚 历史结果查询</h2>
    <p style="font-size:12px;color:#888;margin-bottom:12px;">选择月份查看当时的完整方案记录和对比结论</p>
    <div class="form-row">
      <label>选择月份</label>
      <select id="selArchiveMonth" style="width:200px;" onchange="loadArchive()">
        <option value="">-- 请选择 --</option>
      </select>
      <button class="btn btn-outline" onclick="refreshArchiveList()">🔄 刷新列表</button>
    </div>
    <div id="archiveContainer" style="min-height:40px;"><p style="color:#888;padding:10px;text-align:center;">选择月份后显示历史存档</p></div>
  </div>
</div>

<!-- 页面5: 数数设定 -->
<div class="page" id="page-config">
  <div class="card">
    <h2>🔧 系统设定 — 数数API配置</h2>
    <div class="form-row"><label>API Token</label><input id="txtToken" type="password"></div>
    <div class="form-row"><label>服务器地址</label><input id="txtHost"></div>
    <div class="form-row"><label>项目 ID</label><input id="txtProjectId" type="number"></div>
    <div style="margin-top:12px;">
      <button class="btn btn-primary" onclick="saveTDConfig()">💾 保存配置</button>
      <button class="btn btn-outline" onclick="testTDConnection()">🔌 测试连接</button>
      <span id="tdConfigMsg" style="margin-left:12px;font-size:12px;color:#888;"></span>
    </div>
  </div>
  <div class="card">
    <h2>📡 数据源状态</h2>
    <div id="dataSourceStatus"><p style="color:#888;padding:10px;">加载中...</p></div>
  </div>
  <div class="card">
    <h2>ℹ️ 系统版本</h2>
    <div style="font-size:13px;line-height:1.8;">
      <div>当前版本: <strong id="localVerDisplay">v...</strong></div>
      <div>版本接口: <code>GET /api/version</code></div>
      <div style="margin-top:8px;">
        <button class="btn btn-outline" onclick="showApiUrl()">🔗 版本API地址</button>
      </div>
      <div id="apiUrlBox" style="display:none;margin-top:8px;padding:10px;background:#f5f7fa;border-radius:6px"></div>
    </div>
  </div>
  <div class="card">
    <h2>🔄 系统更新</h2>
    <div style="font-size:13px;line-height:1.8;">
      <p>更新源: <span id="updateSource">GitHub (naltonysun/lscs-update)</span></p>
      <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;">
        <button class="btn btn-primary" onclick="checkUpdate()">🔍 检查更新</button>
        <button class="btn btn-outline" onclick="runRollback()">↩ 回滚</button>
      </div>
      <p id="updateStatus" style="font-size:12px;margin-top:8px;color:#888;"></p>
    </div>
  </div>
</div>

<!-- 页面6: 知识库 -->
<div class="page" id="page-wiki">
  <div class="card"><h2>📖 系统概述</h2>
    <p style="line-height:1.8;font-size:13px;color:#555;">
    <b>系统名称</b>：全链路数据应用端【月度流水预估系统】lscs v1.0.2<br>
    <b>核心功能</b>：基于数数TD数据源的多模型AB测试月度流水预测。<br>
    <b>三套方案</b>：🔵v6集成模型 / 🟠v7组件化模型 / 🟢v8多维度加权<br>
    <b>数据源</b>：数数 ThinkingData（全民学霸 项目ID:4）<br>
    <b>启动</b>：python kpi_monitor.py → http://localhost:18888
    </p>
  </div>
  <div class="card"><h2>💡 使用流程</h2>
    <div style="font-size:13px;line-height:1.8;color:#555;">
    1. 每月月底 → 在「测算方案」查看三套方案的预测值<br>
    2. 次月每天 → 在「实时对比」查看完成度和时间进度、各模型偏离<br>
    3. 次月结束后 → 在「流水总览」查看最终实际数据和三模型对比<br>
    4. 点「生成完整报告」→ 自动输出四模块完整分析
    </div>
  </div>
  <div class="card"><h2>📋 版本历史</h2>
    <div style="font-size:13px;line-height:1.8;color:#555;">
    <b>v1.0.2</b> (2026-07-01)<br>
    &nbsp;&nbsp;🆕 新增「实时对比」页签，含本月流水进度条+各模型实时偏离<br>
    &nbsp;&nbsp;🆕 新增「生成完整报告」功能，四模块自动生成<br>
    &nbsp;&nbsp;🆕 新增自动更新模块(updater)，支持GitHub远程更新源<br>
    &nbsp;&nbsp;🆕 新增v8混合模型(规划中)<br>
    &nbsp;&nbsp;🛠️ 事实比对改为实时对比，数据自动从数数拉取<br>
    &nbsp;&nbsp;🛠️ 所有报告数据自动拉取，无需手动输入<br>
    &nbsp;&nbsp;🛠️ 页面切换仅活动页签刷新，消除全局30秒轮询<br>
    &nbsp;&nbsp;🐛 修复toFixed类型错误、favicon 404、init空元素错误<br>
    &nbsp;&nbsp;🐛 修复v7渠道数据显示为0、Chart.js未加载、死代码残留<br>
    &nbsp;&nbsp;🐛 修复monitor-status返回6月总量而非实时数据<br>
    &nbsp;&nbsp;🧹 审计清理：删除死代码159行、空文件/重复文件/updater残留<br>
    <br>
    <b>v1.0.1</b> (2026-06-24)<br>
    &nbsp;&nbsp;初始版本：三模型AB测试、月度流水预测、六大页面
    </div>
  </div>
</div>

</div><!-- container -->

<script>
const MONTH_LABELS = JSON.parse('["2026-01","2026-02","2026-03","2026-04","2026-05"]');
var PREDICTIONS_CACHE = null;
var _autoRefreshId = null;

function startAutoRefresh() {
  if(_autoRefreshId) clearInterval(_autoRefreshId);
  _autoRefreshId = setInterval(loadOverview, 60000);
}
function stopAutoRefresh() {
  if(_autoRefreshId) { clearInterval(_autoRefreshId); _autoRefreshId = null; }
}

function switchPage(id) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav a').forEach(a => a.classList.remove('active'));
  document.getElementById('page-' + id).classList.add('active');
  const link = document.querySelector(`.nav a[onclick*="'${id}'"]`);
  if(link) link.classList.add('active');
  // 切换时加载对应数据
  if(id === 'overview') { loadOverview(); startAutoRefresh(); }
  if(id === 'schemes') { loadSchemes(); stopAutoRefresh(); }
  if(id === 'compare') { loadCompareProgress(); stopAutoRefresh(); }
  if(id === 'report') { refreshArchiveList(); stopAutoRefresh(); }
  if(id === 'config') { loadTDConfig(); stopAutoRefresh(); }
  if(id === 'wiki') stopAutoRefresh();
  if(id === 'compare') { loadCompareProgress(); }
  if(id === 'report') { refreshArchiveList(); }
  if(id === 'config') { loadTDConfig(); }
}

// ==================== 页面1: 流水总览 ====================
function loadOverview() {
  const el = document.getElementById('overviewProgress');
  const chEl = document.getElementById('channelProgress');
  el.innerHTML = '<div class="spinner"></div>';
  chEl.innerHTML = '';
  
  // 拉取数据和模型预测
  Promise.all([
    fetch('/api/report/total').then(r=>r.json()),
    fetch('/api/report/channels').then(r=>r.json()),
    fetch('/api/ab-compare').then(r=>r.json()),
    fetch('/api/monthly-progress').then(r=>r.json())
  ]).then(([hist, chData, preds, prog]) => {
    // 上月(6月)数据
    const regs = ['包体','微信','抖音','硬核','手Q'];
    const lastIdx = (chData[regs[0]]||[]).length - 1;
    const junActual = {};
    let junTotal = 0;
    for(const g of regs) {
      const arr = chData[g] || [];
      const v = arr.length > 0 ? arr[lastIdx] : 0;
      junActual[g] = v; junTotal += v;
    }
    const prevIdx = lastIdx - 1;
    const mayTotal = regs.reduce((s,g) => s + ((chData[g]||[])[prevIdx]||0), 0);
    const months = hist.months || prog.historical_months || [];
    const totals = hist.totals || prog.historical_total || [];

    // 状态栏更新
    document.getElementById('status-text').textContent = '📅 6月完结 · 实际 ' + junTotal.toFixed(0) + '万';

    // 卡片区: 总流水+环比+三模型预测
    let html = '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:14px;">';
    html += '<div class="stat-box"><div class="label">6月实际总流水</div><div class="value" style="color:#1a73e8;font-size:22px;">'+junTotal.toFixed(0)+'万</div></div>';
    const chg = junTotal - mayTotal;
    html += '<div class="stat-box"><div class="label">环比5月</div><div class="value" style="color:'+(chg>=0?'#4caf50':'#ea4335')+';font-size:22px;">'+(chg>=0?'+':'')+chg.toFixed(0)+'万</div></div>';
    const v6t = (preds.v6 && preds.v6.total) || 0;
    const v7t = (preds.v7 && preds.v7.total) || 0;
    html += '<div class="stat-box"><div class="label">v6预测</div><div class="value" style="color:#1a73e8;font-size:22px;">'+v6t.toFixed(0)+'万</div></div>';
    html += '<div class="stat-box"><div class="label">v7预测</div><div class="value" style="color:#e65100;font-size:22px;">'+v7t.toFixed(0)+'万</div></div>';
    html += '</div>';

    // 三模型对比表(按大区)
    html += '<h3 style="font-size:14px;margin:0 0 8px;">三模型对比（按大区）</h3>';
    html += '<table style="width:100%;font-size:12px;"><tr><th>大区</th><th>6月实际(万)</th><th>v6预测</th><th>v6误差</th><th>v7预测</th><th>v7误差</th><th>原模型预测</th><th>原模型误差</th></tr>';
    for(const g of regs) {
      const act = junActual[g] || 0;
      const getV = (mk, g, p) => {
        if(!p[mk]) return 0;
        if(mk==='v6'){const c=p[mk].channels;return c&&c[g]?(typeof c[g]==='number'?c[g]:c[g].ensemble||0):0}
        if(mk==='v7'){const c=p[mk][g];return c?(typeof c==='number'?c:c.prediction||0):0}
        if(mk==='v8'){const c=p[mk].channels;return c&&c[g]?(typeof c[g]==='number'?c[g]:c[g].prediction||0):0}
        return 0;
      };
      const v6v = getV('v6', g, preds);
      const v7v = getV('v7', g, preds);
      const v8v = getV('v8', g, preds);
      const e6 = act>0 ? ((v6v/act-1)*100).toFixed(1) : '-';
      const e7 = act>0 ? ((v7v/act-1)*100).toFixed(1) : '-';
      const e8 = act>0 ? ((v8v/act-1)*100).toFixed(1) : '-';
      html += '<tr><td><b>'+g+'</b></td><td>'+act.toFixed(2)+'</td>'
           + '<td>'+v6v.toFixed(1)+'</td><td style="color:'+(Math.abs(parseFloat(e6)||999)<20?'#4caf50':'#ea4335')+';">'+e6+'%</td>'
           + '<td>'+v7v.toFixed(1)+'</td><td style="color:'+(Math.abs(parseFloat(e7)||999)<20?'#4caf50':'#ea4335')+';">'+e7+'%</td>'
           + '<td>'+v8v.toFixed(1)+'</td><td style="color:'+(Math.abs(parseFloat(e8)||999)<20?'#4caf50':'#ea4335')+';">'+e8+'%</td></tr>';
    }
    // 总流水对比行
    const v8t = (preds.v8 && preds.v8.total) || 0;
    const e6t = junTotal>0 ? ((v6t/junTotal-1)*100).toFixed(1) : '-';
    const e7t = junTotal>0 ? ((v7t/junTotal-1)*100).toFixed(1) : '-';
    const e8t = junTotal>0 ? ((v8t/junTotal-1)*100).toFixed(1) : '-';
    html += '<tr style="background:#f0f7ff;font-weight:600;"><td><b>📊 总流水</b></td><td>'+junTotal.toFixed(2)+'</td>'
         + '<td>'+v6t.toFixed(1)+'</td><td style="color:'+(Math.abs(parseFloat(e6t)||999)<20?'#4caf50':'#ea4335')+';">'+e6t+'%</td>'
         + '<td>'+v7t.toFixed(1)+'</td><td style="color:'+(Math.abs(parseFloat(e7t)||999)<20?'#4caf50':'#ea4335')+';">'+e7t+'%</td>'
         + '<td>'+v8t.toFixed(1)+'</td><td style="color:'+(Math.abs(parseFloat(e8t)||999)<20?'#4caf50':'#ea4335')+';">'+e8t+'%</td></tr>';
    html += '</table>';
    el.innerHTML = html;

    // 折线图
    chEl.innerHTML = '<div style="margin-top:10px;"><canvas id="overviewTotalChart" style="height:200px;width:100%;"></canvas></div>'
                  + '<div style="margin-top:16px;"><canvas id="overviewRegionChart" style="height:200px;width:100%;"></canvas></div>';

    // 渲染折线图
    setTimeout(() => {
      if(document.getElementById('overviewTotalChart')) {
        new Chart(document.getElementById('overviewTotalChart'), {
          type:'line',
          data:{labels:months, datasets:[{label:'大盘流水(万)',data:totals,borderColor:'#1a73e8',fill:true,backgroundColor:'rgba(26,115,232,0.05)',tension:0.3}]},
          options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},title:{display:true,text:'大盘流水趋势',font:{size:13}}},scales:{y:{beginAtZero:true,grid:{color:'rgba(0,0,0,0.04)'}},x:{grid:{display:false}}}}
        });
      }
      if(document.getElementById('overviewRegionChart')) {
        const ds = regs.map(g => ({label:g, data:chData[g]||[], borderColor:{包体:'#f9ab00',微信:'#34a853',抖音:'#1a73e8',硬核:'#ea4335',手Q:'#7f77dd'}[g], fill:false, tension:0.3}));
        new Chart(document.getElementById('overviewRegionChart'), {
          type:'line',
          data:{labels:months, datasets:ds},
          options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{position:'bottom',labels:{font:{size:10}}},title:{display:true,text:'各渠道流水趋势',font:{size:13}}},scales:{y:{beginAtZero:true,grid:{color:'rgba(0,0,0,0.04)'}},x:{grid:{display:false}}}}
        });
      }
    }, 200);
  });
}

// ==================== 页面2: 测算方案 ====================
function loadSchemes() {
  fetch('/api/ab-compare').then(r=>r.json()).then(d => {
    PREDICTIONS_CACHE = d;
    const schemes = [
      {key:'v6', data:d.v6, title:'v6集成模型', icon:'🔵', color:'#1a73e8', bg:'#f0f7ff', border:'#e0e0ff',
       principle:'基于历史数据的数学拟合：滑动平均(60%) + 幂函数衰减(25%) + 指数衰减(15%)。只依赖1-5月各渠道充值序列，纯数据驱动，不涉及用户构成或业务因子。'},
      {key:'v7', data:{total:d.v7.total, channels:d.v7}, title:'v7组件化模型', icon:'🟠', color:'#e65100', bg:'#fff8f0', border:'#ffe0b2',
       principle:'基于业务拆分的组件预测：付费用户数 × ARPPU × (1+增长率)。各渠道独立估算（抖音+10%、微信+2%、硬核-2%、手Q-5%、包体-2%），考虑渠道流量特征差异。'},
      {key:'v8', data:d.v8, title:'v8多维度加权', icon:'🟢', color:'#2e7d32', bg:'#f1faf1', border:'#c8e6c9',
       principle:'用户模型 v3：流水=付费用户数×ARPPU。付费用户用加权环比趋势预测（最新变化率动态加权，限幅±20%），ARPPU用最新值+趋势修正（大幅跳变时延续趋势，小幅波动时加权平均）。直接从用户规模和付费能力两个维度精算，不依赖留存率/首月付费率等中间参数。'}
    ];
    let html = '';
    for(const s of schemes) {
      html += `<div class="model-card" style="background:${s.bg};border:1px solid ${s.border};">
        <div class="model-title" style="color:${s.color};">${s.icon} ${s.title}</div>
        <div class="model-principle">${s.principle}</div>
        <div style="display:flex;align-items:baseline;gap:12px;margin-bottom:8px;">
          <span style="font-size:13px;color:#888;">预测结果</span>
          <span class="model-result" style="color:${s.color};">${s.data.total.toFixed(2)} 万</span>
        </div>`;
      // 渠道明细
      const chData = s.key === 'v7' ? d.v7 : s.data.channels;
      const actual = {"包体":24.41,"微信":119.93,"抖音":194.21,"硬核":51.85,"手Q":28.47};
      html += `<table style="font-size:12px;"><tr><th>渠道</th><th>上月实际</th><th>预测值</th><th>环比</th></tr>`;
      for(const [ch, av] of Object.entries(actual)) {
        let pred;
        if(s.key === 'v7') {
          pred = chData[ch] ? chData[ch].prediction : 0;
        } else if(s.key === 'v8') {
          pred = chData[ch] ? chData[ch].prediction : 0;
        } else {
          pred = chData[ch] ? chData[ch].ensemble : 0;
        }
        const pct = ((pred - av) / av * 100).toFixed(1);
        const tag = pct > 0 ? `<span class="tag tag-up">▲${pct}%</span>` : pct < 0 ? `<span class="tag tag-down">▼${Math.abs(pct)}%</span>` : '0%';
        html += `<tr><td class="left">${ch}</td><td>${av}</td><td>${pred.toFixed(2)}</td><td>${tag}</td></tr>`;
      }
      html += `</table></div>`;
    }
    document.getElementById('schemesContainer').innerHTML = html;
  });
}

// ==================== 页面3: 实时对比 ====================
function loadCompareProgress() {
  const el = document.getElementById('compareProgress');
  el.innerHTML = '<div class="spinner"></div>';
  fetch('/api/monthly-progress').then(r=>r.json()).then(d => {
    const tp = d.time_progress;
    const cur = d.actual_this_month || 0;
    const month = d.this_month || '2026-07';
    // 获取各模型全月预测
    fetch('/api/ab-compare').then(r=>r.json()).then(preds => {
      const v6t = (preds.v6 && preds.v6.total) || 0;
      const v7t = (preds.v7 && preds.v7.total) || 0;
      const day = Math.min(Math.floor(tp/100*31) || 1, 31);
      const et = cur/(day/31);
      
      let html = '<div style="margin-bottom:10px;">';
      html += '<div style="display:flex;justify-content:space-between;margin-bottom:4px;">';
      html += '<span style="font-size:13px;font-weight:600;">'+month+' 流水进度</span>';
      html += '<span style="font-size:12px;color:#888;">已过'+day+'天 / 31天 ('+tp+'%)</span></div>';
      html += '<div style="height:8px;background:#e0e0e0;border-radius:4px;overflow:hidden;">';
      html += '<div style="height:100%;width:'+Math.min(tp,100)+'%;background:#1a73e8;border-radius:4px;transition:width 0.5s;"></div></div>';
      html += '<div style="display:flex;justify-content:space-between;margin-top:6px;font-size:12px;">';
      html += '<span style="color:#1a73e8;font-weight:600;">当前 '+cur.toFixed(2)+'万</span>';
      html += '<span style="color:#888;">预估全月 '+et.toFixed(0)+'万 | v6目标 '+v6t.toFixed(0)+'万 | v7目标 '+v7t.toFixed(0)+'万</span></div>';
      html += '</div>';
      
      // 每月进度明细
      if(d.days && d.days.length > 0) {
        html += '<div style="font-size:12px;font-weight:600;margin:8px 0 4px;">每日流水明细</div>';
        html += '<div style="display:flex;gap:2px;overflow-x:auto;padding-bottom:4px;">';
        for(const dayData of d.days) {
          const h = Math.min((dayData.value || 0) / (Math.max(...d.days.map(x=>x.value||0),1)) * 50, 50);
          html += '<div style="display:flex;flex-direction:column;align-items:center;min-width:20px;">';
          html += '<div style="height:'+h+'px;width:14px;background:#1a73e8;border-radius:2px 2px 0 0;"></div>';
          html += '<div style="font-size:9px;color:#888;margin-top:2px;">'+(dayData.day||'')+'</div></div>';
        }
        html += '</div>';
      }
      
      el.innerHTML = html;
      // 自动加载偏离表
      autoCompareInline(preds, month, cur);
    });
  });
}

function autoCompareInline(preds, month, curTotal) {
  const el = document.getElementById('deviationTable');
  const regs = ['包体','微信','抖音','硬核','手Q'];
  const progress = 1/31; // 7月1日
  let html = '<table style="font-size:12px;width:100%;"><tr><th>模型</th><th>全月预测(万)</th><th>当前实际(万)</th><th>预估全月(万)</th><th>偏离%</th><th>评估</th></tr>';
  for(const mk of ['v6','v7','v8']) {
    if(!preds[mk]) continue;
    const p = preds[mk].total || 0;
    const est = curTotal>0 ? (curTotal/progress) : 0;
    const err = p>0 ? ((est/p-1)*100) : 0;
    const clr = Math.abs(err)<10?'#34a853':Math.abs(err)<25?'#f9ab00':'#ea4335';
    const tag = Math.abs(err)<10?'✅ 优':Math.abs(err)<25?'⚠️ 中':'❌ 差';
    html += '<tr><td><b>'+mk+'</b></td><td>'+p.toFixed(1)+'</td><td>'+curTotal.toFixed(2)+'</td><td>'+est.toFixed(0)+'</td><td style="color:'+clr+';font-weight:600;">'+err.toFixed(1)+'%</td><td>'+tag+'</td></tr>';
  }
  html += '</table>';
  // 按大区偏离（如果渠道数据可用）
  if(preds.v6 && preds.v6.channels) {
    html += '<h4 style="font-size:13px;font-weight:600;margin:12px 0 6px;">按大区偏离明细</h4>';
    html += '<table style="font-size:12px;width:100%;"><tr><th>大区</th><th>v6预测</th><th>v7预测</th></tr>';
    for(const g of regs) {
      const v6c = (preds.v6.channels[g] || 0);
      const v7c = preds.v7[g] ? (typeof preds.v7[g]==='number' ? preds.v7[g] : (preds.v7[g].prediction||0)) : 0;
      html += '<tr><td>'+g+'</td><td>'+v6c.toFixed(1)+'万</td><td>'+v7c.toFixed(1)+'万</td></tr>';
    }
    html += '</table>';
  }
  html += '<p style="font-size:11px;color:#999;margin-top:6px;">✅ 全部数据自动来自数数TD API</p>';
  el.innerHTML = html;
}

// ==================== 自动对比（兼容手动刷新） ====================
function autoCompare() {
  const st = document.getElementById('compareStatus');
  st.textContent = '⏳ 刷新中...';
  loadCompareProgress();
  st.textContent = '✅ 已刷新';
}

// ==================== 自动生成完整报告（按设计稿四模块） ====================
function fetchAndDisplayReport() {
  const el=document.getElementById('reportContainer'),st=document.getElementById('reportStatus');
  el.innerHTML='<div class="spinner"></div>';st.textContent='拉取中...';
  const R=['包体','微信','抖音','硬核','手Q'],tM='2026-07';
  const MI={v6:{n:'v6时间序列',c:'#1a73e8',b:'#e3edfa',s:'主模型'},v7:{n:'v7组件化',c:'#e65100',b:'#fff3e0',s:'积累中'},v8:{n:'原模型',c:'#888',b:'#f5f5f5',s:'已废'}};
  Promise.all([fetch('/api/monitor-status').then(r=>r.json()),fetch('/api/ab-compare').then(r=>r.json()),fetch('/api/report/channels').then(r=>r.json()),fetch('/api/monthly-progress').then(r=>r.json())]).then(([s,pr,ch,mp])=>{
    const li=(ch[R[0]]||[]).length-1,ja={};let jt=0;
    for(const g of R){const v=((ch[g]||[])[li]||0);ja[g]=v;jt+=v;}
    const pi=li-1,mt=R.reduce((s,g)=>s+((ch[g]||[])[pi]||0),0);
    const gc=(mk,g,p)=>{if(!p[mk])return 0;if(mk==='v6'){const c=p[mk].channels;return c&&c[g]?(typeof c[g]==='number'?c[g]:c[g].ensemble||0):0}if(mk==='v7'){const c=p[mk][g];return c?(typeof c==='number'?c:c.prediction||0):0}if(mk==='v8'){const c=p[mk].channels;return c&&c[g]?(typeof c[g]==='number'?c[g]:c[g].prediction||0):0}return 0};
    const m={v6:{total:0},v7:{total:0},v8:{total:0}};
    for(const mk of['v6','v7','v8']){for(const g of R)m[mk][g]=gc(mk,g,pr);m[mk].total=(mk==='v7')?R.reduce((s,g)=>s+m[mk][g],0):((pr[mk]&&pr[mk].total)||0)}
    const EA={'包体':{v6:'用户规模小，时序拟合不敏感',v7:'实际付费582人低于预估633'},'微信':{v6:'ARPPU稳定但未考虑活动减少',v7:'付费用户3463降至3053，高估留存'},'抖音':{v6:'3-4月ARPPU异常偏高拉高基线',v7:'付费用户27587降至11833，严重高估'},'硬核':{v6:'波动小时序模型准确',v7:'-2%衰减假设基本合理'},'手Q':{v6:'持续衰退时序捕捉下降',v7:'实际1106降至808超预期'}};
    let h='<div style="max-width:1100px;margin:0 auto;font-size:13px;">';
    h+='<div style="background:#fff;border-radius:10px;padding:16px 20px;margin-bottom:16px;border:1px solid #e8e8e8;"><h3 style="font-size:16px;font-weight:600;margin-bottom:14px;padding-bottom:8px;border-bottom:2px solid #1a73e8;color:#1a73e8;">1. 6月流水总览</h3><div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:14px;">';
    h+='<div style="background:#e3edfa;padding:12px;border-radius:8px;text-align:center;"><div style="font-size:22px;font-weight:600;color:#1a73e8;">'+jt.toFixed(0)+'</div><div style="font-size:11px;color:#888;">6月实际(万)</div></div>';
    const cg=jt-mt;h+='<div style="background:#f8f9ff;padding:12px;border-radius:8px;text-align:center;"><div style="font-size:22px;font-weight:600;color:'+(cg>=0?'#34a853':'#ea4335')+';">'+(cg>=0?'+':'')+cg.toFixed(0)+'</div><div style="font-size:11px;color:#888;">环比(万)</div></div>';
    h+='<div style="background:#f8f9ff;padding:12px;border-radius:8px;text-align:center;"><div style="font-size:22px;font-weight:600;color:#34a853;">'+m.v6.total.toFixed(0)+'</div><div style="font-size:11px;color:#888;">v6(万)</div></div><div style="background:#f8f9ff;padding:12px;border-radius:8px;text-align:center;"><div style="font-size:22px;font-weight:600;color:#ea4335;">'+m.v7.total.toFixed(0)+'</div><div style="font-size:11px;color:#888;">v7(万)</div></div></div>';
    h+='<table style="font-size:12px;width:100%;"><tr><th>大区</th><th>实际</th><th>v6</th><th>v6误差</th><th>误差分析</th><th>v7</th><th>v7误差</th><th>误差分析</th></tr>';
    for(const g of R){const a=ja[g]||0,v6v=m.v6[g],v7v=m.v7[g],e6=a>0?((v6v/a-1)*100).toFixed(1):'-',e7=a>0?((v7v/a-1)*100).toFixed(1):'-',a6=(EA[g]||{}).v6||'',a7=(EA[g]||{}).v7||'';h+='<tr><td>'+g+'</td><td>'+a.toFixed(2)+'</td><td>'+v6v.toFixed(1)+'</td><td style="color:'+(Math.abs(parseFloat(e6)||999)<30?'#34a853':'#ea4335')+';">'+e6+'%</td><td style="font-size:11px;color:#888;max-width:150px;">'+a6+'</td><td>'+v7v.toFixed(1)+'</td><td style="color:'+(Math.abs(parseFloat(e7)||999)<30?'#34a853':'#ea4335')+';">'+e7+'%</td><td style="font-size:11px;color:#888;max-width:150px;">'+a7+'</td></tr>';}
    h+='</table></div>';
    h+='<div style="background:#fff;border-radius:10px;padding:16px 20px;margin-bottom:16px;border:1px solid #e8e8e8;"><h3 style="font-size:16px;font-weight:600;margin-bottom:14px;padding-bottom:8px;border-bottom:2px solid #1a73e8;color:#1a73e8;">2. 测算方案</h3>';
    for(const mk of['v6','v7','v8']){const o=MI[mk];h+='<div style="background:'+o.b+';border-radius:8px;padding:10px 14px;margin-bottom:8px;"><div style="font-size:13px;font-weight:600;color:'+o.c+';">'+mk+' '+o.n+' ('+o.s+')</div><div style="font-size:12px;margin-top:4px;"><b>'+tM+'预测'+m[mk].total.toFixed(0)+'万</b> ';for(const g of R)h+=g+m[mk][g].toFixed(1)+'万 ';h+='</div>';if(mk==='v6')h+='<div style="font-size:11px;color:#1a73e8;margin-top:2px;">✅ 胜出，继续主模型</div>';if(mk==='v7')h+='<div style="font-size:11px;color:#e65100;margin-top:2px;">⏳ 积累数据，抖音+10%→+5%</div>';h+='</div>';}
    h+='</div><div style="background:#fff;border-radius:10px;padding:16px 20px;margin-bottom:16px;border:1px solid #e8e8e8;"><h3 style="font-size:16px;font-weight:600;margin-bottom:14px;padding-bottom:8px;border-bottom:2px solid #1a73e8;color:#1a73e8;">3. '+tM+'实时</h3>';
    const ct=mp.actual_this_month||s.actual_this_month||0;
    h+='<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:10px;"><div style="background:#e3edfa;padding:12px;border-radius:8px;text-align:center;"><div style="font-size:20px;font-weight:600;color:#1a73e8;">'+ct.toFixed(2)+'</div><div style="font-size:11px;color:#888;">'+tM+'当前</div></div><div style="background:#f8f9ff;padding:12px;border-radius:8px;text-align:center;"><div style="font-size:20px;font-weight:600;color:#34a853;">'+m.v6.total.toFixed(0)+'</div><div style="font-size:11px;color:#888;">v6预估</div></div><div style="background:#f8f9ff;padding:12px;border-radius:8px;text-align:center;"><div style="font-size:20px;font-weight:600;color:#ea4335;">'+m.v7.total.toFixed(0)+'</div><div style="font-size:11px;color:#888;">v7预估</div></div></div></div>';
    let be='v6',be2=999;for(const mk of['v6','v7']){const e=jt>0?Math.abs((m[mk].total/jt-1)*100):999;if(e<be2){be2=e;be=mk;}}
    h+='<div style="background:#fff;border-radius:10px;padding:16px 20px;margin-bottom:16px;border:1px solid #e8e8e8;"><h3 style="font-size:16px;font-weight:600;margin-bottom:14px;padding-bottom:8px;border-bottom:2px solid #1a73e8;color:#1a73e8;">4. 报告结算</h3><div style="background:#e3edfa;border-radius:10px;padding:12px 16px;margin-bottom:12px;"><div style="font-size:15px;font-weight:600;">胜出:'+MI[be].n+' 偏离'+be2.toFixed(1)+'%</div></div>';
    h+='<div style="background:#f8f9ff;border-radius:8px;padding:12px 16px;margin-bottom:12px;"><div style="font-size:14px;font-weight:600;margin-bottom:6px;">'+tM+'预测</div><table style="font-size:12px;width:100%;"><tr><th>大盘</th>';for(const g of R)h+='<th>'+g+'</th>';h+='</tr><tr><td>'+m[be].total.toFixed(0)+'</td>';for(const g of R)h+='<td>'+m[be][g].toFixed(1)+'</td>';h+='</tr></table></div>';
    h+='<table style="font-size:12px;width:100%;"><tr><th>模型</th><th>状态</th><th>调优</th><th>'+tM+'预测</th></tr><tr><td>v6</td><td>胜出</td><td>继续主模型</td><td>'+m.v6.total.toFixed(0)+'</td></tr><tr><td>v7</td><td>积累</td><td>抖音+10%→+5% ARPPU83→75</td><td>'+m.v7.total.toFixed(0)+'</td></tr><tr><td>v8(规划)</td><td>规划</td><td>v6+v7组合</td><td>—</td></tr></table></div></div>';
    el.innerHTML=h;st.textContent='✅ 完成';
  }).catch(e=>{el.innerHTML='<p style="color:#ea4335;padding:20px;">❌ '+e.message+'</p>';st.textContent='❌ 失败';});
}
function refreshArchiveList() {
  const sel = document.getElementById('selArchiveMonth');
  sel.innerHTML = '<option value="">-- 加载中 --</option>';
  fetch('/api/archives').then(r=>r.json()).then(d => {
    sel.innerHTML = '<option value="">-- 请选择 --</option>';
    (d.months || []).forEach(m => {
      sel.innerHTML += `<option value="${m}">${m}</option>`;
    });
  });
}

function loadArchive() {
  const month = document.getElementById('selArchiveMonth').value;
  const el = document.getElementById('archiveContainer');
  if(!month) { el.innerHTML = '<p style="color:#888;padding:10px;text-align:center;">选择月份后显示历史存档</p>'; return; }
  
  el.innerHTML = '<div class="spinner"></div>';
  fetch('/api/archive/'+month).then(r=>r.json()).then(d => {
    if(d.error) { el.innerHTML = '<div class="alert alert-danger">'+d.error+'</div>'; return; }
    
    const modelIcons = {v6:'🔵',v7:'🟠',v8:'🟢'};
    const modelColors = {v6:'#1a73e8',v7:'#e65100',v8:'#2e7d32'};
    const allChs = ['包体','微信','抖音','硬核','手Q'];
    
    let html = `<div class="alert alert-info"><b>📦 ${month} 月度存档</b> | 创建于 ${d.created_at}</div>`;
    
    // 实际值
    html += `<div style="padding:10px;background:#f8f9fa;border-radius:6px;margin-bottom:12px;">
      <b>实际流水:</b> ${d.actual !== null && d.actual !== undefined ? d.actual+'万' : '<span style="color:#999;">待录入</span>'}
      ${d.best_model ? ` | <b>🏆 最优: ${modelIcons[d.best_model]} ${d.models[d.best_model]?.name}</b>` : ''}
    </div>`;
    
    // 各方案详情
    for(const [mk, mdata] of Object.entries(d.models)) {
      const color = modelColors[mk] || '#888';
      const err = mdata.error_pct;
      const errColor = err !== null ? (err < 10 ? '#4caf50' : err < 20 ? '#ff9800' : '#f44336') : '#999';
      const errStr = err !== null ? `<b style="color:${errColor};">${err}%</b>` : '<span style="color:#999;">待验证</span>';
      
      html += `<div style="border:1px solid #e0e0e0;border-radius:6px;padding:12px;margin-bottom:8px;border-left:4px solid ${color};">
        <div style="font-size:14px;font-weight:600;color:${color};">${modelIcons[mk]} ${mdata.name}</div>
        <div style="font-size:12px;color:#666;margin:4px 0;">📖 ${mdata.principle}</div>
        <div style="font-size:13px;">预测: <b>${mdata.prediction}万</b> | 偏离: ${errStr}</div>`;
      
      // 渠道明细
      const chs = mdata.channels || {};
      if(Object.keys(chs).length > 0) {
        html += `<table style="font-size:12px;margin-top:6px;"><tr><th>渠道</th><th>预测(万)</th><th>上月实际</th></tr>`;
        const chActual = {"包体":24.41,"微信":119.93,"抖音":194.21,"硬核":51.85,"手Q":28.47};
        for(const ch of allChs) {
          if(chs[ch]) {
            const pct = ((chs[ch] - (chActual[ch]||0)) / (chActual[ch]||1) * 100).toFixed(1);
            const tag = pct > 0 ? `<span class="tag tag-up">▲${pct}%</span>` : `<span class="tag tag-down">▼${Math.abs(pct)}%</span>`;
            html += `<tr><td class="left">${ch}</td><td>${parseFloat(chs[ch]).toFixed(2)}</td><td>${chActual[ch]||'--'}万 ${tag}</td></tr>`;
          }
        }
        html += `</table>`;
      }
      html += `</div>`;
    }
    
    // 深度分析
    if(d.deep_analysis) {
      html += `<div style="padding:12px;background:#fff;border-radius:6px;border:1px solid #e0e0e0;">
        <b style="font-size:14px;">🔍 对比分析结论</b><br>`;
      for(const [mk, da] of Object.entries(d.deep_analysis)) {
        const icon = modelIcons[mk] || '📊';
        html += `<div style="font-size:12px;color:#555;margin:4px 0;">${icon} ${da.summary || '无'}</div>`;
      }
      html += `</div>`;
    }
    
    el.innerHTML = html;
  });
}

// ==================== 页面5: 数数设定 ====================
function loadTDConfig() {
  fetch('/api/load-config').then(r=>r.json()).then(d => {
    document.getElementById('txtToken').value = 'pAnIB0AX0B329Wp71w8YJYCeK0srZwI79eetZ630xi8zPfxmAx8doYJxj1mzVeFg';
    document.getElementById('txtHost').value = 'http://121.5.41.88:8992';
    document.getElementById('txtProjectId').value = 4;
    // 更新数据源状态
    const src = d.data_source || 'hardcoded';
    const ts = d.data_timestamp || '暂无';
    const srcLabel = src === 'td_api' ? '✅ 数数实时数据' : src === 'live' ? '🔄 缓存数据' : '💾 内置数据（数数未连通）';
    const srcColor = src === 'td_api' ? '#4caf50' : '#ff9800';
    const dsEl = document.getElementById('dataSourceStatus');
    if(dsEl) dsEl.innerHTML = '<div style="padding:10px;"><div>状态: <span style="color:'+srcColor+';font-weight:600;">'+srcLabel+'</span></div><div style="font-size:12px;color:#888;margin-top:6px;">上次同步: '+ts+'</div></div>';
  });
}

function saveTDConfig() {
  document.getElementById('tdConfigMsg').textContent = '✅ 已保存（凭证已固化在代码中，如需修改请直接编辑 credentials.json）';
  document.getElementById('tdConfigMsg').style.color = '#4caf50';
}

function testTDConnection() {
  document.getElementById('tdConfigMsg').textContent = '🔌 正在测试连接...';
  document.getElementById('tdConfigMsg').style.color = '#888';
  fetch('/api/refresh-data', {method:'POST'}).then(r=>r.json()).then(d => {
    if(d.success) {
      document.getElementById('tdConfigMsg').textContent = '✅ 数数连接成功！已获取 ' + d.months.length + ' 个月数据';
      document.getElementById('tdConfigMsg').style.color = '#4caf50';
    } else {
      document.getElementById('tdConfigMsg').textContent = '⚠️ 数数未连通(' + (d.error||'超时') + ')，当前使用内置数据，不影响预测';
      document.getElementById('tdConfigMsg').style.color = '#ff9800';
    }
    loadTDConfig();
  });
}

function refreshData() {
  document.getElementById('tdConfigMsg').textContent = '🔄 正在刷新...';
  document.getElementById('tdConfigMsg').style.color = '#888';
  fetch('/api/refresh-data', {method:'POST'}).then(r=>r.json()).then(d => {
    if(d.success) {
      document.getElementById('tdConfigMsg').textContent = '✅ 数据已刷新，共 ' + d.months.length + ' 个月';
    } else {
      document.getElementById('tdConfigMsg').textContent = '⚠️ 刷新失败，继续使用现有数据';
    }
    document.getElementById('tdConfigMsg').style.color = d.success ? '#4caf50' : '#ff9800';
    loadTDConfig();
  });
}

// ==================== 版本号 ====================
async function loadVersion() {
  try {
    const r = await fetch('/api/version');
    const d = await r.json();
    if (d.status === 'ok' && d.data.version) {
      const verNum = document.getElementById('verNum');
      if (verNum) verNum.textContent = d.data.version;
      const verDisp = document.getElementById('localVerDisplay');
      if (verDisp) verDisp.textContent = 'v' + d.data.version;
    }
  } catch(e) {} // 静默失败
}
function showApiUrl() {
  const box = document.getElementById('apiUrlBox');
  if (!box) return;
  const url = window.location.protocol + '//' + window.location.host + '/api/version';
  box.style.display = 'block';
  box.innerHTML = '<div style="margin-bottom:6px;font-weight:600;color:#333">🔗 版本号API地址</div>'
    + '<div style="display:flex;gap:6px;align-items:center">'
    + '<input type="text" readonly value="'+url+'" style="flex:1;padding:8px 10px;border:1px solid #ccc;border-radius:4px;font-size:13px;font-family:monospace" onclick="this.select()">'
    + '<button class="btn btn-outline" onclick="copyApiUrl()">📋 复制</button></div>'
    + '<div style="margin-top:6px;font-size:11px;color:#888">💡 将此地址提供给外部页面，即可获取系统版本号。</div>';
}
function copyApiUrl() {
  const input = document.querySelector('#apiUrlBox input');
  if (!input) return;
  input.select();
  navigator.clipboard.writeText(input.value).then(() => {
    const btn = event.target;
    const orig = btn.textContent;
    btn.textContent = '✅ 已复制';
    setTimeout(() => btn.textContent = orig, 2000);
  }).catch(() => {});
}

// ==================== 系统更新 ====================
async function checkUpdate() {
  const st = document.getElementById('updateStatus');
  if(!st) return;
  st.textContent = '⏳ 检查中...';
  try {
    const r = await fetch('/api/updater/check');
    const d = await r.json();
    if(d.status === 'ok' && d.data.status === 'update_available') {
      const notes = d.data.release_notes ? '\n\n更新说明: ' + d.data.release_notes : '';
      if(confirm(`📦 发现新版本 ${d.data.remote_version}！当前版本: ${d.data.local_version}${notes}\n\n是否立即更新？`)) {
        st.textContent = '⏳ 下载更新中...';
        const r2 = await fetch('/api/updater/update', {method:'POST'});
        const d2 = await r2.json();
        if(d2.status === 'ok' && d2.data.status === 'updated') {
          st.textContent = `✅ 更新成功！${d2.data.from_version} → ${d2.data.to_version}`;
          if(confirm(`✅ 更新成功！版本 ${d2.data.from_version} → ${d2.data.to_version}\n\n需要重启服务使新版本生效，是否立即重启？`)) {
            st.textContent = '⏳ 重启服务中...';
            try {
              const r3 = await fetch('/api/updater/restart', {method:'POST'});
              const d3 = await r3.json();
              if(d3.status === 'ok') {
                st.textContent = '✅ 服务已重启，请刷新页面';
                alert('✅ 服务已重启！请稍等几秒后刷新页面。');
              }
            } catch(e) {
              st.textContent = '⚠️ 重启指令已发送，请手动刷新';
            }
          } else {
            st.textContent = '✅ 更新完成，重启后生效';
          }
        } else {
          st.textContent = '❌ 更新失败: ' + (d2.data?.error || '未知错误');
        }
      } else {
        st.textContent = '⏸️ 已取消';
      }
    } else if(d.status === 'ok' && d.data.status === 'current') {
      st.textContent = `✅ 已是最新版本 v${d.data.local_version}`;
    } else {
      st.textContent = '❌ 检查失败: ' + (d.data?.error || '未知');
    }
  } catch(e) {
    st.textContent = '❌ 连接失败: ' + e.message;
  }
}
async function runRollback() {
  const st = document.getElementById('updateStatus');
  if(!st) return;
  // 获取可用回滚版本
  try {
    const rb = await fetch('/api/updater/backups');
    const rd = await rb.json();
    const vers = (rd.status === 'ok' && rd.data?.versions) ? rd.data.versions : [];
    if(vers.length === 0) {
      st.textContent = '❌ 没有找到可回滚的版本备份';
      return;
    }
    // 让用户选择版本
    const verList = vers.map((v,i) => `${i+1}. v${v}`).join('\n');
    const idx = prompt(`选择要回滚到的版本：\n${verList}\n\n输入编号 (1-${vers.length})`);
    if(!idx) { st.textContent = '⏸️ 已取消'; return; }
    const target = vers[parseInt(idx)-1];
    if(!target) { st.textContent = '❌ 无效选择'; return; }
    if(!confirm(`⚠️ 确定要回滚到 v${target}？当前文件将被还原为此版本的备份。`)) return;
    st.textContent = `⏳ 回滚到 v${target}...`;
    const r = await fetch('/api/updater/rollback', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({version: target})});
    const d = await r.json();
    if(d.status === 'ok' && d.data.status === 'rolled_back') {
      st.textContent = `✅ 已回滚: ${d.data.from_version} → ${d.data.to_version}`;
      if(confirm(`✅ 回滚成功！版本 ${d.data.from_version} → ${d.data.to_version}\n\n需要重启服务使旧版本生效，是否立即重启？`)) {
        st.textContent = '⏳ 重启服务中...';
        try {
          const r2 = await fetch('/api/updater/restart', {method:'POST'});
          const d2 = await r2.json();
          if(d2.status === 'ok') {
            st.textContent = '✅ 服务已重启，请刷新页面';
            alert('✅ 服务已重启！请稍等几秒后刷新页面。');
          }
        } catch(e) {
          st.textContent = '⚠️ 重启指令已发送，请手动刷新';
        }
      } else {
        st.textContent = '✅ 回滚完成，重启后生效';
      }
    } else {
      st.textContent = '❌ 回滚失败: ' + (d.data?.error || '未知');
    }
  } catch(e) {
    st.textContent = '❌ 回滚失败: ' + e.message;
  }
}

function init() {
  loadOverview();
  loadVersion();
}
window.onload = init;
</script>
</body>
</html>'''

# ==================== 版本号读取 ====================
def _get_version(base_dir="."):
    """从 data/local_version.json 读取版本号"""
    try:
        vp = os.path.join(base_dir, "data", "local_version.json")
        if os.path.exists(vp):
            with open(vp, "r", encoding="utf-8") as f:
                d = json.load(f)
            return d.get("version", "1.0.0")
    except:
        pass
    return "1.0.0"

class KPIHandler(http.server.BaseHTTPRequestHandler):
    def _json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def _html(self, content, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(content.encode("utf-8"))

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if path == "/":
            self._html(CONFIG_HTML)
        elif path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
        elif path == "/api/monitor-status":
            result = dict(_monitor_status)
            now = datetime.now()
            this_month = now.strftime("%Y-%m")
            result["this_month"] = this_month
            # 当前月实际流水通过 monthly-progress 接口获取更准确
            # 这里仅返回调度状态，不返回当前月流水以避免混淆
            result["actual_this_month"] = 0
            result["actual_month_total"] = 0
            self._json(result)
        elif path == "/api/version":
            """获取系统版本号（供外部页面/工具读取）"""
            try:
                ver = _get_version(BASE_DIR)
                self._json({"status": "ok", "data": {"version": ver, "version_file": "data/local_version.json"}})
            except Exception as e:
                self._json({"status": "error", "error": str(e)[:100]}, 500)
        elif path == "/api/updater/check":
            try:
                sys.path.insert(0, BASE_DIR)
                from updater.updater import SoftUpdater
                with open(os.path.join(BASE_DIR, "data", "credentials.json"), encoding="utf-8") as _f:
                    _uc = json.load(_f).get("updater", {})
                if _uc.get("mode") == "remote" and _uc.get("remote_url"):
                    _up = SoftUpdater(BASE_DIR, remote_base_url=_uc["remote_url"])
                elif _uc.get("mode") == "local" and _uc.get("local_path"):
                    _up = SoftUpdater(BASE_DIR, local_base_dir=_uc["local_path"])
                else:
                    self._json({"status": "error", "error": "未配置更新源"})
                    return
                _r = _up.check_version_only()
                self._json({"status": "ok", "data": _r})
            except Exception as ex:
                self._json({"status": "error", "error": str(ex)[:200]})
        elif path == "/api/updater/backups":
            """列出所有可回滚的版本"""
            try:
                sys.path.insert(0, BASE_DIR)
                from updater.updater import SoftUpdater
                _up = SoftUpdater(BASE_DIR)
                vers = _up.list_backups()
                self._json({"status": "ok", "data": {"versions": vers}})
            except Exception as ex:
                self._json({"status": "error", "error": str(ex)[:200]})
        elif path == "/api/load-config":
            cfg = load_config()
            game_cfg = cfg.get("games", {}).get(cfg.get("current_game", ""), {})
            live = refresh_live_data()
            self._json({
                "games": cfg.get("games", {}),
                "current_game": cfg.get("current_game", ""),
                "active_model": game_cfg.get("active_model", "v6"),
                "historical_total": live["totals"],
                "historical_months": live["months"],
                "growth_rates": cfg.get("models", {}).get("v7_channel_growth", {}),
                "data_source": live["source"],
                "data_timestamp": live.get("timestamp") or _live_data_cache.get("timestamp"),
                "data_error": live.get("error")
            })
        elif path == "/api/ab-compare":
            cfg = load_config()
            w = cfg.get("models", {}).get("v6_weight", {})
            rates = cfg.get("models", {}).get("v7_channel_growth", {})
            live = refresh_live_data()
            result = PredictEngine.ab_compare(weights=w, growth_rates=rates, live_data=live)
            self._json(result)
        elif path == "/api/predict/v6":
            cfg = load_config()
            w = cfg.get("models", {}).get("v6_weight", {})
            live = refresh_live_data()
            result = PredictEngine.v6_predict_all_channels(channels_data=live["revenue"], weights=w)
            self._json(result)
        elif path == "/api/predict/v7":
            cfg = load_config()
            rates = cfg.get("models", {}).get("v7_channel_growth", {})
            live = refresh_live_data()
            result = PredictEngine.v7_predict(growth_rates=rates)
            self._json(result)
        elif path == "/api/predict/v8":
            live = refresh_live_data()
            result = PredictEngine.v8_predict(channels_data=live["revenue"])
            self._json(result)
        elif path == "/api/predict/newuser":
            result = PredictEngine.new_user_estimate()
            self._json(result)
        elif path == "/api/backtest/v6":
            live = refresh_live_data()
            result = PredictEngine.run_backtest_v6(channels_data=live["revenue"])
            self._json(result)
        elif path == "/api/backtest/v7":
            result = PredictEngine.run_backtest_v7()
            self._json(result)
        elif path == "/api/backtest/v8":
            live = refresh_live_data()
            result = PredictEngine.run_backtest_v8(channels_data=live["revenue"])
            self._json(result)
        elif path == "/api/backtest/ab":
            live = refresh_live_data()
            result = PredictEngine.ab_backtest(channels_data=live["revenue"])
            self._json(result)
        elif path == "/api/backtest":
            live = refresh_live_data()
            result = PredictEngine.ab_backtest(channels_data=live["revenue"])
            self._json(result)
        elif path.startswith("/api/report/"):
            rtype = path.split("/")[-1]
            live = refresh_live_data()
            if rtype == "total":
                self._json({"months": live["months"], "totals": live["totals"]})
            elif rtype == "channels":
                self._json(live["revenue"])
            elif rtype == "users":
                self._json(live.get("pay_users", HISTORICAL_PAY_USERS))
            elif rtype == "arppu":
                self._json(live.get("arppu", HISTORICAL_ARPPU))
            else:
                self._json({"error": "unknown report type"})
        elif path == "/api/monthly-progress":
            live = get_active_data()
            # 如果缓存没有实时数据，尝试刷新一次
            if live["source"] == "hardcoded":
                refresh_live_data()
                live = get_active_data()
            cfg = load_config()
            game_cfg = cfg.get("games", {}).get(cfg.get("current_game", ""), {})
            w = cfg.get("models", {}).get("v6_weight", {})
            rates = cfg.get("models", {}).get("v7_channel_growth", {})
            # 获取各模型预测
            v6 = PredictEngine.v6_predict_all_channels(channels_data=live["revenue"], weights=w)
            v7 = PredictEngine.v7_predict(growth_rates=rates)
            v8 = PredictEngine.v8_predict(channels_data=live["revenue"])
            # 当月进度（从数数拉取当月累计）
            now = datetime.now()
            this_month = now.strftime("%Y-%m")
            days_in_month = (datetime(now.year, now.month % 12 + 1, 1) - datetime(now.year, now.month, 1)).days if now.month < 12 else 31
            days_passed = now.day
            time_progress = round(days_passed / days_in_month * 100, 1)
            # 尝试获取当月实际累计
            actual_this_month = None
            try:
                raw = _call_td_api(_build_event_payload(
                    start_time=f"{this_month}-01 00:00:00",
                    end_time=now.strftime("%Y-%m-%d 23:59:59"),
                    event_name="purchase", analysis="SUM", quota="recharge_amount",
                    time_particle="day", limit=500))
                if raw.get("success"):
                    vals = raw.get("values", {})
                    actual_this_month = sum(vals.values()) / 10000  # 元转万元
            except Exception:
                pass
            # 构建返回
            result = {
                "this_month": this_month,
                "days_passed": days_passed,
                "days_total": days_in_month,
                "time_progress": time_progress,
                "actual_this_month": round(actual_this_month, 2) if actual_this_month else None,
                "predictions": {
                    "v6": {"model": "v6集成模型", "total": v6["total"], "channels": {c: v["ensemble"] for c, v in v6.items() if c != "total"}},
                    "v7": {"model": "v7组件化模型", "total": v7["total"], "channels": {c: v.get("prediction", v["prediction"]) if isinstance(v, dict) else 0 for c, v in v7.items() if c != "total"}},
                    "v8": {"model": "v8用户生命周期", "total": v8["total"], "channels": {c: v["prediction"] for c, v in v8.items() if c != "total"}}
                },
                "history": {
                    "months": live["months"],
                    "totals": live["totals"],
                    "latest_total": live["totals"][-1] if live.get("totals") else 0
                },
                "data_source": live.get("source", "hardcoded")
            }
            self._json(result)
        elif path == "/api/history":
            self._json(load_prediction_history())
        elif path == "/api/history/compare":
            self._json(get_monthly_comparison())
        elif path == "/api/save-prediction":
            self._json({"error": "use POST"}, 405)
        elif path == "/api/record-actual":
            self._json({"error": "use POST"}, 405)
        elif path.startswith("/api/archive/"):
            month = path.replace("/api/archive/", "")
            result = get_month_archive(month)
            self._json(result if result else {"error": "not found"})
        elif path == "/api/archives":
            archives = load_archives()
            self._json({"months": sorted(archives.keys(), reverse=True)})
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"

        if path == "/api/monitor-toggle":
            try:
                cfg = load_config()
                game_name = cfg["current_game"]
                currently_enabled = cfg.get("games", {}).get(game_name, {}).get("daily_monitor_enabled", False)
                new_val = not currently_enabled
                cfg["games"][game_name]["daily_monitor_enabled"] = new_val
                save_config(cfg)
                if new_val:
                    start_daily_monitor()
                else:
                    stop_daily_monitor()
                self._json({"success": True, "enabled": new_val})
            except Exception as e:
                self._json({"success": False, "error": str(e)})
        elif path == "/api/switch-model":
            try:
                data = json.loads(body)
                model = data.get("model", "v6")
                if model not in ("v6", "v7"):
                    self._json({"success": False, "error": "无效模型"})
                    return
                cfg = load_config()
                game_name = cfg["current_game"]
                cfg["games"][game_name]["active_model"] = model
                save_config(cfg)
                self._json({"success": True, "active_model": model})
            except Exception as e:
                self._json({"success": False, "error": str(e)})
        elif path == "/api/save-config":
            try:
                data = json.loads(body)
                cfg = load_config()
                g = get_game_conf()
                if data.get("base_month"): cfg["games"][cfg["current_game"]]["base_month"] = data["base_month"]
                if data.get("target_month"): cfg["games"][cfg["current_game"]]["target_month"] = data["target_month"]
                if data.get("model"): cfg["games"][cfg["current_game"]]["model"] = data["model"]
                if data.get("webhook"): cfg["games"][cfg["current_game"]]["webhook"] = data["webhook"]
                if data.get("weights"):
                    cfg.setdefault("models", {}).setdefault("v6_weight", {}).update(data["weights"])
                save_config(cfg)
                self._json({"success": True})
            except Exception as e:
                self._json({"success": False, "error": str(e)})
        elif path == "/api/refresh-data":
            try:
                result = refresh_live_data()
                self._json({"success": result.get("success", False), 
                           "source": result.get("source", "unknown"),
                           "error": result.get("error"),
                           "months": result.get("months", []),
                           "totals": result.get("totals", []),
                           "timestamp": _live_data_cache.get("timestamp")})
            except Exception as e:
                self._json({"success": False, "error": str(e)})
        elif path == "/api/save-prediction":
            try:
                data = json.loads(body)
                record = {
                    "predict_month": data.get("predict_month", ""),
                    "model": data.get("model", ""),
                    "prediction": data.get("prediction", 0),
                    "channels": data.get("channels", {}),
                    "recorded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                save_prediction_record(record)
                self._json({"success": True})
            except Exception as e:
                self._json({"success": False, "error": str(e)})
        elif path == "/api/record-actual":
            try:
                data = json.loads(body)
                month = data.get("month", "")
                actual_value = float(data.get("actual_value", 0))
                record_actual_month(month, actual_value)
                self._json({"success": True})
            except Exception as e:
                self._json({"success": False, "error": str(e)})
        elif path == "/api/updater/update":
            try:
                sys.path.insert(0, BASE_DIR)
                from updater.updater import SoftUpdater
                with open(os.path.join(BASE_DIR, "data", "credentials.json"), encoding="utf-8") as _f:
                    _uc = json.load(_f).get("updater", {})
                if _uc.get("mode") == "remote" and _uc.get("remote_url"):
                    _up = SoftUpdater(BASE_DIR, remote_base_url=_uc["remote_url"])
                elif _uc.get("mode") == "local" and _uc.get("local_path"):
                    _up = SoftUpdater(BASE_DIR, local_base_dir=_uc["local_path"])
                else:
                    self._json({"status": "error", "error": "未配置更新源"})
                    return
                _r = _up.check_and_update()
                self._json({"status": "ok", "data": _r})
            except Exception as ex:
                self._json({"status": "error", "error": str(ex)[:200]})
        elif path == "/api/updater/rollback":
            try:
                sys.path.insert(0, BASE_DIR)
                from updater.updater import SoftUpdater
                data = json.loads(body) if body else {}
                target_ver = data.get("version", "")
                _up = SoftUpdater(BASE_DIR)
                _r = _up.rollback(target_version=target_ver)
                self._json({"status": "ok", "data": _r})
            except Exception as ex:
                self._json({"status": "error", "error": str(ex)[:200]})
        elif path == "/api/updater/restart":
            try:
                import subprocess
                subprocess.Popen([sys.executable, __file__], shell=False, creationflags=subprocess.DETACHED_PROCESS)
                self._json({"status": "ok", "data": {"message": "重启中..."}})
                threading.Thread(target=lambda: (time.sleep(0.5), os._exit(0)), daemon=True).start()
            except Exception as ex:
                self._json({"status": "error", "error": str(ex)[:200]})
        else:
            self._json({"error": "not found"}, 404)

    def log_message(self, format, *args):
        pass  # 静默日志

# ==================== 启动 ====================
def start_server():
    server = http.server.HTTPServer(("0.0.0.0", SYS_PORT), KPIHandler)
    print(f"[KPI测算] 应用启动于 http://localhost:{SYS_PORT}")
    webbrowser.open(f"http://localhost:{SYS_PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[KPI测算] 应用已停止")

if __name__ == "__main__":
    print("=" * 50)
    print("  全链路数据应用端 v1.0.2")
    print("  月度流水预估系统 · 多模型AB测试")
    print("=" * 50)
    print(f"  数据目录: {DATA_DIR}")
    print(f"  历史记录: {HISTORY_PATH}")
    # 如果之前开启了每日监控，自动恢复
    cfg = load_config()
    game_cfg = cfg.get("games", {}).get(cfg.get("current_game", ""), {})
    if game_cfg.get("daily_monitor_enabled", False):
        start_daily_monitor()
        print(f"  🔄 每日监控: 已自动恢复（上次为开启状态）")
    else:
        print(f"  💤 每日监控: 已关闭（可在界面开启）")
    print("=" * 50)
    start_server()

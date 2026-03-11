"""
Shopl & Company - 영업 현황 자동 보고서
Pipedrive 데이터를 분석해서 Slack에 일간/주간/월간 보고서를 전송합니다.

사용법:
  python shopl_sales_report.py --period daily    # 일간 보고서
  python shopl_sales_report.py --period weekly   # 주간 보고서
  python shopl_sales_report.py --period monthly  # 월간 보고서

설치 필요 패키지:
  pip install requests
"""

import sys
import io
import requests
import json
import argparse
from datetime import datetime, timedelta, date
from collections import defaultdict

# Windows 콘솔 UTF-8 출력 보정
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ── 설정 (환경변수 필수) ──────────────────────────────────────────────────────
import os
PIPEDRIVE_API_TOKEN = os.environ.get("PIPEDRIVE_API_TOKEN", "")
SLACK_WEBHOOK_URL   = os.environ.get("SLACK_WEBHOOK_URL", "")
CURRENCY_SYMBOL     = "₩"
# Pipedrive 커스텀 필드 키
COUNTRY_FIELD_KEY   = "8ac4a9ad1df40b2707c202c70b15a906be21c4f4"
# 분석 대상 파이프라인 (MAIN 샤플/하다 = 3)
TARGET_PIPELINE_ID  = 3
# ────────────────────────────────────────────────────────────────────────────


def get_all_deals():
    """Pipedrive에서 모든 딜을 가져옵니다."""
    deals = []
    start = 0
    limit = 500

    while True:
        url = (
            f"https://api.pipedrive.com/v1/deals"
            f"?api_token={PIPEDRIVE_API_TOKEN}"
            f"&limit={limit}&start={start}&status=all_not_deleted"
        )
        response = requests.get(url)
        data = response.json()

        if not data.get("success") or not data.get("data"):
            break

        deals.extend(data["data"])

        pagination = data.get("additional_data", {}).get("pagination", {})
        if not pagination.get("more_items_in_collection"):
            break
        start += limit

    return deals


def get_date_range(period: str):
    """기간에 따라 시작일/종료일 반환. 현재+직전 기간 모두 반환."""
    today = date.today()

    if period == "daily":
        start = today - timedelta(days=1)
        end   = today - timedelta(days=1)
        label = f"{start.strftime('%Y-%m-%d')} (어제)"
        prev_start = today - timedelta(days=2)
        prev_end   = today - timedelta(days=2)

    elif period == "weekly":
        days_since_monday = today.weekday()
        last_monday = today - timedelta(days=days_since_monday + 7)
        last_sunday  = last_monday + timedelta(days=6)
        start = last_monday
        end   = last_sunday
        label = f"{start.strftime('%m/%d')} ~ {end.strftime('%m/%d')} (지난주)"
        prev_start = last_monday - timedelta(days=7)
        prev_end   = last_monday - timedelta(days=1)

    elif period == "monthly":
        first_of_this_month = today.replace(day=1)
        last_month_end   = first_of_this_month - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        start = last_month_start
        end   = last_month_end
        label = f"{start.strftime('%Y년 %m월')}"
        prev_end   = last_month_start - timedelta(days=1)
        prev_start = prev_end.replace(day=1)

    return start, end, label, prev_start, prev_end


def parse_deal_date(deal, period):
    """딜이 해당 기간 안에 들어오는지 판단하기 위한 날짜 추출."""
    # 신규: add_time 기준
    # 성공: won_time 기준
    # 실패: lost_time 기준
    add_str  = deal.get("add_time", "")
    won_str  = deal.get("won_time", "")
    lost_str = deal.get("lost_time", "")

    def to_date(s):
        if not s:
            return None
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except:
            return None

    return {
        "added": to_date(add_str),
        "won":   to_date(won_str),
        "lost":  to_date(lost_str),
    }


OWNER_NAME_MAP = {
    "Dawn (류다언)": "Dawn",
    "서인원": "Danny",
    "이예빈(Evelyn)": "Evelyn",
    "Yujin Lee (Lena)": "Lena",
    "조현완": "Cole",
    "김해리": "Harry",
    "Jun": "Jun",
}
ACTIVE_OWNERS = {"Dawn", "Danny", "Evelyn"}


def normalize_owner(name):
    """Pipedrive 이름을 영문 이름으로 변환. 매핑에 없으면 None(제외 대상)."""
    return OWNER_NAME_MAP.get(name)


def is_target_deal(deal):
    """분석 대상 딜인지 확인: MAIN 샤플/하다 파이프라인 + KRW + 한국."""
    if deal.get("pipeline_id") != TARGET_PIPELINE_ID:
        return False
    if deal.get("currency") != "KRW":
        return False
    if deal.get(COUNTRY_FIELD_KEY) != "한국":
        return False
    owner = normalize_owner(deal.get("owner_name") or "미지정")
    if owner is None:
        return False
    return True


def format_mrr(value):
    """숫자를 만원 단위 MRR 포맷으로 변환."""
    if value is None:
        return "N/A"
    man = round(value / 10_000)
    return f"{CURRENCY_SYMBOL}{man:,}만"


def analyze(deals, start_date, end_date):
    """딜 목록을 분석해서 통계를 반환."""

    # 전사 집계
    total = {
        "new":       {"count": 0, "value": 0},
        "won":       {"count": 0, "value": 0},
        "lost":      {"count": 0, "value": 0},
        "remaining": {"count": 0, "value": 0},
    }

    # 담당자별 집계
    by_owner = defaultdict(lambda: {
        "new":       {"count": 0, "value": 0},
        "won":       {"count": 0, "value": 0},
        "lost":      {"count": 0, "value": 0},
        "remaining": {"count": 0, "value": 0},
    })

    won_deals = []
    lost_deals = []

    for deal in deals:
        if not is_target_deal(deal):
            continue
        dates  = parse_deal_date(deal, None)
        owner = normalize_owner(deal.get("owner_name") or "미지정")
        arr    = deal.get("value") or 0
        value  = round(arr / 12)  # ARR -> MRR
        status = deal.get("status")  # open / won / lost

        in_range = lambda d: d and start_date <= d <= end_date

        # 신규 (추가된 날짜 기준)
        if in_range(dates["added"]):
            total["new"]["count"] += 1
            total["new"]["value"] += value
            by_owner[owner]["new"]["count"] += 1
            by_owner[owner]["new"]["value"] += value

        # 성공
        if in_range(dates["won"]):
            total["won"]["count"] += 1
            total["won"]["value"] += value
            by_owner[owner]["won"]["count"] += 1
            by_owner[owner]["won"]["value"] += value
            won_deals.append({"title": deal.get("title", ""), "owner": owner, "val": value, "date": str(dates["won"]), "added": str(dates["added"]) if dates["added"] else "N/A"})

        # 실패
        if in_range(dates["lost"]):
            total["lost"]["count"] += 1
            total["lost"]["value"] += value
            by_owner[owner]["lost"]["count"] += 1
            by_owner[owner]["lost"]["value"] += value
            lost_deals.append({"title": deal.get("title", ""), "owner": owner, "val": value, "date": str(dates["lost"]), "added": str(dates["added"]) if dates["added"] else "N/A"})

        # 잔여 (end_date 시점 기준 스냅샷)
        # 조건: 기간 종료일 이전에 생성 + 아직 성공/실패 전이거나 기간 종료일 이후에 성공/실패
        added = dates["added"]
        won = dates["won"]
        lost = dates["lost"]
        if added and added <= end_date:
            not_yet_won = (won is None or won > end_date)
            not_yet_lost = (lost is None or lost > end_date)
            if not_yet_won and not_yet_lost:
                total["remaining"]["count"] += 1
                total["remaining"]["value"] += value
                by_owner[owner]["remaining"]["count"] += 1
                by_owner[owner]["remaining"]["value"] += value

    return total, dict(by_owner), won_deals, lost_deals


def calc_rate(part, whole):
    """비율 계산."""
    if whole == 0:
        return 0.0
    return part / whole * 100


def build_summary_table(cur):
    """전사/담당자 요약 테이블 생성 (직전 포함).
    직전 + 신규 - 성공 - 실패 = 잔여 (역산으로 직전 산출)
    """
    prev_cnt = cur["remaining"]["count"] + cur["won"]["count"] + cur["lost"]["count"] - cur["new"]["count"]
    prev_val = cur["remaining"]["value"] + cur["won"]["value"] + cur["lost"]["value"] - cur["new"]["value"]
    return (
        f"```\n"
        f"구분         건수       MRR\n"
        f"─────────────────────────────\n"
        f"직전       {prev_cnt:>5}건   {format_mrr(prev_val):>10}\n"
        f"신규       {cur['new']['count']:>5}건   {format_mrr(cur['new']['value']):>10}\n"
        f"성공       {cur['won']['count']:>5}건   {format_mrr(cur['won']['value']):>10}\n"
        f"실패       {cur['lost']['count']:>5}건   {format_mrr(cur['lost']['value']):>10}\n"
        f"잔여       {cur['remaining']['count']:>5}건   {format_mrr(cur['remaining']['value']):>10}\n"
        f"```"
    )


def build_slack_blocks(period, label, total, by_owner, won_deals, lost_deals):
    """Slack Block Kit 형식의 블록 생성."""

    period_kr = {"weekly": "주간", "monthly": "월간"}[period]
    emoji     = {"weekly": ":bar_chart:", "monthly": ":calendar:"}[period]

    won_rate_cnt  = calc_rate(total["won"]["count"],  total["remaining"]["count"] + total["won"]["count"] + total["lost"]["count"])
    lost_rate_cnt = calc_rate(total["lost"]["count"], total["remaining"]["count"] + total["won"]["count"] + total["lost"]["count"])
    won_rate_val  = calc_rate(total["won"]["value"],  total["remaining"]["value"] + total["won"]["value"] + total["lost"]["value"])

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # 전사 요약 테이블 (직전 포함)
    summary_table = build_summary_table(total)

    # 성공률/실패율
    denom_cnt = total["remaining"]["count"] + total["won"]["count"] + total["lost"]["count"]
    denom_val = total["remaining"]["value"] + total["won"]["value"] + total["lost"]["value"]
    lost_rate_val = calc_rate(total["lost"]["value"], denom_val)

    rate_text = (
        f":white_check_mark: *성공률*  "
        f"고객수 *{won_rate_cnt:.1f}%* ({total['won']['count']}/{denom_cnt}건)  |  "
        f"MRR *{won_rate_val:.1f}%* ({format_mrr(total['won']['value'])}/{format_mrr(denom_val)})\n"
        f":x: *실패율*  "
        f"고객수 *{lost_rate_cnt:.1f}%* ({total['lost']['count']}/{denom_cnt}건)  |  "
        f"MRR *{lost_rate_val:.1f}%* ({format_mrr(total['lost']['value'])}/{format_mrr(denom_val)})"
    )

    # 담당자별 현황 (직전 역산)
    owner_blocks = []
    if by_owner:
        owners_sorted = sorted(by_owner.items(), key=lambda x: -x[1]["won"]["value"])
        for name, s in owners_sorted:
            tbl = build_summary_table(s)
            owner_blocks.append({"name": name, "table": tbl})

    # 소요기간 계산 헬퍼
    def calc_days(added_str, event_str):
        try:
            added = datetime.strptime(added_str, "%Y-%m-%d").date()
            event = datetime.strptime(event_str, "%Y-%m-%d").date()
            return (event - added).days
        except:
            return None

    # 성공/실패 케이스
    won_total_mrr = sum(d["val"] for d in won_deals)
    lost_total_mrr = sum(d["val"] for d in lost_deals)

    if won_deals:
        won_lines = []
        for d in won_deals:
            days = calc_days(d["added"], d["date"])
            days_str = f"{days}일" if days is not None else "N/A"
            won_lines.append(
                f":white_check_mark:  *{d['title']}*  |  {d['owner']}  |  {format_mrr(d['val'])}  |  {d['date']}  |  {days_str}"
            )
        won_text = "\n".join(won_lines)
    else:
        won_text = "_해당 기간 성공 건 없음_"

    if lost_deals:
        lost_lines = []
        for d in lost_deals:
            days = calc_days(d["added"], d["date"])
            days_str = f"{days}일" if days is not None else "N/A"
            lost_lines.append(
                f":no_entry:  *{d['title']}*  |  {d['owner']}  |  {format_mrr(d['val'])}  |  {d['date']}  |  {days_str}"
            )
        lost_text = "\n".join(lost_lines)
    else:
        lost_text = "_해당 기간 실패 건 없음_"

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} Shopl {period_kr} 국내 영업현황 보고서",
                "emoji": True,
            },
        },
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f":calendar:  *{label}*  |  생성: {now}"}
            ],
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*:clipboard:  전사 요약*\n{summary_table}"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": rate_text},
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*:busts_in_silhouette:  담당자별 현황*"},
        },
    ]

    # 담당자별 테이블 블록 추가
    for ob in owner_blocks:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{ob['name']}*\n{ob['table']}"},
        })

    def split_text_blocks(header, text, max_len=2800):
        """긴 텍스트를 여러 블록으로 분할 (Slack 3000자 제한 대응)."""
        lines = text.split("\n")
        chunks = []
        current = ""
        for line in lines:
            if len(current) + len(line) + 1 > max_len:
                chunks.append(current)
                current = line
            else:
                current = current + "\n" + line if current else line
        if current:
            chunks.append(current)
        result = [{"type": "section", "text": {"type": "mrkdwn", "text": f"*{header}*\n{chunks[0]}"}}]
        for chunk in chunks[1:]:
            result.append({"type": "section", "text": {"type": "mrkdwn", "text": chunk}})
        return result

    blocks += [{"type": "divider"}]
    blocks += split_text_blocks(f":trophy:  성공 케이스 ({len(won_deals)}건, {format_mrr(won_total_mrr)})", won_text)
    blocks += split_text_blocks(f":rotating_light:  실패 케이스 ({len(lost_deals)}건, {format_mrr(lost_total_mrr)})", lost_text)
    blocks += [
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": ":bulb: MRR = Pipedrive ARR / 12  |  KRW 딜만 집계  |  건수/실패율 = 해당건 / (잔여+성공+실패)\n:globe_with_meridians: <https://junlee-shopl.github.io/pipedrive-report/|웹 대시보드 바로가기>"}
            ],
        },
    ]

    return blocks


def send_to_slack(blocks, period, label):
    """Slack Block Kit으로 메시지 전송."""
    period_kr = {"weekly": "주간", "monthly": "월간"}[period]
    payload = {
        "text": f"Shopl {period_kr} 국내 영업현황 보고서 - {label}",
        "blocks": blocks,
    }
    response = requests.post(
        SLACK_WEBHOOK_URL,
        json=payload,
        headers={"Content-Type": "application/json"},
    )
    if response.status_code == 200:
        print("Slack 전송 성공!")
    else:
        print(f"Slack 전송 실패: {response.status_code} {response.text}")


def build_html_section(total, by_owner, won_deals, lost_deals, period, label):
    """한 기간의 HTML 섹션을 생성."""
    period_kr = {"weekly": "주간", "monthly": "월간"}[period]

    prev_cnt = total["remaining"]["count"] + total["won"]["count"] + total["lost"]["count"] - total["new"]["count"]
    prev_val = total["remaining"]["value"] + total["won"]["value"] + total["lost"]["value"] - total["new"]["value"]

    denom_cnt = total["remaining"]["count"] + total["won"]["count"] + total["lost"]["count"]
    denom_val = total["remaining"]["value"] + total["won"]["value"] + total["lost"]["value"]
    won_rate_cnt = calc_rate(total["won"]["count"], denom_cnt)
    lost_rate_cnt = calc_rate(total["lost"]["count"], denom_cnt)
    won_rate_val = calc_rate(total["won"]["value"], denom_val)
    lost_rate_val = calc_rate(total["lost"]["value"], denom_val)

    def row(label, cnt, val):
        return f"<tr><td>{label}</td><td>{cnt}건</td><td>{format_mrr(val)}</td></tr>"

    summary = f"""<table class="summary">
<tr><th>구분</th><th>건수</th><th>MRR</th></tr>
{row("직전", prev_cnt, prev_val)}
{row("신규", total['new']['count'], total['new']['value'])}
{row("성공", total['won']['count'], total['won']['value'])}
{row("실패", total['lost']['count'], total['lost']['value'])}
{row("잔여", total['remaining']['count'], total['remaining']['value'])}
</table>"""

    rate_html = f"""<div class="rates">
<div class="rate-row won">&#x2705; 성공률 &nbsp; 고객수 <b>{won_rate_cnt:.1f}%</b> ({total['won']['count']}/{denom_cnt}건) &nbsp;|&nbsp; MRR <b>{won_rate_val:.1f}%</b> ({format_mrr(total['won']['value'])}/{format_mrr(denom_val)})</div>
<div class="rate-row lost">&#x274C; 실패율 &nbsp; 고객수 <b>{lost_rate_cnt:.1f}%</b> ({total['lost']['count']}/{denom_cnt}건) &nbsp;|&nbsp; MRR <b>{lost_rate_val:.1f}%</b> ({format_mrr(total['lost']['value'])}/{format_mrr(denom_val)})</div>
</div>"""

    # 담당자별
    owner_html = ""
    if by_owner:
        owners_sorted = sorted(by_owner.items(), key=lambda x: -x[1]["won"]["value"])
        for name, s in owners_sorted:
            pc = s["remaining"]["count"] + s["won"]["count"] + s["lost"]["count"] - s["new"]["count"]
            pv = s["remaining"]["value"] + s["won"]["value"] + s["lost"]["value"] - s["new"]["value"]
            owner_html += f"""<h3>{name}</h3>
<table class="summary">
<tr><th>구분</th><th>건수</th><th>MRR</th></tr>
{row("직전", pc, pv)}
{row("신규", s['new']['count'], s['new']['value'])}
{row("성공", s['won']['count'], s['won']['value'])}
{row("실패", s['lost']['count'], s['lost']['value'])}
{row("잔여", s['remaining']['count'], s['remaining']['value'])}
</table>"""

    def calc_days_html(added_str, event_str):
        try:
            a = datetime.strptime(added_str, "%Y-%m-%d").date()
            e = datetime.strptime(event_str, "%Y-%m-%d").date()
            return f"{(e - a).days}일"
        except:
            return "N/A"

    won_total_mrr = sum(d["val"] for d in won_deals)
    lost_total_mrr = sum(d["val"] for d in lost_deals)

    won_rows = ""
    for d in won_deals:
        days = calc_days_html(d["added"], d["date"])
        won_rows += f'<tr><td>&#x2705;</td><td class="deal-name">{d["title"]}</td><td>{d["owner"]}</td><td>{format_mrr(d["val"])}</td><td>{d["date"]}</td><td>{days}</td></tr>\n'

    lost_rows = ""
    for d in lost_deals:
        days = calc_days_html(d["added"], d["date"])
        lost_rows += f'<tr><td>&#x26D4;</td><td class="deal-name">{d["title"]}</td><td>{d["owner"]}</td><td>{format_mrr(d["val"])}</td><td>{d["date"]}</td><td>{days}</td></tr>\n'

    won_table = f"""<table class="deals"><tr><th></th><th>고객명</th><th>담당</th><th>MRR</th><th>날짜</th><th>소요</th></tr>
{won_rows}</table>""" if won_deals else "<p class='empty'>해당 기간 성공 건 없음</p>"

    lost_table = f"""<table class="deals"><tr><th></th><th>고객명</th><th>담당</th><th>MRR</th><th>날짜</th><th>소요</th></tr>
{lost_rows}</table>""" if lost_deals else "<p class='empty'>해당 기간 실패 건 없음</p>"

    return f"""
<div class="section"><h2>&#x1F4CB; 전사 요약</h2>{summary}{rate_html}</div>
<div class="section"><h2>&#x1F465; 담당자별 현황</h2>{owner_html}</div>
<div class="section"><h2>&#x1F3C6; 성공 케이스 ({len(won_deals)}건, {format_mrr(won_total_mrr)})</h2>{won_table}</div>
<div class="section"><h2>&#x1F6A8; 실패 케이스 ({len(lost_deals)}건, {format_mrr(lost_total_mrr)})</h2>{lost_table}</div>
"""


def get_weekly_ranges(n=8):
    """최근 n주의 (start, end, label) 리스트 반환 (최신이 마지막)."""
    today = date.today()
    days_since_monday = today.weekday()
    # 지난주 월요일
    last_monday = today - timedelta(days=days_since_monday + 7)
    ranges = []
    for i in range(n - 1, -1, -1):
        mon = last_monday - timedelta(weeks=i)
        sun = mon + timedelta(days=6)
        label = f"{mon.strftime('%m/%d')}"
        ranges.append((mon, sun, label))
    return ranges


def get_monthly_ranges(n=6):
    """최근 n개월의 (start, end, label) 리스트 반환 (최신이 마지막)."""
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - timedelta(days=1)
    ranges = []
    end = last_month_end
    for i in range(n):
        start = end.replace(day=1)
        ranges.append((start, end, start.strftime("%y.%m")))
        end = start - timedelta(days=1)
    ranges.reverse()
    return ranges


def build_trend_section(deals):
    """추세 탭용 HTML 생성 — JS 동적 SVG 꺾은선 그래프."""

    TIERS = ["30p", "10p", "10m"]  # 30+, 10+, 10-

    def get_tier(mrr):
        if mrr >= 300_000:
            return "30p"
        elif mrr >= 100_000:
            return "10p"
        else:
            return "10m"

    def analyze_range_by_tier(deals, start, end):
        """기간별 + MRR 구간별 분석."""
        result = {}
        for t in TIERS:
            result[t] = {
                "new": {"count": 0, "value": 0},
                "won": {"count": 0, "value": 0},
                "lost": {"count": 0, "value": 0},
                "remaining": {"count": 0, "value": 0},
            }

        for deal in deals:
            if not is_target_deal(deal):
                continue
            arr = deal.get("value") or 0
            mrr = round(arr / 12)
            tier = get_tier(mrr)
            dates = parse_deal_date(deal, None)
            status = deal.get("status")
            in_range = lambda d: d and start <= d <= end

            if in_range(dates["added"]):
                result[tier]["new"]["count"] += 1
                result[tier]["new"]["value"] += mrr
            if in_range(dates["won"]):
                result[tier]["won"]["count"] += 1
                result[tier]["won"]["value"] += mrr
            if in_range(dates["lost"]):
                result[tier]["lost"]["count"] += 1
                result[tier]["lost"]["value"] += mrr
            # 잔여 (end 시점 스냅샷)
            added = dates["added"]
            won = dates["won"]
            lost = dates["lost"]
            if added and added <= end:
                not_yet_won = (won is None or won > end)
                not_yet_lost = (lost is None or lost > end)
                if not_yet_won and not_yet_lost:
                    result[tier]["remaining"]["count"] += 1
                    result[tier]["remaining"]["value"] += mrr

        return result

    # 데이터 수집
    weekly_ranges = get_weekly_ranges(12)
    weekly_data = [analyze_range_by_tier(deals, s, e) for s, e, _ in weekly_ranges]
    weekly_labels = [lbl for _, _, lbl in weekly_ranges]

    monthly_ranges = get_monthly_ranges(12)
    monthly_data = [analyze_range_by_tier(deals, s, e) for s, e, _ in monthly_ranges]
    monthly_labels = [lbl for _, _, lbl in monthly_ranges]

    def to_js(data_list, labels):
        """각 기간별 데이터를 tier별로 분리한 JS 객체 배열 생성."""
        rows = []
        for lbl, d in zip(labels, data_list):
            parts = [f'l:"{lbl}"']
            for t in TIERS:
                td = d[t]
                parts.append(f'new_c_{t}:{td["new"]["count"]},won_c_{t}:{td["won"]["count"]},lost_c_{t}:{td["lost"]["count"]},rem_c_{t}:{td["remaining"]["count"]}')
                parts.append(f'new_v_{t}:{td["new"]["value"]},won_v_{t}:{td["won"]["value"]},lost_v_{t}:{td["lost"]["value"]},rem_v_{t}:{td["remaining"]["value"]}')
            rows.append('{' + ','.join(parts) + '}')
        return '[' + ','.join(rows) + ']'

    weekly_js = to_js(weekly_data, weekly_labels)
    monthly_js = to_js(monthly_data, monthly_labels)

    # 테이블은 JS에서 동적 생성 (선택한 tier에 따라 변경)

    return f"""
<div class="section">
<div class="trend-controls">
  <div class="control-group">
    <span class="control-label">기간</span>
    <button class="toggle-btn active" onclick="setPeriodMode('weekly')" id="btn-weekly">주간 (12주)</button>
    <button class="toggle-btn" onclick="setPeriodMode('monthly')" id="btn-monthly">월간 (12개월)</button>
  </div>
  <div class="control-group">
    <span class="control-label">항목</span>
    <label class="check-btn item-checks" style="--c:#1976d2"><input type="checkbox" value="new" checked onchange="drawCharts()"><span>신규</span></label>
    <label class="check-btn item-checks" style="--c:#2e7d32"><input type="checkbox" value="won" checked onchange="drawCharts()"><span>성공</span></label>
    <label class="check-btn item-checks" style="--c:#c62828"><input type="checkbox" value="lost" checked onchange="drawCharts()"><span>실패</span></label>
    <label class="check-btn item-checks" style="--c:#ff8f00"><input type="checkbox" value="rem" onchange="drawCharts()"><span>잔여</span></label>
  </div>
  <div class="control-group">
    <span class="control-label">MRR 구간</span>
    <label class="check-btn tier-checks" style="--c:#6a1b9a"><input type="checkbox" value="30p" checked onchange="drawCharts()"><span>30+</span></label>
    <label class="check-btn tier-checks" style="--c:#00838f"><input type="checkbox" value="10p" checked onchange="drawCharts()"><span>10+</span></label>
    <label class="check-btn tier-checks" style="--c:#795548"><input type="checkbox" value="10m" checked onchange="drawCharts()"><span>10-</span></label>
  </div>
</div>
</div>
<div class="section"><h2 id="chart-count-title">건수 추세</h2><div id="chart-count"></div></div>
<div class="section"><h2 id="chart-mrr-title">MRR 추세</h2><div id="chart-mrr"></div></div>
<div class="section" id="trend-table-section"><h3>상세 데이터</h3><div id="trend-table"></div></div>
<script>
var trendData={{weekly:{weekly_js},monthly:{monthly_js}}};
var periodMode='weekly';
var TIERS=['30p','10p','10m'];
var seriesDef={{new:{{color:'#1976d2',name:'신규'}},won:{{color:'#2e7d32',name:'성공'}},lost:{{color:'#c62828',name:'실패'}},rem:{{color:'#ff8f00',name:'잔여'}}}};
function setPeriodMode(m){{periodMode=m;document.getElementById('btn-weekly').classList.toggle('active',m==='weekly');document.getElementById('btn-monthly').classList.toggle('active',m==='monthly');drawCharts();}}
function getChecked(cls){{var r=[];document.querySelectorAll(cls+' input:checked').forEach(function(el){{r.push(el.value)}});return r;}}
function fmtMrr(v){{var m=Math.round(v/10000);return '\\u20a9'+m.toLocaleString()+'\\ub9cc';}}
function aggregateByTier(rawData,tiers){{
  return rawData.map(function(d){{
    var out={{l:d.l}};
    ['new','won','lost','rem'].forEach(function(k){{
      var sc=0,sv=0;
      tiers.forEach(function(t){{sc+=(d[k+'_c_'+t]||0);sv+=(d[k+'_v_'+t]||0);}});
      out[k+'_c']=sc;out[k+'_v']=sv;
    }});
    return out;
  }});
}}
function drawSVG(container,data,keys,field,fmtFn){{
  var W=850,H=250,pL=60,pR=20,pT=35,pB=40,cW=W-pL-pR,cH=H-pT-pB,n=data.length;
  if(n<2){{container.innerHTML='<p style="color:#999">데이터 부족</p>';return;}}
  var maxV=1;keys.forEach(function(k){{data.forEach(function(d){{var v=d[k+'_'+field];if(v>maxV)maxV=v;}});}});
  var s='<svg width="'+W+'" height="'+H+'" viewBox="0 0 '+W+' '+H+'" style="display:block;max-width:100%">';
  s+='<rect x="'+pL+'" y="'+pT+'" width="'+cW+'" height="'+cH+'" fill="#fafafa" rx="4"/>';
  for(var i=0;i<5;i++){{var y=pT+cH*i/4,val=maxV*(4-i)/4;var lt=field==='c'?Math.round(val)+'\\uac74':fmtMrr(val);s+='<line x1="'+pL+'" y1="'+y+'" x2="'+(pL+cW)+'" y2="'+y+'" stroke="#e0e0e0"/>';s+='<text x="'+(pL-8)+'" y="'+(y+4)+'" text-anchor="end" font-size="10" fill="#999">'+lt+'</text>';}}
  for(var i=0;i<n;i++){{var x=pL+cW*i/(n-1);s+='<text x="'+x+'" y="'+(H-8)+'" text-anchor="middle" font-size="10" fill="#999">'+data[i].l+'</text>';}}
  keys.forEach(function(k){{var sd=seriesDef[k],pts=[];for(var i=0;i<n;i++){{var x=pL+cW*i/(n-1),v=data[i][k+'_'+field],y=pT+cH*(1-v/maxV);pts.push([x,y,v]);}}
    var path='';pts.forEach(function(p,i){{path+=(i===0?'M':'L')+p[0].toFixed(1)+','+p[1].toFixed(1);}});
    s+='<path d="'+path+'" fill="none" stroke="'+sd.color+'" stroke-width="2.5" stroke-linejoin="round"/>';
    pts.forEach(function(p){{s+='<circle cx="'+p[0].toFixed(1)+'" cy="'+p[1].toFixed(1)+'" r="4" fill="'+sd.color+'" stroke="#fff" stroke-width="1.5"/>';var dv=field==='c'?p[2]:fmtMrr(p[2]);s+='<text x="'+p[0].toFixed(1)+'" y="'+(p[1]-9)+'" text-anchor="middle" font-size="9" fill="'+sd.color+'" font-weight="600">'+dv+'</text>';}});
  }});
  var lx=pL;keys.forEach(function(k){{var sd=seriesDef[k];s+='<rect x="'+lx+'" y="8" width="12" height="12" rx="2" fill="'+sd.color+'"/>';s+='<text x="'+(lx+16)+'" y="18" font-size="11" fill="#555">'+sd.name+'</text>';lx+=sd.name.length*14+30;}});
  s+='</svg>';container.innerHTML=s;
}}
function drawTable(data,colLabel){{
  var h='<table class="trend-table"><tr><th>'+colLabel+'</th><th>신규</th><th>성공</th><th>실패</th><th>잔여</th><th>성공MRR</th><th>실패MRR</th></tr>';
  data.forEach(function(d){{
    h+='<tr><td>'+d.l+'</td><td>'+d.new_c+'건</td><td>'+d.won_c+'건</td><td>'+d.lost_c+'건</td><td>'+d.rem_c+'건</td><td>'+fmtMrr(d.won_v)+'</td><td>'+fmtMrr(d.lost_v)+'</td></tr>';
  }});
  h+='</table>';
  document.getElementById('trend-table').innerHTML=h;
}}
function drawCharts(){{
  var rawData=trendData[periodMode];
  var keys=getChecked('.item-checks');
  var tiers=getChecked('.tier-checks');
  var data=aggregateByTier(rawData,tiers);
  var label=periodMode==='weekly'?'주간 (최근 12주)':'월간 (최근 12개월)';
  var colLabel=periodMode==='weekly'?'주':'월';
  document.getElementById('chart-count-title').innerHTML='&#x1F4C8; 건수 추세 — '+label;
  document.getElementById('chart-mrr-title').innerHTML='&#x1F4B0; MRR 추세 — '+label;
  drawSVG(document.getElementById('chart-count'),data,keys,'c',function(v){{return v+'건'}});
  drawSVG(document.getElementById('chart-mrr'),data,keys,'v',fmtMrr);
  drawTable(data,colLabel);
}}
drawCharts();
</script>"""


def build_client_section(deals):
    """고객사 탭용 HTML 생성 — 필터링 가능한 딜 목록."""
    import json as _json

    def get_tier(mrr):
        if mrr >= 300_000:
            return "30p"
        elif mrr >= 100_000:
            return "10p"
        else:
            return "10m"

    client_list = []
    for deal in deals:
        if not is_target_deal(deal):
            continue
        arr = deal.get("value") or 0
        mrr = round(arr / 12)
        owner = normalize_owner(deal.get("owner_name") or "미지정")
        status = deal.get("status", "")
        add_time = (deal.get("add_time") or "")[:10]
        update_time = (deal.get("update_time") or "")[:10]
        won_time = (deal.get("won_time") or "")[:10]
        lost_time = (deal.get("lost_time") or "")[:10]

        # 상태 매핑
        if status == "won":
            status_kr = "성공"
            status_key = "won"
        elif status == "lost":
            status_kr = "실패"
            status_key = "lost"
        else:
            status_kr = "진행중"
            status_key = "open"

        client_list.append({
            "id": deal.get("id"),
            "t": deal.get("title") or deal.get("org_name") or "",
            "o": owner,
            "m": mrr,
            "s": status_key,
            "sk": status_kr,
            "tier": get_tier(mrr),
            "add": add_time,
            "upd": update_time,
            "won": won_time,
            "lost": lost_time,
        })

    # MRR 내림차순 정렬
    client_list.sort(key=lambda x: -x["m"])
    clients_js = _json.dumps(client_list, ensure_ascii=False)

    return f"""
<div class="section">
<div class="trend-controls">
  <div class="control-group">
    <span class="control-label">상태</span>
    <label class="check-btn cl-status" style="--c:#1976d2"><input type="checkbox" value="open" checked onchange="drawClients()"><span>진행중</span></label>
    <label class="check-btn cl-status" style="--c:#2e7d32"><input type="checkbox" value="won" checked onchange="drawClients()"><span>성공</span></label>
    <label class="check-btn cl-status" style="--c:#c62828"><input type="checkbox" value="lost" checked onchange="drawClients()"><span>실패</span></label>
  </div>
  <div class="control-group">
    <span class="control-label">MRR 구간</span>
    <label class="check-btn cl-tier" style="--c:#6a1b9a"><input type="checkbox" value="30p" checked onchange="drawClients()"><span>30+</span></label>
    <label class="check-btn cl-tier" style="--c:#00838f"><input type="checkbox" value="10p" checked onchange="drawClients()"><span>10+</span></label>
    <label class="check-btn cl-tier" style="--c:#795548"><input type="checkbox" value="10m" checked onchange="drawClients()"><span>10-</span></label>
  </div>
  <div class="control-group">
    <span class="control-label">담당자</span>
    <label class="check-btn cl-owner" style="--c:#555"><input type="checkbox" value="all" checked onchange="toggleAllOwners(this)"><span>전체</span></label>
  </div>
  <div class="control-group">
    <input type="text" id="cl-search" placeholder="고객사 검색..." oninput="drawClients()" style="padding:6px 12px;border:1px solid #ddd;border-radius:6px;font-size:13px;width:180px;">
  </div>
</div>
</div>
<div class="section"><div id="cl-summary" style="font-size:13px;color:#888;margin-bottom:12px;"></div><div id="cl-table"></div></div>
<script>
var clientData={clients_js};
(function(){{
  var ownerList=['Dawn','Danny','Evelyn'];
  var container=document.querySelector('.cl-owner').parentElement;
  ownerList.forEach(function(o){{
    var lbl=document.createElement('label');
    lbl.className='check-btn cl-owner';
    lbl.style.cssText='--c:#555';
    lbl.innerHTML='<input type="checkbox" value="'+o+'" checked onchange="drawClients()"><span>'+o+'</span>';
    container.appendChild(lbl);
  }});
}})();
function toggleAllOwners(el){{
  var checked=el.checked;
  document.querySelectorAll('.cl-owner input').forEach(function(cb){{cb.checked=checked}});
  drawClients();
}}
function getCheckedCl(cls){{var r=[];document.querySelectorAll(cls+' input:checked').forEach(function(el){{if(el.value!=='all')r.push(el.value)}});return r;}}
function fmtMrrCl(v){{var m=Math.round(v/10000);return '\\u20a9'+m.toLocaleString()+'\\ub9cc';}}
function drawClients(){{
  var statuses=getCheckedCl('.cl-status');
  var tiers=getCheckedCl('.cl-tier');
  var owners=getCheckedCl('.cl-owner');
  var q=(document.getElementById('cl-search').value||'').toLowerCase();
  var filtered=clientData.filter(function(d){{
    if(statuses.indexOf(d.s)<0)return false;
    if(tiers.indexOf(d.tier)<0)return false;
    if(owners.length>0 && owners.indexOf(d.o)<0)return false;
    if(q && d.t.toLowerCase().indexOf(q)<0)return false;
    return true;
  }});
  var totalMrr=0;filtered.forEach(function(d){{totalMrr+=d.m}});
  document.getElementById('cl-summary').innerHTML='총 <b>'+filtered.length+'</b>건 &nbsp;|&nbsp; MRR 합계 <b>'+fmtMrrCl(totalMrr)+'</b>';
  var statusColor={{open:'#1976d2',won:'#2e7d32',lost:'#c62828'}};
  var h='<table class="deals" style="width:100%"><tr><th>고객사</th><th>담당자</th><th>MRR</th><th>상태</th><th>생성일</th><th>최종 업데이트</th></tr>';
  filtered.forEach(function(d){{
    var sc=statusColor[d.s]||'#333';
    var link='https://shoplworks.pipedrive.com/deal/'+d.id;h+='<tr><td class="deal-name" style="max-width:250px"><a href="'+link+'" target="_blank" style="color:#1264a3;text-decoration:none">'+d.t+'</a></td><td>'+d.o+'</td><td style="text-align:right">'+fmtMrrCl(d.m)+'</td><td style="color:'+sc+';font-weight:500">'+d.sk+'</td><td>'+d.add+'</td><td>'+d.upd+'</td></tr>';
  }});
  h+='</table>';
  document.getElementById('cl-table').innerHTML=h;
}}
drawClients();
</script>"""


def generate_html_page(deals, output_path="docs/index.html"):
    """주간/월간 + 추세 보고서를 하나의 HTML 페이지로 생성."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    tabs_html = ""
    contents_html = ""

    for i, period in enumerate(["weekly", "monthly"]):
        period_kr = {"weekly": "주간", "monthly": "월간"}[period]
        start_date, end_date, label, _, _ = get_date_range(period)
        total, by_owner, won_deals, lost_deals = analyze(deals, start_date, end_date)
        active = " active" if i == 0 else ""
        tabs_html += f'<button class="tab{active}" onclick="showTab(\'{period}\')">{period_kr}</button>\n'
        section = build_html_section(total, by_owner, won_deals, lost_deals, period, label)
        display = "block" if i == 0 else "none"
        contents_html += f'<div id="tab-{period}" class="tab-content" style="display:{display}"><div class="label">{label} &nbsp;|&nbsp; 생성: {now}</div>{section}</div>\n'

    # 추세 탭
    tabs_html += '<button class="tab" onclick="showTab(\'trend\')">추세</button>\n'
    trend_section = build_trend_section(deals)
    contents_html += f'<div id="tab-trend" class="tab-content" style="display:none"><div class="label">추세 분석 &nbsp;|&nbsp; 생성: {now}</div>{trend_section}</div>\n'

    # 고객사 탭
    tabs_html += '<button class="tab" onclick="showTab(\'clients\')">고객사</button>\n'
    client_section = build_client_section(deals)
    contents_html += f'<div id="tab-clients" class="tab-content" style="display:none"><div class="label">고객사 목록 &nbsp;|&nbsp; 생성: {now}</div>{client_section}</div>\n'

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Shopl 영업현황 보고서</title>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:-apple-system,'Malgun Gothic',sans-serif; background:#f5f5f5; color:#333; padding:20px; }}
.container {{ max-width:900px; margin:0 auto; }}
h1 {{ color:#1a1a1a; font-size:1.5em; margin-bottom:16px; }}
.tabs {{ display:flex; gap:8px; margin-bottom:20px; }}
.tab {{ background:#e0e0e0; color:#555; border:none; padding:10px 24px; border-radius:8px; cursor:pointer; font-size:14px; font-weight:600; }}
.tab.active {{ background:#1264a3; color:#fff; }}
.tab:hover {{ background:#d0d0d0; }}
.tab.active:hover {{ background:#1264a3; }}
.label {{ color:#888; font-size:13px; margin-bottom:16px; }}
.section {{ background:#fff; border-radius:10px; padding:20px; margin-bottom:16px; box-shadow:0 1px 3px rgba(0,0,0,0.08); }}
.section h2 {{ color:#1a1a1a; font-size:1.1em; margin-bottom:12px; }}
.section h3 {{ color:#e8912d; font-size:0.95em; margin:16px 0 8px; }}
table.summary {{ width:100%; border-collapse:collapse; font-size:14px; }}
table.summary th {{ text-align:left; color:#888; padding:6px 12px; border-bottom:1px solid #e0e0e0; }}
table.summary td {{ padding:6px 12px; border-bottom:1px solid #f0f0f0; }}
table.summary tr:nth-child(2) td {{ color:#888; }}
table.summary tr:nth-child(3) td {{ color:#1976d2; }}
table.summary tr:nth-child(4) td {{ color:#2e7d32; }}
table.summary tr:nth-child(5) td {{ color:#c62828; }}
table.summary tr:nth-child(6) td {{ color:#333; font-weight:600; }}
.rates {{ margin-top:12px; font-size:14px; line-height:1.8; }}
.rate-row.won {{ color:#2e7d32; }}
.rate-row.lost {{ color:#c62828; }}
table.deals {{ width:100%; border-collapse:collapse; font-size:13px; }}
table.deals th {{ text-align:left; color:#888; padding:6px 8px; border-bottom:1px solid #e0e0e0; }}
table.deals td {{ padding:6px 8px; border-bottom:1px solid #f0f0f0; }}
table.deals td.deal-name {{ color:#1a1a1a; font-weight:500; max-width:300px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }}
.empty {{ color:#999; font-style:italic; }}
.footer {{ text-align:center; color:#aaa; font-size:12px; margin-top:24px; }}
.chart-grid {{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:16px; }}
@media(max-width:700px) {{ .chart-grid {{ grid-template-columns:1fr; }} }}
.chart-block {{ background:#fafafa; border-radius:8px; padding:12px; }}
.chart-block h4 {{ font-size:13px; color:#555; margin-bottom:8px; }}
.bar-row {{ display:flex; align-items:center; gap:6px; margin-bottom:4px; }}
.bar-label {{ font-size:11px; color:#888; min-width:36px; text-align:right; }}
.bar-track {{ flex:1; height:18px; background:#eee; border-radius:4px; overflow:hidden; }}
.bar-fill {{ height:100%; border-radius:4px; transition:width 0.3s; min-width:2px; }}
.bar-value {{ font-size:11px; color:#555; min-width:48px; }}
table.trend-table {{ width:100%; border-collapse:collapse; font-size:13px; margin-top:8px; }}
table.trend-table th {{ text-align:left; color:#888; padding:6px 10px; border-bottom:1px solid #e0e0e0; }}
table.trend-table td {{ padding:6px 10px; border-bottom:1px solid #f0f0f0; }}
.trend-controls {{ display:flex; flex-wrap:wrap; gap:16px; align-items:center; }}
.control-group {{ display:flex; align-items:center; gap:8px; }}
.control-label {{ font-size:13px; font-weight:600; color:#555; }}
.toggle-btn {{ background:#e0e0e0; color:#555; border:none; padding:7px 16px; border-radius:6px; cursor:pointer; font-size:13px; font-weight:500; transition:all 0.2s; }}
.toggle-btn:hover {{ background:#d0d0d0; }}
.toggle-btn.active {{ background:#1264a3; color:#fff; }}
.check-btn {{ display:inline-flex; align-items:center; gap:4px; padding:5px 12px; border-radius:6px; cursor:pointer; font-size:13px; border:2px solid var(--c,#999); color:var(--c,#333); background:#fff; transition:all 0.2s; }}
.check-btn input {{ display:none; }}
.check-btn:has(input:checked) {{ background:var(--c,#999); color:#fff; }}
</style>
</head>
<body>
<div class="container">
<h1>&#x1F4CA; Shopl 국내 영업현황 보고서</h1>
<div class="tabs">{tabs_html}</div>
{contents_html}
<div class="footer">MRR = Pipedrive ARR / 12 &nbsp;|&nbsp; KRW 딜만 집계 &nbsp;|&nbsp; 성공/실패율 = 해당건 / (잔여+성공+실패)</div>
</div>
<script>
function showTab(period) {{
  document.querySelectorAll('.tab-content').forEach(el => el.style.display = 'none');
  document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
  document.getElementById('tab-' + period).style.display = 'block';
  event.target.classList.add('active');
}}
</script>
</body>
</html>"""

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"HTML 페이지 생성: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Shopl 영업 현황 Slack 보고서")
    parser.add_argument(
        "--period",
        choices=["weekly", "monthly"],
        help="보고서 기간: weekly / monthly",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Slack 전송 없이 터미널에만 출력",
    )
    parser.add_argument(
        "--generate-page",
        action="store_true",
        help="HTML 대시보드 페이지 생성 (docs/index.html)",
    )
    args = parser.parse_args()

    if not args.period and not args.generate_page:
        parser.error("--period 또는 --generate-page 중 하나는 필요합니다")

    print(f"Pipedrive 데이터 가져오는 중...")
    deals = get_all_deals()
    print(f"총 {len(deals)}개 딜 로드 완료")

    if args.generate_page:
        generate_html_page(deals)

    if args.period:
        start_date, end_date, label, _, _ = get_date_range(args.period)
        print(f"분석 기간: {start_date} ~ {end_date}")

        total, by_owner, won_deals, lost_deals = analyze(deals, start_date, end_date)
        blocks = build_slack_blocks(args.period, label, total, by_owner, won_deals, lost_deals)

        # dry-run: 콘솔 요약 출력
        print(f"\n{'='*50}")
        period_kr = {"weekly": "주간", "monthly": "월간"}[args.period]
        print(f"  Shopl {period_kr} 국내 영업현황 보고서  |  {label}")
        print(f"  신규 {total['new']['count']}건 | 성공 {total['won']['count']}건 | 실패 {total['lost']['count']}건 | 잔여 {total['remaining']['count']}건")
        print(f"  성공 MRR {format_mrr(total['won']['value'])} | 실패 MRR {format_mrr(total['lost']['value'])} | 잔여 MRR {format_mrr(total['remaining']['value'])}")
        if won_deals:
            print(f"  성공 케이스: {', '.join(d['title'][:20] for d in won_deals)}")
        if lost_deals:
            print(f"  실패 케이스: {', '.join(d['title'][:20] for d in lost_deals)}")
        print(f"{'='*50}\n")

        if args.dry_run:
            print("dry-run 모드: Slack 전송 건너뜀")
        else:
            send_to_slack(blocks, args.period, label)


if __name__ == "__main__":
    main()

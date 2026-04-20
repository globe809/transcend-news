"""
創見資訊新聞監控系統 — 自動抓取腳本
由 GitHub Actions 排程執行（台灣時間 08:00 / 16:00）
儲存結果到 Firebase Firestore
"""

import os
import json
import hashlib
import datetime
import sys
import time
import requests
import feedparser
import firebase_admin
from firebase_admin import credentials, firestore

# ─── 情緒關鍵字 ───
POS_KW = ['獲獎','表揚','優勝','榮獲','營收','新高','成長','獲利','上漲','突破',
           '合作','推出','創新','領先','冠軍','熱銷','供不應求','超預期','亮眼',
           'award','growth','revenue','record high','profit','partnership','launch',
           'beats','strong','bullish','surge','rise','positive','upgrade','outperform']
NEG_KW = ['崩盤','瑕疵','虧損','下滑','召回','訴訟','罰款','跌','下跌','危機',
           '裁員','停產','倒閉','供應鏈中斷','市場萎縮','利空',
           'recall','loss','lawsuit','fine','crash','decline','fall','drop',
           'risk','bearish','weak','miss','cut','layoff','bankruptcy','downgrade']

# ─── 台灣媒體對照表 ───
TAIWAN_MEDIA = {
    'udn.com': '聯合報', 'money.udn.com': '經濟日報', 'ctee.com.tw': '工商時報',
    'chinatimes.com': '中時新聞網', 'technews.tw': '科技新報', 'ithome.com.tw': 'iThome',
    'digitimes.com.tw': '電子時報', 'digitimes.com': '電子時報',
    'eettaiwan.com': 'EE Times Taiwan', 'eetimes.com': 'EE Times',
    'anue.com.tw': '鉅亨網', 'cnyes.com': '鉅亨網', 'news.cnyes.com': '鉅亨網', 'm.cnyes.com': '鉅亨網',
    'wantrich.chinatimes.com': '旺得富理財網',
    'ltn.com.tw': '自由時報', 'ec.ltn.com.tw': '自由時報', 'setn.com': '三立新聞', 'tvbs.com.tw': 'TVBS',
    'ettoday.net': 'ETtoday', 'finance.ettoday.net': 'ETtoday財經雲', 'storm.mg': '風傳媒', 'businessweekly.com.tw': '商業週刊',
    'cw.com.tw': '天下雜誌', 'bnext.com.tw': 'Meet 創業小聚', 'inside.com.tw': 'INSIDE',
    'moneydj.com': '精實財經(MoneyDJ)', 'stockfeel.com.tw': '股感',
    'nownews.com': 'NOWnews', 'mirrormedia.mg': '鏡週刊', 'ctinews.com': '中天新聞',
    # 新增媒體
    'cna.com.tw': '中央社',
    'wealth.com.tw': '財訊雙周刊',
    'nextapple.com': '壹蘋新聞網', 'news.nextapple.com': '壹蘋新聞網',
    'mnews.tw': '鏡報', 'mirrordaily.news': '鏡報',
    'newtalk.tw': 'Newtalk',
    'gvm.com.tw': '遠見',
    'ustv.com.tw': '非凡財經',
    'trendforce.com': '集邦科技(TrendForce)',
    'pcdiy.com.tw': 'PC DIY',
    'imageinfo.com.tw': 'Image Media',
    'ioiotimes.com': 'ioio Times',
    'moneyweekly.com.tw': '理財周刊',
    'ftnn.com.tw': 'FTNN新聞網',
    'chinatimes.com/stock': '時報資訊',
}

# ─── RSS 新聞來源 ───
def get_sources(mode):
    """
    mode: 'morning' = 創見 + 競品 + 供應商
          'afternoon' = 美國市場 + 競品 + 供應商
          'all' = 全部
    """
    transcend = [
        {'label': '創見資訊', 'url': 'https://news.google.com/rss/search?q=創見資訊&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Transcend Info', 'url': 'https://news.google.com/rss/search?q=Transcend+Information+memory&hl=en&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': '2451 股票', 'url': 'https://news.google.com/rss/search?q=2451+創見+股票&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': '聯合報科技', 'url': 'https://udn.com/rssfeed/news/2/6644?ch=news', 'cat': 'transcend', 'filter': '創見'},
        {'label': '中央社', 'url': 'https://www.cna.com.tw/rss/aall.aspx', 'cat': 'transcend', 'filter': '創見'},
        {'label': '財訊', 'url': 'https://www.wealth.com.tw/rss/all', 'cat': 'transcend', 'filter': '創見'},
        {'label': '遠見', 'url': 'https://www.gvm.com.tw/rss', 'cat': 'transcend', 'filter': '創見'},
        {'label': '壹蘋新聞', 'url': 'https://tw.nextapple.com/rss.xml', 'cat': 'transcend', 'filter': '創見'},
        {'label': 'TrendForce', 'url': 'https://www.trendforce.com/rss', 'cat': 'transcend', 'filter': 'Transcend'},
        {'label': 'Google-財訊創見', 'url': 'https://news.google.com/rss/search?q=創見+site:wealth.com.tw&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-中央社創見', 'url': 'https://news.google.com/rss/search?q=創見+site:cna.com.tw&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-電子時報創見', 'url': 'https://news.google.com/rss/search?q=創見+site:digitimes.com.tw&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-非凡創見', 'url': 'https://news.google.com/rss/search?q=創見+非凡財經&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-PC DIY', 'url': 'https://news.google.com/rss/search?q=Transcend+site:pcdiy.com.tw&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-TrendForce', 'url': 'https://news.google.com/rss/search?q=Transcend+site:trendforce.com&hl=en&gl=US&ceid=US:en', 'cat': 'transcend'},
        # ─── 補齊重要財經媒體 ───
        {'label': 'Google-經濟日報創見', 'url': 'https://news.google.com/rss/search?q=創見+site:money.udn.com&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-工商時報創見', 'url': 'https://news.google.com/rss/search?q=創見+site:ctee.com.tw&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-中時新聞創見', 'url': 'https://news.google.com/rss/search?q=創見+site:chinatimes.com&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-鉅亨網創見',   'url': 'https://news.google.com/rss/search?q=創見+site:cnyes.com&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-自由時報創見', 'url': 'https://news.google.com/rss/search?q=創見+site:ltn.com.tw&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-MoneyDJ創見',  'url': 'https://news.google.com/rss/search?q=創見+site:moneydj.com&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-時報資訊創見', 'url': 'https://news.google.com/rss/search?q=創見+site:chinatimes.com/stock&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-FTNN創見',     'url': 'https://news.google.com/rss/search?q=創見+FTNN&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-理財周刊創見', 'url': 'https://news.google.com/rss/search?q=創見+site:moneyweekly.com.tw&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        # ─── 補齊 4/15 新聞稿未抓到的媒體 ───
        {'label': '鉅亨網台股 RSS',       'url': 'https://news.cnyes.com/rss/cat/tw_stock', 'cat': 'transcend', 'filter': '創見'},
        {'label': '鉅亨網台股新聞 RSS',   'url': 'https://news.cnyes.com/rss/cat/tw_stock_news', 'cat': 'transcend', 'filter': '創見'},
        {'label': 'Google-旺得富創見',    'url': 'https://news.google.com/rss/search?q=創見+site:wantrich.chinatimes.com&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': '聯合報財經 RSS',        'url': 'https://udn.com/rssfeed/news/2/6645?ch=news', 'cat': 'transcend', 'filter': '創見'},
        {'label': '聯合報股市 RSS',        'url': 'https://udn.com/rssfeed/news/2/6881?ch=news', 'cat': 'transcend', 'filter': '創見'},
        {'label': '經濟日報 RSS',          'url': 'https://money.udn.com/rssfeed/news/1001/5591?ch=news', 'cat': 'transcend', 'filter': '創見'},
        {'label': 'Google-聯合報創見',    'url': 'https://news.google.com/rss/search?q=創見+site:udn.com&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        # ─── 依 clipping report 補齊高頻媒體 ───
        {'label': 'Google-鏡報創見(mnews)',    'url': 'https://news.google.com/rss/search?q=創見+site:mnews.tw&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-鏡報創見(mirror)',   'url': 'https://news.google.com/rss/search?q=創見+site:mirrordaily.news&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-Newtalk創見',        'url': 'https://news.google.com/rss/search?q=創見+site:newtalk.tw&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-ETtoday財經創見',    'url': 'https://news.google.com/rss/search?q=創見+site:finance.ettoday.net&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
        {'label': 'Google-自由時報財經創見',   'url': 'https://news.google.com/rss/search?q=創見+site:ec.ltn.com.tw&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'transcend'},
    ]
    us_market = [
        # ─── 上游供應商（英文）───
        {'label': 'Samsung Memory EN',   'url': 'https://news.google.com/rss/search?q=Samsung+memory+semiconductor&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
        {'label': 'Micron EN',           'url': 'https://news.google.com/rss/search?q=Micron+Technology+memory&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
        {'label': 'SK Hynix EN',         'url': 'https://news.google.com/rss/search?q=SK+Hynix+memory+semiconductor&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
        {'label': 'Kioxia EN',           'url': 'https://news.google.com/rss/search?q=Kioxia+flash+storage&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
        {'label': 'SanDisk WD EN',       'url': 'https://news.google.com/rss/search?q=SanDisk+OR+Western+Digital+NAND+flash&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
        # ─── DRAM 市場趨勢 ───
        {'label': 'DRAM Market Trend',   'url': 'https://news.google.com/rss/search?q=DRAM+market+trend+price+supply&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
        {'label': 'DRAM Industry',       'url': 'https://news.google.com/rss/search?q=DRAM+industry+demand+outlook&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
        # ─── Flash 市場趨勢 ───
        {'label': 'NAND Flash Trend',    'url': 'https://news.google.com/rss/search?q=NAND+Flash+market+trend+price&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
        {'label': 'Flash Storage Market','url': 'https://news.google.com/rss/search?q=flash+storage+market+outlook+supply&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
        # ─── Transcend Information ───
        {'label': 'Transcend Info EN',   'url': 'https://news.google.com/rss/search?q="Transcend+Information"&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
        {'label': 'Transcend Memory EN', 'url': 'https://news.google.com/rss/search?q=Transcend+memory+storage+flash&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
    ]
    competitors = [
        # ─── 競品：僅中文 ───
        {'label': 'ADATA 威剛',         'url': 'https://news.google.com/rss/search?q=威剛+ADATA&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'competitor', 'brand': 'ADATA'},
        {'label': 'Innodisk 宜鼎',      'url': 'https://news.google.com/rss/search?q=宜鼎+Innodisk&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'competitor', 'brand': 'Innodisk'},
        {'label': 'Apacer 宇瞻',        'url': 'https://news.google.com/rss/search?q=宇瞻+Apacer&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'competitor', 'brand': 'Apacer'},
        {'label': 'Silicon Power 廣穎', 'url': 'https://news.google.com/rss/search?q=廣穎+Silicon+Power&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'competitor', 'brand': 'Silicon Power'},
        {'label': 'Kingston 金士頓',    'url': 'https://news.google.com/rss/search?q=金士頓+Kingston&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'competitor', 'brand': 'Kingston'},
        {'label': 'Lexar 雷克沙',       'url': 'https://news.google.com/rss/search?q=Lexar+雷克沙&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'competitor', 'brand': 'Lexar'},
        {'label': 'PNY 必恩威',         'url': 'https://news.google.com/rss/search?q=PNY+必恩威&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'competitor', 'brand': 'PNY'},
    ]
    suppliers = [
        # ─── 供應商：僅中文 ───
        {'label': '三星 Samsung',    'url': 'https://news.google.com/rss/search?q=三星+Samsung+半導體+記憶體&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'Samsung'},
        {'label': '美光 Micron',     'url': 'https://news.google.com/rss/search?q=美光+Micron+記憶體&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'Micron'},
        {'label': 'SK 海力士',       'url': 'https://news.google.com/rss/search?q=SK+海力士+Hynix+記憶體&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'SK Hynix'},
        {'label': 'Kioxia 鎧俠',     'url': 'https://news.google.com/rss/search?q=Kioxia+鎧俠+快閃+儲存&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'Kioxia'},
        {'label': 'SanDisk 威騰',    'url': 'https://news.google.com/rss/search?q=SanDisk+威騰+儲存&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'SanDisk/WD'},
        {'label': '慧榮 SMI',        'url': 'https://news.google.com/rss/search?q=慧榮+Silicon+Motion&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'SMI'},
        {'label': '群聯 Phison',     'url': 'https://news.google.com/rss/search?q=群聯+Phison&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'Phison'},
        {'label': '瑞昱 Realtek',    'url': 'https://news.google.com/rss/search?q=瑞昱+Realtek&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'Realtek'},
    ]

    community = [
        # ─── 社群觀測（PTT Stock，過濾含創見/2451的討論）───
        {'label': 'PTT Stock-創見', 'url': 'https://www.ptt.cc/bbs/Stock/index.rss', 'cat': 'community', 'filter': '創見'},
        {'label': 'PTT Stock-2451', 'url': 'https://www.ptt.cc/bbs/Stock/index.rss', 'cat': 'community', 'filter': '2451'},
    ]

    if mode == 'morning':
        return transcend + competitors + suppliers + community
    elif mode == 'afternoon':
        return us_market + competitors + suppliers + community
    else:  # all
        return transcend + us_market + competitors + suppliers + community


def analyze_sentiment(title, content=''):
    text = (title + ' ' + content).lower()
    pos = sum(1 for k in POS_KW if k.lower() in text)
    neg = sum(1 for k in NEG_KW if k.lower() in text)
    if pos > neg:
        return 'positive'
    elif neg > pos:
        return 'negative'
    return 'neutral'


def extract_media_from_title(title):
    """從標題後綴提取媒體名稱（Google News 格式：「標題 - 媒體名稱」）"""
    import re
    if not title:
        return None
    match = re.search(r'[-–—]\s*([^-–—]{2,40})\s*$', title)
    if not match:
        return None
    candidate = match.group(1).strip()
    for name in TAIWAN_MEDIA.values():
        if candidate == name or name in candidate or candidate in name:
            return name
    if 2 <= len(candidate) <= 30 and not candidate.isdigit():
        return candidate
    return None


def get_media_name(entry, link='', title=''):
    # 1. 優先從標題後綴解析（Google News 格式）
    from_title = extract_media_from_title(title)
    if from_title:
        return from_title

    # 2. 從 author 欄位比對
    author = getattr(entry, 'author', '') or ''
    if author and len(author) < 40 and '@' not in author and 'google' not in author.lower():
        for name in TAIWAN_MEDIA.values():
            if name in author or author in name:
                return name
        if len(author) >= 2 and not author.startswith('http'):
            return author

    # 3. 從連結 domain 比對（排除 news.google.com）
    try:
        from urllib.parse import urlparse
        domain = urlparse(link).netloc.replace('www.', '')
        if domain and 'google' not in domain:
            for key, name in TAIWAN_MEDIA.items():
                if key in domain or domain in key:
                    return name
            return domain
    except Exception:
        pass

    return '未知媒體'


def make_article_id(link, title):
    raw = (link or title or '') + 'v1'
    return hashlib.md5(raw.encode('utf-8')).hexdigest()[:20]


def parse_date(entry):
    """Try multiple date fields, return datetime object."""
    for field in ['published_parsed', 'updated_parsed', 'created_parsed']:
        val = getattr(entry, field, None)
        if val:
            try:
                return datetime.datetime(*val[:6], tzinfo=datetime.timezone.utc)
            except Exception:
                pass
    return datetime.datetime.now(datetime.timezone.utc)


def clean_html(text):
    import re
    return re.sub(r'<[^>]+>', '', text or '').strip()


def extract_reporter(raw_author):
    """嘗試從作者欄位辨識個人記者姓名（排除媒體機構名稱）"""
    if not raw_author:
        return None
    a = raw_author.strip()
    if not a or len(a) < 2 or len(a) > 15 or '@' in a or 'http' in a:
        return None
    # 排除已知媒體名稱
    for name in TAIWAN_MEDIA.values():
        if a == name or name in a or a in name:
            return None
    # 中文人名（2-4字）或英文人名（含空格大寫開頭）
    import re
    is_cn = bool(re.match(r'^[\u4e00-\u9fa5]{2,4}$', a))
    is_en = bool(re.match(r'^[A-Z][a-z]+([ ][A-Z][a-z]+)+$', a))
    return a if (is_cn or is_en) else None


def fetch_source(src, retry=2):
    for attempt in range(retry + 1):
        try:
            feed = feedparser.parse(src['url'])
            items = []
            kw_filter = src.get('filter')
            for entry in feed.entries[:30]:
                title = clean_html(getattr(entry, 'title', ''))
                if not title:
                    continue
                if kw_filter and kw_filter not in title:
                    continue
                link = getattr(entry, 'link', '') or getattr(entry, 'id', '')
                content = clean_html(getattr(entry, 'summary', '') or getattr(entry, 'description', ''))[:500]
                pub_date = parse_date(entry)

                # ─── 60 天舊文過濾：跳過超過 60 天的文章 ───
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                if pub_date and (now_utc - pub_date).days > 60:
                    continue

                raw_author = (getattr(entry, 'author', '') or '').strip()
                media_name = get_media_name(entry, link, title)

                # 去除標題後面的「- 媒體名稱」（Google News 格式）
                import re as _re
                suffix_match = _re.search(r'\s*[-–—]\s*([^-–—]{2,40})\s*$', title)
                if suffix_match:
                    removed = suffix_match.group(1).strip()
                    if media_name != '未知媒體' and (removed == media_name or media_name in removed or removed in media_name):
                        title = title[:suffix_match.start()].strip()

                article = {
                    'id': make_article_id(link, title),
                    'title': title,
                    'content': content,
                    'link': link,
                    'pubDate': pub_date,
                    'sentiment': analyze_sentiment(title, content),
                    'cat': src['cat'],
                    'brand': src.get('brand'),
                    'sourceName': src['label'],
                    'mediaName': media_name,
                    'rawAuthor': raw_author,
                    'fetchedAt': datetime.datetime.now(datetime.timezone.utc),
                    'fetchMode': os.environ.get('FETCH_MODE', 'all'),
                }
                items.append(article)
            print(f"  ✓ {src['label']}: {len(items)} 則新聞")
            return items
        except Exception as e:
            if attempt < retry:
                print(f"  ⚠ {src['label']} 失敗 (嘗試 {attempt+1}/{retry}): {e}")
                time.sleep(2)
            else:
                print(f"  ✗ {src['label']} 最終失敗: {e}")
                return []


def save_to_firestore(db, articles):
    """批次寫入 Firestore，每 400 筆一批"""
    batch_size = 400
    saved = 0
    for i in range(0, len(articles), batch_size):
        batch = db.batch()
        chunk = articles[i:i + batch_size]
        for a in chunk:
            ref = db.collection('news').document(a['id'])
            batch.set(ref, a, merge=True)
        batch.commit()
        saved += len(chunk)
    return saved


def fetch_stock_prices(db):
    """抓取台股行情（使用台灣證交所官方 API）並存入 Firebase stocks/latest"""
    # tse = 上市（TWSE），otc = 上櫃（TPEx）
    # exchange: 'tse'=上市, 'otc'=上櫃, 'auto'=自動偵測(同時查兩個交易所)
    STOCKS = {
        '2451': ('創見資訊',  'tse'),   # 上市
        '3260': ('威剛科技',  'auto'),  # 自動偵測
        '6248': ('廣穎電通',  'auto'),  # 自動偵測
        '5483': ('宜鼎國際',  'auto'),  # 自動偵測
        '4967': ('十銓科技',  'tse'),   # 上市
    }
    print("\n📈 抓取台股行情（台灣證交所）...")
    # auto 模式：同時帶入 tse_ 和 otc_ 前綴，API 會自動忽略不存在的那個
    all_codes = []
    for code, (_, ex) in STOCKS.items():
        if ex == 'auto':
            all_codes.append(f"tse_{code}.tw")
            all_codes.append(f"otc_{code}.tw")
        else:
            all_codes.append(f"{ex}_{code}.tw")
    ex_ch = '|'.join(all_codes)
    url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}&json=1&delay=0"
    try:
        r = requests.get(url, headers={
            'Referer': 'https://mis.twse.com.tw/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }, timeout=15)
        items = r.json().get('msgArray', [])
    except Exception as e:
        print(f"  ⚠ TWSE API 失敗: {e}")
        return

    stock_data = {}
    seen_codes = set()   # 防止 tse/otc 重複
    for item in items:
        code = item.get('c', '')
        if code not in STOCKS or code in seen_codes:
            continue
        seen_codes.add(code)
        name = STOCKS[code][0]
        # z = 當前成交價（非交易時間為 '-'），y = 昨日收盤
        z_raw = item.get('z', '-')
        y_raw = item.get('y', '0')
        try:
            y = float(y_raw) if y_raw and y_raw != '-' else 0
            price = float(z_raw) if z_raw and z_raw != '-' else y
        except (ValueError, TypeError):
            price, y = 0, 0
        change = round(price - y, 2) if y else 0
        pct = round(change / y * 100, 2) if y else 0
        try:
            vol = int(str(item.get('v', '0')).replace(',', '') or 0) * 1000
        except (ValueError, TypeError):
            vol = 0
        stock_data[code] = {
            'name': name,
            'price': round(price, 2),
            'change': change,
            'changePct': pct,
            'volume': vol,
            'updatedAt': firestore.SERVER_TIMESTAMP,
        }
        sign = '+' if change >= 0 else ''
        print(f"  {code} {name}: ${price:.1f} ({sign}{pct:.2f}%)")

    if stock_data:
        db.collection('stocks').document('latest').set(stock_data, merge=True)
        print(f"  ✅ 股價已存入 Firebase ({len(stock_data)} 檔)")
    else:
        print("  ⚠ 未取得任何股價（可能為非交易時間，下次交易時段後會自動更新）")


def main():
    mode = os.environ.get('FETCH_MODE', 'all')
    print(f"\n{'='*50}")
    print(f"創見資訊新聞監控 — 自動抓取")
    print(f"模式: {mode} | 時間: {datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')} (台灣時間)")
    print(f"{'='*50}")

    # ─── Firebase 初始化 ───
    sa_key = os.environ.get('FIREBASE_SERVICE_ACCOUNT')
    if not sa_key:
        print("❌ 找不到 FIREBASE_SERVICE_ACCOUNT 環境變數")
        sys.exit(1)

    # 支援兩種格式：原始 JSON 或 Base64 編碼的 JSON
    import base64
    sa_json = sa_key.strip()
    if not sa_json.startswith('{'):
        print("🔄 偵測到 Base64 格式，正在解碼...")
        try:
            sa_json = base64.b64decode(sa_json).decode('utf-8')
            print("✅ Base64 解碼成功")
        except Exception as e:
            print(f"❌ Base64 解碼失敗: {e}")
            sys.exit(1)

    try:
        sa_dict = json.loads(sa_json)
        cred = credentials.Certificate(sa_dict)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("✅ Firebase Firestore 已連線\n")
    except json.JSONDecodeError as e:
        print(f"❌ JSON 格式錯誤（行 {e.lineno} 欄 {e.colno}）: {e.msg}")
        print("💡 建議到 base64encode.org 將 JSON 轉為 Base64 後再貼入 GitHub Secret")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Firebase 初始化失敗: {e}")
        sys.exit(1)

    # ─── 抓取新聞 ───
    sources = get_sources(mode)
    print(f"📡 開始抓取 {len(sources)} 個來源...\n")

    all_articles = []
    seen_links = set()

    seen_titles = set()
    for src in sources:
        articles = fetch_source(src)
        for a in articles:
            import re as _re
            norm_title = _re.sub(r'[\s\W]+', '', (a.get('title') or '')).lower()[:30]
            if norm_title and norm_title in seen_titles:
                continue
            if a['link'] and a['link'] in seen_links:
                continue
            seen_titles.add(norm_title)
            seen_links.add(a['link'])
            all_articles.append(a)

    print(f"\n📊 共抓取 {len(all_articles)} 則不重複新聞")

    # ─── 儲存到 Firestore ───
    if all_articles:
        print(f"\n💾 儲存到 Firebase Firestore...")
        saved = save_to_firestore(db, all_articles)
        print(f"✅ 成功儲存 {saved} 則新聞")
    else:
        print("⚠ 沒有新聞可儲存")

    # ─── 情緒統計 ───
    pos = sum(1 for a in all_articles if a['sentiment'] == 'positive')
    neg = sum(1 for a in all_articles if a['sentiment'] == 'negative')
    neu = sum(1 for a in all_articles if a['sentiment'] == 'neutral')
    print(f"\n📈 情緒分佈: 正面 {pos} / 負面 {neg} / 中立 {neu}")

    # ─── 股價抓取 ───
    fetch_stock_prices(db)

    print(f"\n{'='*50}")
    print("抓取完成！")
    print(f"{'='*50}\n")


if __name__ == '__main__':
    main()

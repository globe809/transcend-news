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
    'anue.com.tw': '鉅亨網', 'cnyes.com': '鉅亨網',
    'ltn.com.tw': '自由時報', 'setn.com': '三立新聞', 'tvbs.com.tw': 'TVBS',
    'ettoday.net': 'ETtoday', 'storm.mg': '風傳媒', 'businessweekly.com.tw': '商業週刊',
    'cw.com.tw': '天下雜誌', 'bnext.com.tw': 'Meet 創業小聚', 'inside.com.tw': 'INSIDE',
    'moneydj.com': '精實財經(MoneyDJ)', 'stockfeel.com.tw': '股感',
    'nownews.com': 'NOWnews', 'mirrormedia.mg': '鏡週刊', 'ctinews.com': '中天新聞',
    # 新增媒體
    'cna.com.tw': '中央社',
    'wealth.com.tw': '財訊雙周刊',
    'nextapple.com': '壹蘋新聞網',
    'gvm.com.tw': '遠見',
    'ustv.com.tw': '非凡財經',
    'trendforce.com': '集邦科技(TrendForce)',
    'pcdiy.com.tw': 'PC DIY',
    'imageinfo.com.tw': 'Image Media',
    'ioiotimes.com': 'ioio Times',
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
    ]
    us_market = [
        {'label': 'DRAM Market', 'url': 'https://news.google.com/rss/search?q=DRAM+memory+market&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
        {'label': 'NAND Flash', 'url': 'https://news.google.com/rss/search?q=NAND+Flash+storage+market&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
        {'label': 'Memory Chip', 'url': 'https://news.google.com/rss/search?q=memory+chip+semiconductor+market&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
        {'label': 'Flash Storage', 'url': 'https://news.google.com/rss/search?q=flash+storage+industry+supply&hl=en&gl=US&ceid=US:en', 'cat': 'usMarket'},
    ]
    competitors = [
        # ─── 競品：英文新聞 ───
        {'label': 'ADATA EN',          'url': 'https://news.google.com/rss/search?q=ADATA+memory+storage&hl=en&gl=US&ceid=US:en', 'cat': 'competitor', 'brand': 'ADATA'},
        {'label': 'Innodisk EN',       'url': 'https://news.google.com/rss/search?q=Innodisk+industrial+flash+storage&hl=en&gl=US&ceid=US:en', 'cat': 'competitor', 'brand': 'Innodisk'},
        {'label': 'Apacer EN',         'url': 'https://news.google.com/rss/search?q=Apacer+memory+flash+storage&hl=en&gl=US&ceid=US:en', 'cat': 'competitor', 'brand': 'Apacer'},
        {'label': 'Silicon Power EN',  'url': 'https://news.google.com/rss/search?q=Silicon+Power+memory+flash&hl=en&gl=US&ceid=US:en', 'cat': 'competitor', 'brand': 'Silicon Power'},
        {'label': 'Kingston EN',       'url': 'https://news.google.com/rss/search?q=Kingston+Technology+memory+flash&hl=en&gl=US&ceid=US:en', 'cat': 'competitor', 'brand': 'Kingston'},
        {'label': 'Lexar EN',          'url': 'https://news.google.com/rss/search?q=Lexar+memory+card+flash+storage&hl=en&gl=US&ceid=US:en', 'cat': 'competitor', 'brand': 'Lexar'},
        {'label': 'PNY EN',            'url': 'https://news.google.com/rss/search?q=PNY+Technologies+flash+memory&hl=en&gl=US&ceid=US:en', 'cat': 'competitor', 'brand': 'PNY'},
        # ─── 競品：中文新聞 ───
        {'label': 'ADATA 威剛 TW',     'url': 'https://news.google.com/rss/search?q=ADATA+威剛&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'competitor', 'brand': 'ADATA'},
        {'label': 'Innodisk 宜鼎 TW',  'url': 'https://news.google.com/rss/search?q=Innodisk+宜鼎&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'competitor', 'brand': 'Innodisk'},
        {'label': 'Apacer 宇瞻 TW',    'url': 'https://news.google.com/rss/search?q=Apacer+宇瞻&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'competitor', 'brand': 'Apacer'},
        {'label': 'Silicon Power 廣穎 TW', 'url': 'https://news.google.com/rss/search?q=廣穎+Silicon+Power&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'competitor', 'brand': 'Silicon Power'},
        {'label': 'Kingston 金士頓 TW', 'url': 'https://news.google.com/rss/search?q=金士頓+Kingston&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'competitor', 'brand': 'Kingston'},
        {'label': 'Lexar TW',          'url': 'https://news.google.com/rss/search?q=Lexar+記憶卡+隨身碟&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'competitor', 'brand': 'Lexar'},
    ]
    suppliers = [
        # ─── DRAM 上游：英文新聞 ───
        {'label': 'Samsung DRAM EN',   'url': 'https://news.google.com/rss/search?q=Samsung+DRAM+memory+DDR5&hl=en&gl=US&ceid=US:en', 'cat': 'supplier', 'brand': 'Samsung'},
        {'label': 'Micron DRAM EN',    'url': 'https://news.google.com/rss/search?q=Micron+Technology+DRAM+memory&hl=en&gl=US&ceid=US:en', 'cat': 'supplier', 'brand': 'Micron'},
        {'label': 'SK Hynix EN',       'url': 'https://news.google.com/rss/search?q=SK+Hynix+DRAM+HBM+memory&hl=en&gl=US&ceid=US:en', 'cat': 'supplier', 'brand': 'SK Hynix'},
        # ─── DRAM 上游：中文新聞 ───
        {'label': 'Samsung 三星 TW',   'url': 'https://news.google.com/rss/search?q=三星+Samsung+DRAM+記憶體&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'Samsung'},
        {'label': 'Micron 美光 TW',    'url': 'https://news.google.com/rss/search?q=美光+Micron+DRAM&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'Micron'},
        {'label': 'SK Hynix TW',       'url': 'https://news.google.com/rss/search?q=SK+海力士+Hynix+記憶體&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'SK Hynix'},
        # ─── NAND Flash 上游：英文新聞 ───
        {'label': 'SanDisk NAND EN',   'url': 'https://news.google.com/rss/search?q=SanDisk+Western+Digital+NAND+Flash&hl=en&gl=US&ceid=US:en', 'cat': 'supplier', 'brand': 'SanDisk/WD'},
        {'label': 'Samsung NAND EN',   'url': 'https://news.google.com/rss/search?q=Samsung+NAND+Flash+V-NAND&hl=en&gl=US&ceid=US:en', 'cat': 'supplier', 'brand': 'Samsung'},
        {'label': 'Kioxia EN',         'url': 'https://news.google.com/rss/search?q=Kioxia+NAND+Flash+BiCS&hl=en&gl=US&ceid=US:en', 'cat': 'supplier', 'brand': 'Kioxia'},
        {'label': 'Micron NAND EN',    'url': 'https://news.google.com/rss/search?q=Micron+NAND+Flash+QLC+TLC&hl=en&gl=US&ceid=US:en', 'cat': 'supplier', 'brand': 'Micron'},
        # ─── NAND Flash 上游：中文新聞 ───
        {'label': 'SanDisk WD TW',     'url': 'https://news.google.com/rss/search?q=SanDisk+威騰+NAND+Flash&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'SanDisk/WD'},
        {'label': 'Samsung NAND TW',   'url': 'https://news.google.com/rss/search?q=三星+NAND+Flash+快閃記憶體&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'Samsung'},
        {'label': 'Kioxia TW',         'url': 'https://news.google.com/rss/search?q=Kioxia+鎧俠+NAND+Flash&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'Kioxia'},
        # ─── NAND Controller（主控晶片）───
        {'label': 'SMI 慧榮',          'url': 'https://news.google.com/rss/search?q=SMI+Silicon+Motion+慧榮&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'SMI'},
        {'label': 'Phison 群聯',       'url': 'https://news.google.com/rss/search?q=Phison+群聯+主控&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'Phison'},
        {'label': 'Realtek 瑞昱',      'url': 'https://news.google.com/rss/search?q=Realtek+瑞昱&hl=zh-TW&gl=TW&ceid=TW:zh-Hant', 'cat': 'supplier', 'brand': 'Realtek'},
        {'label': 'SMI EN',            'url': 'https://news.google.com/rss/search?q=Silicon+Motion+SMI+NAND+controller&hl=en&gl=US&ceid=US:en', 'cat': 'supplier', 'brand': 'SMI'},
        {'label': 'Phison EN',         'url': 'https://news.google.com/rss/search?q=Phison+Electronics+NAND+controller&hl=en&gl=US&ceid=US:en', 'cat': 'supplier', 'brand': 'Phison'},
    ]

    if mode == 'morning':
        return transcend + competitors + suppliers
    elif mode == 'afternoon':
        return us_market + competitors + suppliers
    else:  # all
        return transcend + us_market + competitors + suppliers


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

    for src in sources:
        articles = fetch_source(src)
        for a in articles:
            if a['link'] not in seen_links:
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
    print(f"\n{'='*50}")
    print("抓取完成！")
    print(f"{'='*50}\n")


if __name__ == '__main__':
    main()

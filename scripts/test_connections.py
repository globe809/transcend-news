"""
連線測試腳本
測試：
  1. Groq API（llama-3.1-8b-instant 摘要一則新聞，完全免費）
  2. Gmail SMTP（寄一封測試信）

執行方式：
  GROQ_API_KEY=xxx GMAIL_USER=xxx@gmail.com GMAIL_APP_PASSWORD=xxx \
  python scripts/test_connections.py

GitHub Actions 手動觸發：見 .github/workflows/test-connections.yml
"""

import os
import sys
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

GROQ_KEY   = os.environ.get('GROQ_API_KEY', '')
GMAIL_USER = os.environ.get('GMAIL_USER', '')
GMAIL_PW   = os.environ.get('GMAIL_APP_PASSWORD', '')
EMAIL_TO   = os.environ.get('EMAIL_RECIPIENT', GMAIL_USER) or 'elvis814@gmail.com'

results = {}

# ══════════════════════════════════════════════
# 測試 1：Google Gemini API
# ══════════════════════════════════════════════
print("\n" + "="*50)
print("測試 1：Groq API 連線")
print("="*50)

if not GROQ_KEY:
    print("  ✗ 未設定 GROQ_API_KEY")
    print("  💡 前往 https://console.groq.com 免費申請（不需信用卡）")
    results['groq'] = False
else:
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        resp = client.chat.completions.create(
            model='llama-3.1-8b-instant',
            messages=[
                {
                    'role': 'system',
                    'content': (
                        '你是半導體產業分析師。'
                        '請用繁體中文，以 2-3 個重點條列摘要以下英文新聞。'
                        '格式：•重點一 •重點二 •重點三（用 • 分隔，不要換行）'
                    )
                },
                {
                    'role': 'user',
                    'content': (
                        '標題：Micron Technology Reports Record Revenue Driven by AI Memory Demand\n'
                        '內文：Micron Technology announced record quarterly revenue of $8.7 billion, '
                        'driven by surging demand for high-bandwidth memory chips used in AI servers. '
                        'The company raised its outlook for the full year, citing strong orders from '
                        'major cloud providers.'
                    )
                }
            ],
            max_tokens=200,
            temperature=0.2,
        )
        summary = resp.choices[0].message.content.strip()
        model   = resp.model
        tokens  = resp.usage.total_tokens
        print(f"  ✅ 連線成功！")
        print(f"  模型：{model}")
        print(f"  消耗 Token：{tokens}")
        print(f"  摘要輸出：")
        for line in summary.split('•'):
            if line.strip():
                print(f"    • {line.strip()}")
        results['groq'] = True
    except Exception as e:
        print(f"  ✗ 失敗：{e}")
        results['groq'] = False


# ══════════════════════════════════════════════
# 測試 2：Gmail SMTP
# ══════════════════════════════════════════════
print("\n" + "="*50)
print("測試 2：Gmail SMTP 寄信")
print("="*50)

if not GMAIL_USER or not GMAIL_PW:
    print("  ✗ 未設定 GMAIL_USER 或 GMAIL_APP_PASSWORD")
    results['gmail'] = False
else:
    try:
        html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:sans-serif;max-width:500px;margin:40px auto;color:#111">
  <div style="background:#960014;color:white;padding:20px;border-radius:8px 8px 0 0">
    <h2 style="margin:0">✅ Gmail 連線測試成功</h2>
  </div>
  <div style="border:1px solid #e5e7eb;border-top:none;padding:20px;border-radius:0 0 8px 8px">
    <p>這是由 <strong>創見資訊（2451）新聞監控系統</strong> 發送的測試郵件。</p>
    <p>如果你看到這封信，代表 Gmail SMTP 設定正確，每日上游市場日報將可正常寄出。</p>
    <hr style="border:none;border-top:1px solid #f3f4f6">
    <p style="color:#9ca3af;font-size:12px">
      寄件帳號：{GMAIL_USER}<br>
      收件帳號：{EMAIL_TO}
    </p>
  </div>
</body></html>"""

        msg = MIMEMultipart('alternative')
        msg['Subject'] = '✅ 連線測試｜創見資訊新聞監控 Gmail SMTP'
        msg['From']    = GMAIL_USER
        msg['To']      = EMAIL_TO
        msg.attach(MIMEText(html, 'html', 'utf-8'))

        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=ctx) as server:
            server.login(GMAIL_USER, GMAIL_PW)
            server.send_message(msg)

        print(f"  ✅ 信件已寄出！")
        print(f"  寄件：{GMAIL_USER}")
        print(f"  收件：{EMAIL_TO}")
        print(f"  請至信箱確認是否收到測試信。")
        results['gmail'] = True
    except smtplib.SMTPAuthenticationError:
        print("  ✗ 帳號驗證失敗（SMTP 認證錯誤）")
        print("  💡 請確認：")
        print("     1. Gmail 是否已開啟「兩步驟驗證」")
        print("     2. GMAIL_APP_PASSWORD 是否為 App Password（不是 Gmail 登入密碼）")
        results['gmail'] = False
    except Exception as e:
        print(f"  ✗ 失敗：{e}")
        results['gmail'] = False


# ══════════════════════════════════════════════
# 總結
# ══════════════════════════════════════════════
print("\n" + "="*50)
print("測試結果總結")
print("="*50)
print(f"  Groq API  ：{'✅ 通過' if results.get('groq') else '✗ 失敗'}")
print(f"  Gmail SMTP：{'✅ 通過' if results.get('gmail') else '✗ 失敗'}")
print()

if not all(results.values()):
    sys.exit(1)   # 讓 GitHub Actions 顯示失敗狀態

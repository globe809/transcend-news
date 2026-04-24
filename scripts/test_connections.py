"""
連線測試腳本
測試：Gmail SMTP（寄一封測試信）+ Gemini API

執行方式：
  GMAIL_USER=xxx@gmail.com GMAIL_APP_PASSWORD=xxx GEMINI_API_KEY=xxx python scripts/test_connections.py

GitHub Actions 手動觸發：見 .github/workflows/test-connections.yml
"""

import os
import sys
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

GMAIL_USER  = os.environ.get('GMAIL_USER', '')
GMAIL_PW    = os.environ.get('GMAIL_APP_PASSWORD', '')
EMAIL_TO    = os.environ.get('EMAIL_RECIPIENT', GMAIL_USER) or 'elvis814@gmail.com'
GEMINI_KEY  = os.environ.get('GEMINI_API_KEY', '')

results = {}

# ══════════════════════════════════════════════
# 測試：Gemini API
# ══════════════════════════════════════════════
print("\n" + "="*50)
print("測試：Gemini API 摘要")
print("="*50)

if not GEMINI_KEY:
    print("  ✗ 未設定 GEMINI_API_KEY")
    results['gemini'] = False
else:
    try:
        from google import genai
        client = genai.Client(api_key=GEMINI_KEY)
        # 依序嘗試可用模型
        MODELS = ['gemini-2.0-flash', 'gemini-2.5-flash', 'gemini-2.0-flash-exp']
        success_model = None
        summary = None
        last_err = None
        for m in MODELS:
            try:
                resp = client.models.generate_content(
                    model=m,
                    contents=(
                        '你是半導體產業分析師。用繁體中文，2個重點條列摘要以下英文新聞：\n'
                        '標題：TrendForce: DRAM Prices Expected to Rise in Q3 2025\n'
                        '格式：•重點一 •重點二（用 • 分隔，不要換行）'
                    ),
                )
                summary = resp.text.strip()
                success_model = m
                break
            except Exception as e:
                last_err = e
                print(f"  模型 {m} 失敗：{e}")
        if success_model:
            print(f"  ✅ Gemini API 連線成功！")
            print(f"  模型：{success_model}")
            print(f"  摘要測試結果：{summary}")
        else:
            raise last_err
        results['gemini'] = True
    except Exception as e:
        print(f"  ✗ 失敗：{e}")
        if 'RESOURCE_EXHAUSTED' in str(e) or 'limit' in str(e).lower():
            print("  💡 額度不足，請確認：")
            print("     1. API Key 是否來自 aistudio.google.com")
            print("     2. 該 Google 帳號是否有啟用付費方案或 Gemini Advanced")
        elif 'API_KEY_INVALID' in str(e):
            print("  💡 API Key 無效，請重新到 aistudio.google.com 產生")
        results['gemini'] = False

# ══════════════════════════════════════════════
# 測試：Gmail SMTP
# ══════════════════════════════════════════════
print("\n" + "="*50)
print("測試：Gmail SMTP 寄信")
print("="*50)

if not GMAIL_USER or not GMAIL_PW:
    print("  ✗ 未設定 GMAIL_USER 或 GMAIL_APP_PASSWORD")
    results['gmail'] = False
else:
    try:
        gemini_status = '✅ 通過' if results.get('gemini') else '✗ 未測試或失敗'
        html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:sans-serif;max-width:500px;margin:40px auto;color:#111">
  <div style="background:#960014;color:white;padding:20px;border-radius:8px 8px 0 0">
    <h2 style="margin:0">✅ 連線測試成功</h2>
  </div>
  <div style="border:1px solid #e5e7eb;border-top:none;padding:20px;border-radius:0 0 8px 8px">
    <p>這是由 <strong>創見資訊（2451）新聞監控系統</strong> 發送的測試郵件。</p>
    <table style="width:100%;border-collapse:collapse;margin-top:12px">
      <tr style="background:#f9fafb">
        <td style="padding:8px 12px;border:1px solid #e5e7eb">Gmail SMTP</td>
        <td style="padding:8px 12px;border:1px solid #e5e7eb">✅ 通過</td>
      </tr>
      <tr>
        <td style="padding:8px 12px;border:1px solid #e5e7eb">Gemini API</td>
        <td style="padding:8px 12px;border:1px solid #e5e7eb">{gemini_status}</td>
      </tr>
    </table>
    <hr style="border:none;border-top:1px solid #f3f4f6;margin-top:16px">
    <p style="color:#9ca3af;font-size:12px">
      寄件帳號：{GMAIL_USER}<br>
      收件帳號：{EMAIL_TO}
    </p>
  </div>
</body></html>"""

        msg = MIMEMultipart('alternative')
        msg['Subject'] = '✅ 連線測試｜創見資訊新聞監控'
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
print("測試結果")
print("="*50)
print(f"  Gemini API：{'✅ 通過' if results.get('gemini') else '✗ 失敗'}")
print(f"  Gmail SMTP：{'✅ 通過' if results.get('gmail') else '✗ 失敗'}")
print()

if not all(results.values()):
    sys.exit(1)

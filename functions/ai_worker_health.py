"""
本機 AI worker（tools/local_ai_worker.py）健康檢查。

該 worker 跑在使用者自己的電腦上（讀 ai_jobs、呼叫本機 Ollama、寫回
ai_insights），Cloud Functions 完全看不到它有沒有在跑——如果它停了，
ai_jobs 會無聲無息一直堆積，沒有任何排程會失敗、也不會有錯誤訊息。

這個檢查本身跑在 Cloud Functions（見 main.py 的 ai_worker_health_job），
只做唯讀彙總、寫進客戶端可讀的 job_status/ai_worker，讓前端「系統健康」
頁面就算 worker 完全沒在跑，也能看到「待處理筆數持續增加、最近一次
分析是很久以前」這個信號，不需要等使用者自己發現。
"""

from firebase_admin import firestore


def check_ai_worker_health(db):
    pending = db.collection('ai_jobs').where('status', '==', 'pending').count().get()[0][0].value

    last_insight_at = None
    for doc in (db.collection('ai_insights')
                .order_by('analyzedAt', direction=firestore.Query.DESCENDING)
                .limit(1).stream()):
        last_insight_at = doc.to_dict().get('analyzedAt')

    db.collection('job_status').document('ai_worker').set({
        'pendingCount': pending,
        'lastInsightAt': last_insight_at,
        'checkedAt': firestore.SERVER_TIMESTAMP,
    }, merge=True)

    print(f"  🤖 本機 AI worker 健康檢查：待處理 {pending} 筆，最近一次分析 {last_insight_at}")
    return {'pendingCount': pending, 'lastInsightAt': last_insight_at}

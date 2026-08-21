"""
functions/ai_worker_health.py 單元測試 — 完全離線，firebase_admin 以 stub 取代。
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

for _mod in ('firebase_admin', 'firebase_admin.credentials', 'firebase_admin.firestore'):
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock(name=_mod)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'functions'))

import ai_worker_health  # noqa: E402


class TestCheckAiWorkerHealth(unittest.TestCase):
    def _mock_db(self, pending_count, last_insight):
        db = MagicMock(name='db')
        count_result = MagicMock()
        count_result.get.return_value = [[MagicMock(value=pending_count)]]
        db.collection.return_value.where.return_value.count.return_value = count_result

        if last_insight is not None:
            insight_doc = MagicMock()
            insight_doc.to_dict.return_value = {'analyzedAt': last_insight}
            stream_result = [insight_doc]
        else:
            stream_result = []
        db.collection.return_value.order_by.return_value.limit.return_value.stream.return_value = stream_result
        return db

    def test_writes_pending_count_and_last_insight_and_returns_summary(self):
        db = self._mock_db(pending_count=42, last_insight='FAKE_TIMESTAMP')

        result = ai_worker_health.check_ai_worker_health(db)

        self.assertEqual(result['pendingCount'], 42)
        self.assertEqual(result['lastInsightAt'], 'FAKE_TIMESTAMP')
        db.collection.return_value.where.assert_called_with('status', '==', 'pending')
        status_ref = db.collection.return_value.document.return_value
        payload = status_ref.set.call_args.args[0]
        self.assertEqual(payload['pendingCount'], 42)
        self.assertEqual(payload['lastInsightAt'], 'FAKE_TIMESTAMP')
        self.assertIn('checkedAt', payload)

    def test_reports_zero_backlog_and_no_insight_yet_without_crashing(self):
        db = self._mock_db(pending_count=0, last_insight=None)

        result = ai_worker_health.check_ai_worker_health(db)

        self.assertEqual(result['pendingCount'], 0)
        self.assertIsNone(result['lastInsightAt'])


if __name__ == '__main__':
    unittest.main()

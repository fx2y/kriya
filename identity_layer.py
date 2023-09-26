import hashlib
import hmac
import sqlite3


class IdentityLayer:
    def __init__(self, db_file):
        self.db_file = db_file

    def verify_access_key(self, access_key, secret_key):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()

        c.execute('SELECT secret_key FROM access_keys WHERE access_key = ?', (access_key,))
        row = c.fetchone()

        if row is None:
            return False

        expected_signature = hmac.new(row[0].encode('utf-8'), access_key.encode('utf-8'), hashlib.sha256).hexdigest()
        actual_signature = hmac.new(secret_key.encode('utf-8'), access_key.encode('utf-8'), hashlib.sha256).hexdigest()

        conn.close()

        return hmac.compare_digest(expected_signature, actual_signature)

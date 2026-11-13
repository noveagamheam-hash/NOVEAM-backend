import ast
import base64
import hashlib
import hmac
import importlib.util
import json
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
import secrets
import sys
from types import ModuleType, SimpleNamespace
from typing import Optional
from unittest.mock import patch
import xml.etree.ElementTree as ET
from cryptography.fernet import Fernet

class HTTPException(Exception):
    def __init__(self, status_code, detail):
        self.status_code = status_code
        self.detail = detail

stub = ModuleType('fastapi')
stub.HTTPException = HTTPException
sys.modules['fastapi'] = stub
folder = Path('backend/backend-python')
spec = importlib.util.spec_from_file_location('account_security', folder/'account_security.py')
security = importlib.util.module_from_spec(spec)
spec.loader.exec_module(security)
os.environ['NOVEAM_MFA_KEY'] = Fernet.generate_key().decode()

def expect(code, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except HTTPException as exc:
        assert exc.status_code == code, (exc.status_code, code)
    else:
        raise AssertionError(f'Expected {code}')

original = 'GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ'
encrypted = security.encrypt(original)
assert encrypted != original and security.decrypt(encrypted) == original
assert security.decrypt(original) == original
expect(503, security.decrypt, 'fernet:invalid')
with patch.dict(os.environ, {'NOVEAM_MFA_KEY': 'bad'}):
    expect(503, security.encrypt, original)
qr = security.qr_code(original, 'test&/user')
assert qr.startswith('data:image/svg+xml;base64,')
svg = ET.fromstring(base64.b64decode(qr.split(',')[1]))
assert svg.tag.endswith('svg') and len(list(svg.iter())) > 100

class Conn:
    def __init__(self):
        self.codes = set()
        self.sessions = {}
        self.result = None
    def execute(self, query, params):
        if query.startswith('INSERT INTO sessions'):
            self.sessions[params[0]] = {'mfa_verified': params[-1], 'expires_at': params[2]}
        if 'FROM sessions s JOIN' in query:
            row = self.sessions.get(params[0])
            self.result = row if row and row['expires_at'] > params[1] else None
        if 'DELETE FROM mfa_recovery_codes' in query:
            if len(params) == 1:
                self.codes = {item for item in self.codes if item[0] != params[0]}
            else:
                self.result = params if params in self.codes else None
                self.codes.discard(params)
        return self
    def fetchone(self):
        return self.result
    @contextmanager
    def cursor(self):
        yield self
    def executemany(self, query, rows):
        self.codes.update(rows)

conn = Conn()
codes = security.replace_recovery(conn, 1)
assert len(set(codes)) == 10
assert all(len(code) == 20 and (1, security.digest(code)) in conn.codes for code in codes)
assert not security.consume_recovery(conn, 2, codes[0])
assert security.consume_recovery(conn, 1, codes[0].upper())
assert not security.consume_recovery(conn, 1, codes[0])
new_codes = security.replace_recovery(conn, 1)
assert not security.consume_recovery(conn, 1, codes[1])
assert security.consume_recovery(conn, 1, new_codes[1])

@contextmanager
def db():
    yield conn

tree = ast.parse((folder/'main.py').read_text(encoding='utf-8'))
names = {'create_session','authenticate_session','matching_totp_counter','_totp_code'}
funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
assert len(funcs) == len(names)
for fn in funcs:
    fn.decorator_list = []
ns = {**globals(), 'account_security': security, 'policy_safety': SimpleNamespace(ENABLED=False)}
exec(compile(ast.Module(body=funcs, type_ignores=[]), 'selected actual backend functions', 'exec'), ns)
create = ns['create_session']
authenticate = ns['authenticate_session']
pending = create(1)
assert pending not in conn.sessions
assert authenticate('Bearer '+pending, enrollment=True)[0]['mfa_verified'] is False
expect(403, authenticate, 'Bearer '+pending, protected=True)
legacy = secrets.token_urlsafe(40)
conn.sessions[legacy] = {'mfa_verified':False, 'expires_at':'9999'}
expect(401, authenticate, 'Bearer '+legacy, enrollment=True)
for remember, days in [(True,30),(False,1)]:
    verified = create(1, verified=True, remember=remember)
    row, db_id = authenticate('Bearer '+verified, protected=True)
    assert db_id == security.digest(verified)
    duration = datetime.fromisoformat(row['expires_at']) - datetime.now(timezone.utc)
    assert timedelta(days=days, seconds=-2) < duration <= timedelta(days=days)
    del conn.sessions[db_id]
    expect(401, authenticate, 'Bearer '+verified)
assert ns['_totp_code'](original, datetime.fromtimestamp(59, timezone.utc)) == '287082'
assert ns['matching_totp_counter'](original, ns['_totp_code'](original)) is not None
assert ns['matching_totp_counter'](original, '１２３４５６') is None

payload = SimpleNamespace(identifier='tester', username='', captcha_token='')
request = SimpleNamespace(client=SimpleNamespace(host='127.0.0.1'))
config = {'NOVEAM_TURNSTILE_SITE_KEY':'public', 'NOVEAM_TURNSTILE_SECRET':'private',
          'NOVEAM_TURNSTILE_HOSTNAME':'staging.example.test'}
with patch.dict(os.environ, config):
    with patch.object(security, 'count_attempts', return_value=[2,2]):
        security.check_login_risk(db, request, payload)
    with patch.object(security, 'count_attempts', return_value=[2,4]):
        expect(403, security.check_login_risk, db, request, payload)
        payload.captcha_token = 'fake-token'
        for result, accepted in [
            ({'success':True,'action':'login','hostname':config['NOVEAM_TURNSTILE_HOSTNAME']}, True),
            ({'success':True,'action':'signup','hostname':config['NOVEAM_TURNSTILE_HOSTNAME']}, False),
            ({'success':True,'action':'login','hostname':'elsewhere'}, False),
            ({'success':False}, False),
        ]:
            from io import BytesIO
            with patch.object(security, 'urlopen', return_value=BytesIO(json.dumps(result).encode())):
                if accepted:
                    security.check_login_risk(db, request, payload)
                else:
                    expect(403, security.check_login_risk, db, request, payload)
        with patch.object(security, 'urlopen', side_effect=TimeoutError):
            expect(503, security.check_login_risk, db, request, payload)
    with patch.object(security, 'count_attempts', return_value=[11]):
        expect(429, security.limit_factor_attempts, db, 1)
print('PASS TOTP RFC vector, QR SVG, Fernet, recovery replacement/single-use, session gate/revocation/TTL, CAPTCHA decisions/outage and MFA throttle (mocked DB/provider)')

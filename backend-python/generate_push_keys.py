"""Run locally once. Writes private values to .env.push, never to stdout."""
import base64
from pathlib import Path
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization

if __name__ == '__main__':
    path = Path('.env.push')
    key = ec.generate_private_key(ec.SECP256R1())
    private = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    public = key.public_key().public_bytes(serialization.Encoding.X962, serialization.PublicFormat.UncompressedPoint)
    encode = lambda value: base64.urlsafe_b64encode(value).decode().rstrip('=')
    with path.open('x', encoding='utf-8') as output:
        output.write('VAPID_PUBLIC_KEY=' + encode(public) + '\n')
        output.write('VAPID_PRIVATE_KEY=' + encode(private) + '\n')
        output.write('VAPID_SUBJECT=mailto:REPLACE_WITH_YOUR_CONTACT_EMAIL\n')
    print('Created .env.push. Copy its values into Render environment settings. Keep this file private; never upload it to Hostinger or GitHub.')

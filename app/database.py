import json

import sqlalchemy.types as types
from cryptography.fernet import Fernet


class Encrypted(types.TypeDecorator):

    impl = types.BLOB

    # TODO: should be secured in production
    ENCRYPTOR = Fernet(b"o78wbST5GH4zBfjZ1xwzyyamaKD2d9FFq12y0nXe4kY=")

    def process_bind_param(self, value, dialect):
        encrypted_value = self.ENCRYPTOR.encrypt(json.dumps(value).encode("utf-8"))
        return encrypted_value

    def process_result_value(self, value, dialect):
        decrypted_value = json.loads(self.ENCRYPTOR.decrypt(value))
        return decrypted_value

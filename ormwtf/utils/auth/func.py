import bcrypt

# ----------------------- #


def hash_secret(value: str):
    value_bytes = value.encode("utf-8")
    salt = bcrypt.gensalt()
    hashed_value = bcrypt.hashpw(password=value_bytes, salt=salt).decode("utf-8")

    return hashed_value


# ....................... #


def verify_secret(plain: str, hashed: str):
    plain_enc = plain.encode("utf-8")
    hashed_enc = hashed.encode("utf-8")

    return bcrypt.checkpw(password=plain_enc, hashed_password=hashed_enc)

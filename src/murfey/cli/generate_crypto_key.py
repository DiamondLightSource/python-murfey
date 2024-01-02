from cryptography.fernet import Fernet


def run():
    print(Fernet.generate_key().decode())

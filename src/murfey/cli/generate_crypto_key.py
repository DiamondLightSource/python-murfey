from cryptography import Fernet


def run():
    print(Fernet.generate_key().decode())

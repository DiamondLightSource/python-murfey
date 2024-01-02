from murfey.cli.generate_crypto_key import run


def test_crypto_key_generation(capsys):
    run()
    captured = capsys.readouterr()
    assert isinstance(captured.out, str)
    assert captured.out

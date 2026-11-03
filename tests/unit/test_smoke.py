import hookgate
import hookgate.domain


def test_package_is_importable() -> None:
    assert hookgate.__name__ == "hookgate"
    assert hookgate.domain.__name__ == "hookgate.domain"

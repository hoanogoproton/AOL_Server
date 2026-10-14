"""Kiem tra asset giao dien khong dung ban cache cu sau khi nang cap."""

from pathlib import Path


def test_index_gan_version_cho_css_va_javascript():
    """HTML phai ep trinh duyet tai lai CSS/JS khi giao dien thay doi."""
    index_html = (Path(__file__).parent.parent / "static" / "index.html").read_text(
        encoding="utf-8"
    )

    assert '/static/style.css?v=' in index_html
    assert '/static/script.js?v=' in index_html

from app.llm.pre_classifier import (
    decide_hybrid_classification,
    normalize_short_bench_line,
    pre_classify_bench_line,
    split_line_wise_bench_items,
)


def test_pre_classifier_positive_system_analyst():
    s = "Системный аналитик Даниил С. Middle+ 4.8 лет опыта, ставка - 2050₽ /cv_8xY2O"
    r = pre_classify_bench_line(s)
    assert r.is_confident
    assert r.kind == "BENCH"


def test_pre_classifier_positive_dotnet():
    s = ".Net Орхан С. Senior 5.6 лет опыта, ставка - 1650₽ /cv_D4k5n"
    r = pre_classify_bench_line(s)
    assert r.is_confident
    assert r.kind == "BENCH"


def test_pre_classifier_positive_java():
    s = "Java Михаил У. Middle+ 4.8 лет опыта, ставка - 2200₽ /cv_pY5XM"
    r = pre_classify_bench_line(s)
    assert r.is_confident
    assert r.kind == "BENCH"


def test_pre_classifier_negative_vacancy():
    s = "Нужен .NET Senior на проект, удалённо, 2200"
    r = pre_classify_bench_line(s)
    assert not r.is_confident


def test_pre_classifier_negative_other():
    s = "Всем привет"
    r = pre_classify_bench_line(s)
    assert not r.is_confident


def test_pre_classifier_closed_strikethrough_not_auto_bench():
    s = "~~Вакансия закрыта~~"
    r = pre_classify_bench_line(s)
    assert not r.is_confident


def test_normalization_short_bench_line():
    s = ".Net Иван Middle + 4,8 лет опыта, ставка - 2050₽ /cv_abc"
    n = normalize_short_bench_line(s)
    assert "DotNet" in n
    assert "4.8" in n
    assert "CV_CODE: cv_abc" in n


def test_line_wise_split_8_items():
    text = "\n".join(
        [
            "Системный аналитик Даниил С. Middle+ 4.8 лет опыта, ставка - 2050₽ /cv_8xY2O",
            "Системный аналитик Юлия А. Middle+ 4.9 лет опыта, ставка - 1750₽ /cv_povEq",
            "Системный аналитик Даниил Л. Middle+ 4.0 лет опыта, ставка - 1750₽ /cv_pYP9o",
            ".Net Орхан С. Senior 5.6 лет опыта, ставка - 1650₽ /cv_D4k5n",
            ".Net Алиса К. Middle 3.3 лет опыта, ставка - 1750₽ /cv_8xMKG",
            "Java Михаил У. Middle+ 4.8 лет опыта, ставка - 2200₽ /cv_pY5XM",
            "React Марк Б. Middle+ 5.1 лет опыта, ставка - 1500₽ /cv_pGoNj",
            "Python Даниил Б. Senior 5.3 лет опыта, ставка - 2100₽ /cv_D4bw2",
        ]
    )
    items = split_line_wise_bench_items(text)
    assert len(items) == 8


def test_hybrid_decision_skips_llm_when_rule_confident():
    pre = pre_classify_bench_line("Java Михаил У. Middle+ 4.8 лет опыта, ставка - 2200₽ /cv_pY5XM")
    d = decide_hybrid_classification(pre)
    assert not d.needs_llm
    assert d.kind == "BENCH"
    assert d.source == "rule"


def test_hybrid_decision_with_other_and_weak_signal_fallback():
    pre = pre_classify_bench_line("React Марк Б. Middle+ 5.1 лет опыта, ставка - 1500₽ /cv_pGoNj")
    d = decide_hybrid_classification(pre, llm_label="OTHER")
    assert d.kind in ("BENCH", "OTHER")

from api import parse_spec, Spec


def test_parsing():
    test_cases = [
        ("", None, Spec(kind="source", source=None, schema=None, table=None, column=None)),
        (":", None, Spec(kind="schema", source=None, schema=None, table=None, column=None)),
        ("::", None, Spec(kind="table", source=None, schema=None, table=None, column=None)),
        (":::", None, Spec(kind="column", source=None, schema=None, table=None, column=None)),
        ("a:::d", None, Spec(kind="column", source="a", schema=None, table=None, column="d")),
        ("a:b:c:d", None, Spec(kind="column", source="a", schema="b", table="c", column="d")),
        ("", "%", Spec(kind="source", source="%", schema="%", table="%", column="%")),
        ("a:b", "%", Spec(kind="schema", source="a", schema="b", table="%", column="%")),
        ("a:b:c", "%", Spec(kind="table", source="a", schema="b", table="c", column="%")),
        ("a:b:c:d", "%", Spec(kind="column", source="a", schema="b", table="c", column="d")),
        ("a:::d", "%", Spec(kind="column", source="a", schema="%", table="%", column="d")),
    ]
    for query, default, expected in test_cases:
        assert parse_spec(query, default=default) == expected
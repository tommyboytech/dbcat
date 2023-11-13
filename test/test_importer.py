import re

import pytest
import sqlalchemy

from api import import_from_object_stream, IntegrityError


def test_add_foreign_key(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        s1 = catalog.add_source("s1", "sqlite")
        sc1 = catalog.add_schema("sc1", s1)
        t1 = catalog.add_table("t1", sc1)
        t1_c1 = catalog.add_column("c1", "Int", 0, t1)
        t2 = catalog.add_table("t2", sc1)
        t2_c1 = catalog.add_column("t1c1", "Int", 1, t2)
        fk1 = catalog.add_foreign_key(t2_c1, t1_c1)
        assert fk1.fqdn == ("s1", "sc1", "t2", "t1c1", "s1", "sc1", "t1", "c1")


def test_foreign_key_must_be_unique(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with catalog.managed_session:
            s1 = catalog.add_source("s1", "sqlite")
            sc1 = catalog.add_schema("sc1", s1)
            t1 = catalog.add_table("t1", sc1)
            t1_c1 = catalog.add_column("c1", "Int", 0, t1)
            t2 = catalog.add_table("t2", sc1)
            t2_c1 = catalog.add_column("t1c1", "Int", 1, t2)
            catalog.add_foreign_key(t2_c1, t1_c1)
            catalog.add_foreign_key(t2_c1, t1_c1)


def test_get_foreign_key(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        s1 = catalog.add_source("s1", "sqlite")
        sc1 = catalog.add_schema("sc1", s1)
        t1 = catalog.add_table("t1", sc1)
        t1_c1 = catalog.add_column("c1", "Int", 0, t1)
        t2 = catalog.add_table("t2", sc1)
        t2_c1 = catalog.add_column("t1c1", "Int", 1, t2)
        catalog.add_foreign_key(t2_c1, t1_c1)
    with catalog.managed_session:
        test_cases = [
            (
                ("s1", "sc1", "t2", "t1c1"),
                ("s1", "sc1", "t1", "c1"),
                True,
            ),
            (
                ("s1", "sc1", "t1", "c1"),
                ("s1", "sc1", "t2", "t1c1"),
                False,
            ),
            (
                ("s1", "sc1", "t3", "t1c1"),
                ("s1", "sc1", "t1", "c1"),
                False,
            ),
            (
                ("s1", "sc1", "t2", "t1c1"),
                ("s1", "sc3", "t1", "c1"),
                False,
            ),
        ]
        for src, tgt, success in test_cases:
            assert success == bool(catalog.get_foreign_key(*(src + tgt)))


def test_import_foreign_key_fails(open_catalog_connection, caplog):
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        s1 = catalog.add_source("s1", "sqlite")
        sc1 = catalog.add_schema("sc1", s1)
        t1 = catalog.add_table("t1", sc1)
        catalog.add_column("c1", "Int", 0, t1)
        t2 = catalog.add_table("t2", sc1)
        catalog.add_column("t1c1", "Int", 1, t2)
    with catalog.managed_session:
        test_cases = [
            {
                "data": {
                    "source": {
                        "database": "s1",
                        "schema": "sc1",
                        "table": "t2",
                        "column": "t1c1",
                    },
                    "target": {
                        "database": "s1",
                        "schema": "sc1",
                        "table": "t1",
                        "column": "c1",
                    },
                },
                "exc": r"no 'type' field in object",
            }, {
                "data": {
                    "type": "foreign-key",
                    "source": {
                        "database": "s1",
                        "schema": "sc1",
                        "table": "t2",
                        "column": "t1c1",
                    },
                },
                "exc": r"no 'target' field in foreign-key",
            }, {
                "data": {
                    "type": "foreign-key",
                    "target": {
                        "database": "s1",
                        "schema": "sc1",
                        "table": "t1",
                        "column": "c1",
                    },
                },
                "exc": r"no 'source' field in foreign-key",
            }, {
                "data": {
                    "type": "foreign-key",
                    "source": {
                        "schema": "sc1",
                        "table": "t2",
                        "column": "t1c1",
                    },
                    "target": {
                        "database": "s1",
                        "schema": "sc1",
                        "table": "t1",
                        "column": "c1",
                    },
                },
                "exc": r"no 'database' field in 'source'",
            }, {
                "data": {
                    "type": "foreign-key",
                    "source": {
                        "database": "s1",
                        "table": "t2",
                        "column": "t1c1",
                    },
                    "target": {
                        "database": "s1",
                        "schema": "sc1",
                        "table": "t1",
                        "column": "c1",
                    },
                },
                "exc": r"no 'schema' field in 'source'",
            }, {
                "data": {
                    "type": "foreign-key",
                    "source": {
                        "database": "s1",
                        "schema": "sc1",
                        "column": "t1c1",
                    },
                    "target": {
                        "database": "s1",
                        "schema": "sc1",
                        "table": "t1",
                        "column": "c1",
                    },
                },
                "exc": r"no 'table' field in 'source'",
            }, {
                "data": {
                    "type": "foreign-key",
                    "source": {
                        "database": "s1",
                        "schema": "sc1",
                        "table": "t2",
                    },
                    "target": {
                        "database": "s1",
                        "schema": "sc1",
                        "table": "t1",
                        "column": "c1",
                    },
                },
                "exc": r"no 'column' field in 'source'",
            }, {
                "data": {
                    "type": "foreign-key",
                    "source": {
                        "database": "Xs1",
                        "schema": "sc1",
                        "table": "t2",
                        "column": "t1c1",
                    },
                    "target": {
                        "database": "s1",
                        "schema": "sc1",
                        "table": "t1",
                        "column": "c1",
                    },
                },
                "exc": r"'source' column \<Xs1, sc1, t2, t1c1\> does not exist",
            }, {
                "data": {
                    "type": "foreign-key",
                    "source": {
                        "database": "s1",
                        "schema": "sc1",
                        "table": "t2",
                        "column": "t1c1",
                    },
                    "target": {
                        "database": "Xs1",
                        "schema": "sc1",
                        "table": "t1",
                        "column": "c1",
                    },
                },
                "exc": r"'target' column \<Xs1, sc1, t1, c1\> does not exist",
            },
        ]
        for tc in test_cases:
            with pytest.raises(IntegrityError) as exc:
                import_from_object_stream(catalog, [tc["data"]])
            assert re.search(tc["exc"], caplog.text)
            caplog.clear()


def test_import_foreign_key_succeeds(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        s1 = catalog.add_source("s1", "sqlite")
        sc1 = catalog.add_schema("sc1", s1)
        t1 = catalog.add_table("t1", sc1)
        catalog.add_column("c1", "Int", 0, t1)
        t2 = catalog.add_table("t2", sc1)
        catalog.add_column("t1c1", "Int", 1, t2)
        data = {
            "type": "foreign-key",
            "source": {
                "database": "s1",
                "schema": "sc1",
                "table": "t2",
                "column": "t1c1",
            },
            "target": {
                "database": "s1",
                "schema": "sc1",
                "table": "t1",
                "column": "c1",
            },
        }
        import_from_object_stream(catalog, [data])
        assert bool(
            catalog.get_foreign_key(
                "s1", "sc1", "t2", "t1c1", "s1", "sc1", "t1", "c1"
            )
        )


def test_import_schema_failures(open_catalog_connection, caplog):
    test_cases = [
        {
            "data": {
                "type": "schema",
                "schema": "sc1",
            },
            "exc": r"no 'database' field",
        }, {
            "data": {
                "type": "schema",
                "database": "s1",
            },
            "exc": r"no 'schema' field",
        }, {
            "data": {
                "type": "schema",
                "database": "s1",
                "schema": "sc1",
            },
            "exc": r"schema \<s1, sc1\> is already defined",
        }
    ]
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        s1 = catalog.add_source("s1", "sqlite")
        catalog.add_schema("sc1", s1)
        for tc in test_cases:
            with pytest.raises(IntegrityError) as exc:
                import_from_object_stream(catalog,[tc["data"]])
            assert re.search(tc["exc"], caplog.text)


def test_import_schema_succeeds(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        catalog.add_source("s1", "sqlite")
        import_from_object_stream(catalog, [{
            "type": "schema",
            "database": "s1",
            "schema": "sc1",
        }])
        assert bool(
            catalog.get_schema("s1", "sc1")
        )


def test_import_table_failures(open_catalog_connection, caplog):
    test_cases = [
        {
            "data": {
                "type": "table",
                "schema": "sc1",
                "table": "t1",
            },
            "exc": r"no 'database' field",
        }, {
            "data": {
                "type": "table",
                "database": "s1",
                "table": "t1",
            },
            "exc": r"no 'schema' field",
        }, {
            "data": {
                "type": "table",
                "database": "s1",
                "schema": "sc1",
            },
            "exc": r"no 'table' field",
        }, {
            "data": {
                "type": "table",
                "database": "s1",
                "schema": "sc1",
                "table": "t1",
            },
            "exc": r"table \<s1, sc1, t1\> is already defined",
        }
    ]
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        s1 = catalog.add_source("s1", "sqlite")
        sc1 = catalog.add_schema("sc1", s1)
        catalog.add_table("t1", sc1)
        for tc in test_cases:
            with pytest.raises(IntegrityError) as exc:
                import_from_object_stream(catalog,[tc["data"]])
            assert re.search(tc["exc"], caplog.text)


def test_import_table_succeeds(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        s1 = catalog.add_source("s1", "sqlite")
        catalog.add_schema("sc1", s1)
        import_from_object_stream(catalog, [{
            "type": "table",
            "database": "s1",
            "schema": "sc1",
            "table": "t1",
        }])
        assert bool(
            catalog.get_table("s1", "sc1", "t1")
        )


def test_import_column_failures(open_catalog_connection, caplog):
    test_cases = [
        {
            "data": {
                "type": "column",
                "schema": "sc1",
                "table": "t1",
                "column": "c1",
                "data_type": "text",
                "sort_order": 1,
            },
            "exc": r"no 'database' field",
        }, {
            "data": {
                "type": "column",
                "database": "s1",
                "table": "t1",
                "column": "c1",
                "data_type": "text",
                "sort_order": 1,
            },
            "exc": r"no 'schema' field",
        }, {
            "data": {
                "type": "column",
                "database": "s1",
                "schema": "sc1",
                "column": "c1",
                "data_type": "text",
                "sort_order": 1,
            },
            "exc": r"no 'table' field",
        }, {
            "data": {
                "type": "column",
                "database": "s1",
                "schema": "sc1",
                "table": "t1",
                "data_type": "text",
                "sort_order": 1,
            },
            "exc": r"no 'column' field",
        }, {
            "data": {
                "type": "column",
                "database": "s1",
                "schema": "sc1",
                "table": "t1",
                "column": "c1",
                "sort_order": 1,
            },
            "exc": r"no 'data_type' field",
        }, {
            "data": {
                "type": "column",
                "database": "s1",
                "schema": "sc1",
                "table": "t1",
                "column": "c1",
                "data_type": "text",
            },
            "exc": r"no 'sort_order' field",
        }, {
            "data": {
                "type": "column",
                "database": "s1",
                "schema": "sc1",
                "table": "t1",
                "column": "c1",
                "data_type": "text",
                "sort_order": 1,
            },
            "exc": r"column \<s1, sc1, t1, c1\> is already defined",
        }
    ]
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        s1 = catalog.add_source("s1", "sqlite")
        sc1 = catalog.add_schema("sc1", s1)
        t1 = catalog.add_table("t1", sc1)
        catalog.add_column("c1", "text", 1, t1)
        for tc in test_cases:
            with pytest.raises(IntegrityError) as exc:
                import_from_object_stream(catalog,[tc["data"]])
            assert re.search(tc["exc"], caplog.text)


def test_import_column_succeeds(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        s1 = catalog.add_source("s1", "sqlite")
        sc1 = catalog.add_schema("sc1", s1)
        catalog.add_table("t1", sc1)
        import_from_object_stream(catalog, [{
            "type": "column",
            "database": "s1",
            "schema": "sc1",
            "table": "t1",
            "column": "c1",
            "data_type": "text",
            "sort_order": 1,
        }])
        c1 = catalog.get_column("s1", "sc1", "t1", "c1")
        assert c1.data_type == "text"
        assert c1.sort_order == 1
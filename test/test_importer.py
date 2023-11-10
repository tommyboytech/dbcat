import pytest
import sqlalchemy


def test_add_foreign_key(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with catalog.managed_session:
        s1 = catalog.add_source("s1", "sqlite")
        sc1 = catalog.add_schema("sc1", s1)
        t1 = catalog.add_table("t1", sc1)
        t1c1 = catalog.add_column("c1", "Int", 0, t1)
        t2 = catalog.add_table("t2", sc1)
        t2_t1c1 = catalog.add_column("t1c1", "Int", 1, t2)
        fk1 = catalog.add_foreign_key(t2_t1c1, t1c1)
        assert fk1.fqdn == ("s1", "sc1", "t2", "t1c1", "s1", "sc1", "t1", "c1")


def test_foreign_key_must_be_unique(open_catalog_connection):
    catalog, conf = open_catalog_connection
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        with catalog.managed_session:
            s1 = catalog.add_source("s1", "sqlite")
            sc1 = catalog.add_schema("sc1", s1)
            t1 = catalog.add_table("t1", sc1)
            t1c1 = catalog.add_column("c1", "Int", 0, t1)
            t2 = catalog.add_table("t2", sc1)
            t2_t1c1 = catalog.add_column("t1c1", "Int", 1, t2)
            catalog.add_foreign_key(t2_t1c1, t1c1)
            catalog.add_foreign_key(t2_t1c1, t1c1)

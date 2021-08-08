import unittest
import unittest.mock

import os
import shutil
import copy
import json
import pymysql.cursors

import ipaddress

import relations
import relations_pymysql

class SourceModel(relations.Model):
    SOURCE = "PyMySQLSource"

class Simple(SourceModel):
    id = int
    name = str

class Plain(SourceModel):
    ID = None
    simple_id = int
    name = str

relations.OneToMany(Simple, Plain)

class Meta(SourceModel):
    id = int
    name = str
    flag = bool
    spend = float
    stuff = list
    things = dict, {"extract": "for__0___1"}
    push = str, {"inject": "stuff__-1__relations.io___1"}

def subnet_attr(values, value):

    values["address"] = str(value)
    min_ip = value[0]
    max_ip = value[-1]
    values["min_address"] = str(min_ip)
    values["min_value"] = int(min_ip)
    values["max_address"] = str(max_ip)
    values["max_value"] = int(max_ip)

class Net(SourceModel):

    id = int
    ip = ipaddress.IPv4Address, {
        "attr": {"compressed": "address", "__int__": "value"},
        "init": "address",
        "label": "address",
        "extract": {"address": str, "value": int}
    }
    subnet = ipaddress.IPv4Network, {
        "attr": subnet_attr,
        "init": "address",
        "label": "address"
    }

    LABEL = "ip__address"
    INDEX = "ip__value"

class Unit(SourceModel):
    id = int
    name = str, {"format": "fancy"}

class Test(SourceModel):
    id = int
    unit_id = int
    name = str, {"format": "shmancy"}

class Case(SourceModel):
    id = int
    test_id = int
    name = str

relations.OneToMany(Unit, Test)
relations.OneToOne(Test, Case)

class TestSource(unittest.TestCase):

    maxDiff = None

    def setUp(self):

        self.source = relations_pymysql.Source("PyMySQLSource", "test_source", host=os.environ["MYSQL_HOST"], port=int(os.environ["MYSQL_PORT"]))
        self.source.connection.cursor().execute("CREATE DATABASE IF NOT EXISTS `test_source`")

        shutil.rmtree("ddl", ignore_errors=True)
        os.makedirs("ddl", exist_ok=True)

    def tearDown(self):

        cursor = self.source.connection.cursor()
        cursor.execute("DROP DATABASE IF EXISTS `test_source`")

    @unittest.mock.patch("relations.SOURCES", {})
    @unittest.mock.patch("pymysql.connect", unittest.mock.MagicMock())
    def test___init__(self):

        source = relations_pymysql.Source("unit", "init", connection="corkneckshurn")
        self.assertFalse(source.created)
        self.assertEqual(source.name, "unit")
        self.assertEqual(source.schema, "init")
        self.assertEqual(source.connection, "corkneckshurn")
        self.assertEqual(relations.SOURCES["unit"], source)

        source = relations_pymysql.Source("test", "init", host="db.com", extra="stuff")
        self.assertTrue(source.created)
        self.assertEqual(source.name, "test")
        self.assertEqual(source.schema, "init")
        self.assertEqual(source.connection, pymysql.connect.return_value)
        self.assertEqual(relations.SOURCES["test"], source)
        pymysql.connect.assert_called_once_with(cursorclass=pymysql.cursors.DictCursor, host="db.com", extra="stuff")

    @unittest.mock.patch("relations.SOURCES", {})
    @unittest.mock.patch("pymysql.connect", unittest.mock.MagicMock())
    def test___del__(self):

        source = relations_pymysql.Source("test", "init", host="db.com", extra="stuff")
        source.connection = None
        del relations.SOURCES["test"]
        pymysql.connect.return_value.close.assert_not_called()

        relations_pymysql.Source("test", "init", host="db.com", extra="stuff")
        del relations.SOURCES["test"]
        pymysql.connect.return_value.close.assert_called_once_with()

    def test_table(self):

        model = {
            "table": "people"
        }
        self.assertEqual(self.source.table(model), "`test_source`.`people`")

        model["schema"] = "stuff"
        self.assertEqual(self.source.table(model), "`stuff`.`people`")

        model = unittest.mock.MagicMock()
        model.SCHEMA = None

        model.TABLE = "people"
        self.assertEqual(self.source.table(model), "`test_source`.`people`")

        model.SCHEMA = "stuff"
        self.assertEqual(self.source.table(model), "`stuff`.`people`")

    def test_encode(self):

        model = unittest.mock.MagicMock()
        people = unittest.mock.MagicMock()
        stuff = unittest.mock.MagicMock()
        things = unittest.mock.MagicMock()

        people.kind = str
        stuff.kind = list
        things.kind = dict

        people.store = "people"
        stuff.store = "stuff"
        things.store = "things"

        model._fields._order = [people, stuff, things]

        values = {
            "people": "sure",
            "stuff": None,
            "things": None
        }

        self.assertEqual(self.source.encode(model, values), {
            "people": "sure",
            "stuff": None,
            "things": None
        })

        values = {
            "people": "sure",
            "stuff": [],
            "things": {}
        }

        self.assertEqual(self.source.encode(model, values), {
            "people": "sure",
            "stuff": '[]',
            "things": '{}'
        })

    def test_decode(self):

        model = unittest.mock.MagicMock()
        people = unittest.mock.MagicMock()
        stuff = unittest.mock.MagicMock()
        things = unittest.mock.MagicMock()

        people.kind = str
        stuff.kind = list
        things.kind = dict

        people.store = "people"
        stuff.store = "stuff"
        things.store = "things"

        model._fields._order = [people, stuff, things]

        values = {
            "people": "sure",
            "stuff": None,
            "things": None
        }

        self.assertEqual(self.source.decode(model, values), {
            "people": "sure",
            "stuff": None,
            "things": None
        })

        values = {
            "people": "sure",
            "stuff": '[]',
            "things": '{}'
        }

        self.assertEqual(self.source.decode(model, values), {
            "people": "sure",
            "stuff": [],
            "things": {}
        })

    def test_walk(self):

        self.assertEqual(self.source.walk("a__b__0___1"), '$.a.b[0]."1"')

    def test_field_init(self):

        class Field:
            pass

        field = Field()

        self.source.field_init(field)

        self.assertIsNone(field.auto_increment)
        self.assertIsNone(field.definition)

    def test_model_init(self):

        class Check(relations.Model):
            id = int
            name = str

        model = Check()

        self.source.model_init(model)

        self.assertIn("QUERY", model.UNDEFINE)
        self.assertIsNone(model.SCHEMA)
        self.assertEqual(model.TABLE, "check")
        self.assertEqual(model.QUERY.get(), "SELECT * FROM `test_source`.`check`")
        self.assertIsNone(model.DEFINITION)
        self.assertTrue(model._fields._names["id"].auto_increment)
        self.assertTrue(model._fields._names["id"].auto)

    def test_column_define(self):

        # Specific

        field = relations.Field(int, definition="id")
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "id")

        # TINYINT

        field = relations.Field(bool, store="_flag")
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`_flag` TINYINT")

        # TINYINT default

        field = relations.Field(bool, store="_flag", default=False)
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`_flag` TINYINT NOT NULL DEFAULT 0")

        # TINYINT none

        field = relations.Field(bool, store="_flag", none=False)
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`_flag` TINYINT NOT NULL")

        # INTEGER

        field = relations.Field(int, store="_id")
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`_id` INTEGER")

        # INTEGER default

        field = relations.Field(int, store="_id", default=0)
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`_id` INTEGER NOT NULL DEFAULT 0")

        # INTEGER none

        field = relations.Field(int, store="_id", none=False)
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`_id` INTEGER NOT NULL")

        # INTEGER auto_increment

        field = relations.Field(int, store="_id", auto_increment=True)
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`_id` INTEGER AUTO_INCREMENT")

        # INTEGER full

        field = relations.Field(int, store="_id", none=False, auto_increment=True, default=0)
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`_id` INTEGER NOT NULL AUTO_INCREMENT DEFAULT 0")

        # FLOAT

        field = relations.Field(float, store="spend")
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`spend` DOUBLE")

        # FLOAT default

        field = relations.Field(float, store="spend", default=0.1)
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`spend` DOUBLE NOT NULL DEFAULT 0.1")

        # FLOAT none

        field = relations.Field(float, store="spend", none=False)
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`spend` DOUBLE NOT NULL")

        # VARCHAR

        field = relations.Field(str, name="name")
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`name` VARCHAR(255)")

        # VARCHAR length

        field = relations.Field(str, name="name", length=32)
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`name` VARCHAR(32)")

        # VARCHAR default

        field = relations.Field(str, name="name", default='ya')
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`name` VARCHAR(255) NOT NULL DEFAULT 'ya'")

        # VARCHAR none

        field = relations.Field(str, name="name", none=False)
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`name` VARCHAR(255) NOT NULL")

        # VARCHAR full

        field = relations.Field(str, name="name", length=32, none=False, default='ya')
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), "`name` VARCHAR(32) NOT NULL DEFAULT 'ya'")

        # JSON (list)

        field = relations.Field(list, name='stuff')
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), '`stuff` JSON NOT NULL')

        # JSON (dict)

        field = relations.Field(dict, name='things')
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), '`things` JSON NOT NULL')

        # JSON (anything)

        field = relations.Field(ipaddress.IPv4Address, name='ip', attr="whatev")
        self.source.field_init(field)
        self.assertEqual(self.source.column_define(field.define()), '`ip` JSON')

    def test_extract_define(self):

        self.assertEqual(
            self.source.extract_define('grab', 'a__b__0___1', 'bool'),
            "`grab__a__b__0___1` TINYINT AS (`grab`->>'$.a.b[0].\"1\"')"
        )
        self.assertEqual(
            self.source.extract_define('grab', 'c__b__0___1', 'int'),
            "`grab__c__b__0___1` INTEGER AS (`grab`->>'$.c.b[0].\"1\"')"
        )
        self.assertEqual(
            self.source.extract_define('grab', 'c__d__0___1', 'float'),
            "`grab__c__d__0___1` DOUBLE AS (`grab`->>'$.c.d[0].\"1\"')"
        )
        self.assertEqual(
            self.source.extract_define('grab', 'c__d__1___1', 'str'),
            "`grab__c__d__1___1` VARCHAR(255) AS (`grab`->>'$.c.d[1].\"1\"')"
        )
        self.assertEqual(
            self.source.extract_define('grab', 'c__d__1___2', 'dict'),
            "`grab__c__d__1___2` JSON AS (`grab`->>'$.c.d[1].\"2\"')"
        )

    def test_field_define(self):

        # EXTRACTED

        field = relations.Field(dict, name='grab', extract={
            "a__b__0___1": bool,
            "c__b__0___1": int,
            "c__d__0___1": float,
            "c__d__1___1": str,
            "c__d__1___2": list
        })
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field.define(), definitions)
        self.assertEqual(definitions, [
            "`grab` JSON NOT NULL",
            "`grab__a__b__0___1` TINYINT AS (`grab`->>'$.a.b[0].\"1\"')",
            "`grab__c__b__0___1` INTEGER AS (`grab`->>'$.c.b[0].\"1\"')",
            "`grab__c__d__0___1` DOUBLE AS (`grab`->>'$.c.d[0].\"1\"')",
            "`grab__c__d__1___1` VARCHAR(255) AS (`grab`->>'$.c.d[1].\"1\"')",
            "`grab__c__d__1___2` JSON AS (`grab`->>'$.c.d[1].\"2\"')"
        ])

        # (not) EXTRACTED

        field = relations.Field(dict, name='grab', extract={
            "a__b__0___1": bool,
            "c__b__0___1": int,
            "c__d__0___1": float,
            "c__d__1___1": str,
            "c__d__1___2": list
        })
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field.define(), definitions, extract=False)
        self.assertEqual(definitions, [
            "`grab` JSON NOT NULL"
        ])

        # INJECTED

        field = relations.Field(str, name='toss', inject="things__a__b__0___1")
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field.define(), definitions)
        self.assertEqual(definitions, [])

    def test_index_define(self):

        self.assertEqual(self.source.index_define("id-name", ["id", "name"]), "INDEX `id_name` (`id`,`name`)")
        self.assertEqual(self.source.index_define("id-name", ["id", "name"], True), "UNIQUE `id_name` (`id`,`name`)")

    def test_model_define(self):

        class Simple(relations.Model):

            SOURCE = "PyMySQLSource"
            DEFINITION = "whatever"

            id = int
            name = str

            INDEX = ["id", "name"]

        self.assertEqual(self.source.model_define(Simple.thy().define()), ["whatever"])

        Simple.DEFINITION = None
        self.assertEqual(self.source.model_define(Simple.thy().define()), ["""CREATE TABLE IF NOT EXISTS `test_source`.`simple` (
  `id` INTEGER AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE `name` (`name`),
  INDEX `id_name` (`id`,`name`)
)"""])

        cursor = self.source.connection.cursor()
        cursor.execute(self.source.model_define(Simple.thy().define())[0])
        cursor.close()

    def test_field_add(self):

        # EXTRACTED

        field = relations.Field(dict, name='grab', extract={
            "a__b__0___1": bool,
            "c__b__0___1": int,
            "c__d__0___1": float,
            "c__d__1___1": str,
            "c__d__1___2": list
        })
        self.source.field_init(field)
        migrations = []
        self.source.field_add(field.define(), migrations)
        self.assertEqual(migrations, [
            "ADD `grab` JSON NOT NULL",
            "ADD `grab__a__b__0___1` TINYINT AS (`grab`->>'$.a.b[0].\"1\"')",
            "ADD `grab__c__b__0___1` INTEGER AS (`grab`->>'$.c.b[0].\"1\"')",
            "ADD `grab__c__d__0___1` DOUBLE AS (`grab`->>'$.c.d[0].\"1\"')",
            "ADD `grab__c__d__1___1` VARCHAR(255) AS (`grab`->>'$.c.d[1].\"1\"')",
            "ADD `grab__c__d__1___2` JSON AS (`grab`->>'$.c.d[1].\"2\"')"
        ])

        # INJECTED

        field = relations.Field(str, name='toss', inject="things__a__b__0___1")
        self.source.field_init(field)
        migrations = []
        self.source.field_add(field.define(), migrations)
        self.assertEqual(migrations, [])

    def test_field_remove(self):

        # EXTRACTED

        field = relations.Field(dict, name='grab', extract={
            "a__b__0___1": bool,
            "c__b__0___1": int,
            "c__d__0___1": float,
            "c__d__1___1": str,
            "c__d__1___2": list
        })
        self.source.field_init(field)
        migrations = []
        self.source.field_remove(field.define(), migrations)
        self.assertEqual(migrations, [
            "DROP `grab`",
            "DROP `grab__a__b__0___1`",
            "DROP `grab__c__b__0___1`",
            "DROP `grab__c__d__0___1`",
            "DROP `grab__c__d__1___1`",
            "DROP `grab__c__d__1___2`"
        ])

        # INJECTED

        field = relations.Field(str, name='toss', inject="things__a__b__0___1")
        self.source.field_init(field)
        migrations = []
        self.source.field_remove(field.define(), migrations)
        self.assertEqual(migrations, [])

    def test_field_change(self):

        # EXTRACTED

        field = relations.Field(dict, name='grab', extract={
            "a__b__0___1": bool,
            "c__b__0___1": int,
            "c__d__0___1": float,
            "c__d__1___1": str,
            "c__d__1___2": list
        })
        self.source.field_init(field)
        definition = field.define()
        migration = {
            "store": "bag",
            "extract": {
                "a__b__0___1": 'bool',
                "c__b__0___1": 'int',
                "c__d__0___1": 'float',
                "c__d__2___1": 'str',
                "c__d__1___2": 'str'
            }
        }
        migrations = []
        self.source.field_change(definition, migration, migrations)
        self.assertEqual(migrations, [
            "CHANGE `grab` `bag` JSON NOT NULL",
            "DROP `grab__c__d__1___1`",
            "CHANGE `grab__a__b__0___1` `bag__a__b__0___1` TINYINT AS (`bag`->>'$.a.b[0].\"1\"')",
            "CHANGE `grab__c__b__0___1` `bag__c__b__0___1` INTEGER AS (`bag`->>'$.c.b[0].\"1\"')",
            "CHANGE `grab__c__d__0___1` `bag__c__d__0___1` DOUBLE AS (`bag`->>'$.c.d[0].\"1\"')",
            "CHANGE `grab__c__d__1___2` `bag__c__d__1___2` VARCHAR(255) AS (`bag`->>'$.c.d[1].\"2\"')",
            "ADD `bag__c__d__2___1` VARCHAR(255) AS (`bag`->>'$.c.d[2].\"1\"')"
        ])

        # INJECTED

        field = relations.Field(str, name='toss', inject="things__a__b__0___1")
        self.source.field_init(field)
        definition = field.define()
        migration = {**definition, "store": "bag"}
        migrations = []
        self.source.field_change(definition, migration, migrations)
        self.assertEqual(migrations, [])

    def test_model_add(self):

        self.assertEqual(self.source.model_add(Simple.thy().define()), ["""CREATE TABLE IF NOT EXISTS `test_source`.`simple` (
  `id` INTEGER AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE `name` (`name`)
)"""])

        cursor = self.source.connection.cursor()
        cursor.execute(self.source.model_add(Simple.thy().define())[0])
        cursor.close()

    def test_model_remove(self):

        self.assertEqual(self.source.model_remove(Simple.thy().define()), ["""DROP TABLE IF EXISTS `test_source`.`simple`"""])

        cursor = self.source.connection.cursor()
        cursor.execute(self.source.model_add(Simple.thy().define())[0])
        cursor.execute(self.source.model_remove(Simple.thy().define())[0])
        cursor.close()

    def test_model_change(self):

        class Simple(relations.Model):

            SOURCE = "PyMySQLSource"

            id = int
            name = str
            fie = int
            foe = int

            UNIQUE = {
                "foe": ["foe"],
                "labels": ["name", "id"]
            }
            INDEX = {
                "speedy": ["id", "name"],
                "fie": ["fie"]
            }

        migration = {
            "table": "simples",
            "fields": {
                "add": [
                    {
                        "name": "fee",
                        "store": "fee",
                        "kind": "int",
                        "none": True
                    }
                ],
                "remove": ["fie"],
                "change": {
                    "foe": {
                        "name": "fum",
                        "kind": "float"
                    }
                }
            },
            "unique": {
                "add": {
                    "fee": ["fee"]
                },
                "remove": ["foe"],
                "rename": {
                    "labels": "label"
                }
            },
            "index": {
                "add": {
                    "foe-fee": ["foe", "fee"]
                },
                "remove": ["fie"],
                "rename": {
                    "speedy": "speed"
                }
            }
        }

        self.assertEqual(self.source.model_change(Simple.thy().define(), migration)[0], """ALTER TABLE `test_source`.`simple`
  RENAME TO `test_source`.`simples`,
  ADD `fee` INTEGER,
  DROP `fie`,
  CHANGE `foe` `foe` DOUBLE,
  ADD UNIQUE `fee` (`fee`),
  DROP INDEX `foe`,
  RENAME INDEX `labels` TO `label`,
  ADD INDEX `foe_fee` (`foe`,`fee`),
  DROP INDEX `fie`,
  RENAME INDEX `speedy` TO `speed`""")

        cursor = self.source.connection.cursor()
        cursor.execute(self.source.model_define(Simple.thy().define())[0])
        cursor.execute(self.source.model_change(Simple.thy().define(), migration)[0])
        cursor.close()

    def test_field_create(self):

        # Standard

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        fields = []
        clause = []
        self.source.field_create( field, fields, clause)
        self.assertEqual(fields, ["`id`"])
        self.assertEqual(clause, ["%(id)s"])
        self.assertFalse(field.changed)

        # auto

        field = relations.Field(int, name="id", auto=True)
        self.source.field_init(field)
        fields = []
        clause = []
        self.source.field_create( field, fields, clause)
        self.assertEqual(fields, [])
        self.assertEqual(clause, [])

        # inject

        field = relations.Field(int, name="id", inject=True)
        self.source.field_init(field)
        fields = []
        clause = []
        self.source.field_create( field, fields, clause)
        self.assertEqual(fields, [])
        self.assertEqual(clause, [])

    def test_model_create(self):

        simple = Simple("sure")
        simple.plain.add("fine")

        cursor = self.source.connection.cursor()
        [cursor.execute(statement) for statement in Simple.define() + Plain.define() + Meta.define()]

        simple.create()

        self.assertEqual(simple.id, 1)
        self.assertEqual(simple._action, "update")
        self.assertEqual(simple._record._action, "update")
        self.assertEqual(simple.plain[0].simple_id, 1)
        self.assertEqual(simple.plain._action, "update")
        self.assertEqual(simple.plain[0]._record._action, "update")

        cursor.execute("SELECT * FROM test_source.simple")
        self.assertEqual(cursor.fetchone(), {"id": 1, "name": "sure"})

        simples = Simple.bulk().add("ya").create()
        self.assertEqual(simples._models, [])

        cursor.execute("SELECT * FROM test_source.simple WHERE name='ya'")
        self.assertEqual(cursor.fetchone(), {"id": 2, "name": "ya"})

        cursor.execute("SELECT * FROM test_source.plain")
        self.assertEqual(cursor.fetchone(), {"simple_id": 1, "name": "fine"})

        Meta("yep", True, 3.50, [1, None], {"for": [{"1": "yep"}]}, "sure").create()
        cursor.execute("SELECT * FROM test_source.meta")
        self.assertEqual(cursor.fetchone(), {
            "id": 1,
            "name": "yep",
            "flag": 1,
            "spend": 3.50,
            "stuff": '[1, {"relations.io": {"1": "sure"}}]',
            "things": '{"for": [{"1": "yep"}]}',
            "things__for__0___1": "yep"
        })

        cursor.close()

    def test_field_retrieve(self):

        # IN

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter([1, 2, 3], "in")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, "`id` IN (%s,%s,%s)")
        self.assertEqual(values, [1, 2, 3])

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter([], "in")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, "FALSE")
        self.assertEqual(values, [])

        # NOT IN

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter([1, 2, 3], "ne")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, "`id` NOT IN (%s,%s,%s)")
        self.assertEqual(values, [1, 2, 3])

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter([], "ne")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, "TRUE")
        self.assertEqual(values, [])

        # LIKE

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter(1, 'like')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, '`id` LIKE %s')
        self.assertEqual(values, ["%1%"])

        # NOT LIKE

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter(1, 'notlike')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, '`id` NOT LIKE %s')
        self.assertEqual(values, ["%1%"])

        # IS NULL

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter(True, 'null')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, '`id` IS NULL')
        self.assertEqual(values, [])

        # IS NOT NULL

        field = relations.Field(int, name='id')
        self.source.field_init(field)
        field.filter(False, 'null')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, '`id` IS NOT NULL')
        self.assertEqual(values, [])

        # JSON

        field = relations.Field(dict, name='meta')
        self.source.field_init(field)
        field.filter(1, 'a__b__0___1')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, "`meta`->>%s=%s")
        self.assertEqual(values, ['$.a.b[0]."1"', 1])

        field = relations.Field(dict, name='meta', extract="a__b__0___1")
        self.source.field_init(field)
        field.filter(1, 'a__b__0___1')
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, "`meta__a__b__0___1`=%s")
        self.assertEqual(values, [1])

        # =

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter(1)
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, "`id`=%s")
        self.assertEqual(values, [1])

        # >

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter(1, "gt")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, "`id`>%s")
        self.assertEqual(values, [1])

        # >=

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter(1, "gte")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, "`id`>=%s")
        self.assertEqual(values, [1])

        # <

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter(1, "lt")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, "`id`<%s")
        self.assertEqual(values, [1])

        # <=

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter(1, "lte")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve(field, query, values)
        self.assertEqual(query.wheres, "`id`<=%s")
        self.assertEqual(values, [1])

    def test_model_like(self):

        cursor = self.source.connection.cursor()

        [cursor.execute(statement) for statement in Unit.define() + Test.define() + Case.define() + Meta.define() + Net.define()]

        Unit([["stuff"], ["people"]]).create()

        unit = Unit.one()

        query = copy.deepcopy(unit.QUERY)
        values = []
        self.source.model_like(unit, query, values)
        self.assertEqual(query.wheres, '')
        self.assertEqual(values, [])

        unit = Unit.one(like="p")
        query = copy.deepcopy(unit.QUERY)
        values = []
        self.source.model_like(unit, query, values)
        self.assertEqual(query.wheres, '(`name` LIKE %s)')
        self.assertEqual(values, ['%p%'])

        unit = Unit.one(name="people")
        unit.test.add("things")[0]
        unit.update()

        test = Test.many(like="p")
        query = copy.deepcopy(test.QUERY)
        values = []
        self.source.model_like(test, query, values)
        self.assertEqual(query.wheres, '(`unit_id` IN (%s) OR `name` LIKE %s)')
        self.assertEqual(values, [unit.id, '%p%'])
        self.assertFalse(test.overflow)

        test = Test.many(like="p", _chunk=1)
        query = copy.deepcopy(test.QUERY)
        values = []
        self.source.model_like(test, query, values)
        self.assertEqual(query.wheres, '(`unit_id` IN (%s) OR `name` LIKE %s)')
        self.assertEqual(values, [unit.id, '%p%'])
        self.assertTrue(test.overflow)

        Unit.many().delete()
        test = Test.many(like="p")
        query = copy.deepcopy(test.QUERY)
        values = []
        self.source.model_like(test, query, values)
        self.assertEqual(query.wheres, '(`name` LIKE %s)')
        self.assertEqual(values, ['%p%'])

        class Nut(SourceModel):

            id = int
            name = str
            ip = ipaddress.IPv4Address, {"attr": {"compressed": "address", "__int__": "value"}, "init": "address", "label": ["address", "value"], "extract": "address"}
            subnet = ipaddress.IPv4Network, {"attr": subnet_attr, "init": "address", "label": "address"}

            LABEL = ["ip", "subnet__min_address"]
            UNIQUE = False

        net = Nut.many(like="p")
        query = copy.deepcopy(net.QUERY)
        values = []
        self.source.model_like(net, query, values)
        self.assertEqual(query.wheres, '(`ip__address` LIKE %s OR `ip`->>%s LIKE %s OR `subnet`->>%s LIKE %s)')
        self.assertEqual(values, ['%p%', '$.value', '%p%', '$.min_address', '%p%'])

    def test_model_sort(self):

        unit = Unit.one()

        query = copy.deepcopy(unit.QUERY)
        self.source.model_sort(unit, query)
        self.assertEqual(query.order_bys, '`name`')

        unit._sort = ['-id']
        query = copy.deepcopy(unit.QUERY)
        self.source.model_sort(unit, query)
        self.assertEqual(query.order_bys, '`id` DESC')
        self.assertIsNone(unit._sort)

    def test_model_limit(self):

        unit = Unit.one()

        query = copy.deepcopy(unit.QUERY)
        values = []
        self.source.model_limit(unit, query, values)
        self.assertEqual(query.limits, '')
        self.assertEqual(values, [])

        unit._limit = 2
        query = copy.deepcopy(unit.QUERY)
        values = []
        self.source.model_limit(unit, query, values)
        self.assertEqual(query.limits, '%s')
        self.assertEqual(values, [2])

        unit._offset = 1
        query = copy.deepcopy(unit.QUERY)
        values = []
        self.source.model_limit(unit, query, values)
        self.assertEqual(query.limits, '%s, %s')
        self.assertEqual(values, [1, 2])

    def test_model_count(self):

        cursor = self.source.connection.cursor()

        [cursor.execute(statement) for statement in Unit.define() + Test.define() + Case.define()]

        Unit([["stuff"], ["people"]]).create()

        self.assertEqual(Unit.many().count(), 2)

        self.assertEqual(Unit.many(name="people").count(), 1)

        self.assertEqual(Unit.many(like="p").count(), 1)

    def test_model_retrieve(self):

        cursor = self.source.connection.cursor()

        [cursor.execute(statement) for statement in Unit.define() + Test.define() + Case.define() + Meta.define() + Net.define()]

        Unit([["stuff"], ["people"]]).create()

        models = Unit.one(name__in=["people", "stuff"])
        self.assertRaisesRegex(relations.ModelError, "unit: more than one retrieved", models.retrieve)

        model = Unit.one(name="things")
        self.assertRaisesRegex(relations.ModelError, "unit: none retrieved", model.retrieve)

        self.assertIsNone(model.retrieve(False))

        unit = Unit.one(name="people")

        self.assertEqual(unit.id, 2)
        self.assertEqual(unit._action, "update")
        self.assertEqual(unit._record._action, "update")

        unit.test.add("things")[0].case.add("persons")
        unit.update()

        model = Unit.many(test__name="things")

        self.assertEqual(model.id, [2])
        self.assertEqual(model[0]._action, "update")
        self.assertEqual(model[0]._record._action, "update")
        self.assertEqual(model[0].test[0].id, 1)
        self.assertEqual(model[0].test[0].case.name, "persons")

        model = Unit.many(like="p")
        self.assertEqual(model.name, ["people"])

        model = Test.many(like="p").retrieve()
        self.assertEqual(model.name, ["things"])
        self.assertFalse(model.overflow)

        model = Test.many(like="p", _chunk=1).retrieve()
        self.assertEqual(model.name, ["things"])
        self.assertTrue(model.overflow)

        Meta("yep", True, 1.1, [1, None], {"a": 1}).create()
        model = Meta.one(name="yep")

        self.assertEqual(model.flag, True)
        self.assertEqual(model.spend, 1.1)
        self.assertEqual(model.stuff, [1, {"relations.io": {"1": None}}])
        self.assertEqual(model.things, {"a": 1})

        self.assertEqual(Unit.many().name, ["people", "stuff"])
        self.assertEqual(Unit.many().sort("-name").name, ["stuff", "people"])
        self.assertEqual(Unit.many().sort("-name").limit(1, 1).name, ["people"])
        self.assertEqual(Unit.many().sort("-name").limit(0).name, [])
        self.assertEqual(Unit.many(name="people").limit(1).name, ["people"])

        Meta("dive", stuff=[1, 2, 3, None], things={"a": {"b": [1], "c": "sure"}, "4": 5, "for": [{"1": "yep"}]}).create()

        model = Meta.many(stuff__1=2)
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things__a__b__0=1)
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things__a__c__like="su")
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things__a__d__null=True)
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things___4=5)
        self.assertEqual(model[0].name, "dive")

        model = Meta.many(things__a__b__0__gt=1)
        self.assertEqual(len(model), 0)

        model = Meta.many(things__a__c__notlike="su")
        self.assertEqual(len(model), 0)

        model = Meta.many(things__a__d__null=False)
        self.assertEqual(len(model), 0)

        model = Meta.many(things___4=6)
        self.assertEqual(len(model), 0)

        Net(ip="1.2.3.4", subnet="1.2.3.0/24").create()
        Net().create()

        model = Net.many(like='1.2.3.')
        self.assertEqual(model[0].ip.compressed, "1.2.3.4")

        model = Net.many(ip__address__like='1.2.3.')
        self.assertEqual(model[0].ip.compressed, "1.2.3.4")

        model = Net.many(ip__value__gt=int(ipaddress.IPv4Address('1.2.3.0')))
        self.assertEqual(model[0].ip.compressed, "1.2.3.4")

        model = Net.many(subnet__address__like='1.2.3.')
        self.assertEqual(model[0].ip.compressed, "1.2.3.4")

        model = Net.many(subnet__min_value=int(ipaddress.IPv4Address('1.2.3.0')))
        self.assertEqual(model[0].ip.compressed, "1.2.3.4")

        model = Net.many(ip__address__notlike='1.2.3.')
        self.assertEqual(len(model), 0)

        model = Net.many(ip__value__lt=int(ipaddress.IPv4Address('1.2.3.0')))
        self.assertEqual(len(model), 0)

        model = Net.many(subnet__address__notlike='1.2.3.')
        self.assertEqual(len(model), 0)

        model = Net.many(subnet__max_value=int(ipaddress.IPv4Address('1.2.3.0')))
        self.assertEqual(len(model), 0)

    def test_model_labels(self):

        cursor = self.source.connection.cursor()

        [cursor.execute(statement) for statement in Unit.define() + Test.define() + Case.define() + Meta.define() + Net.define()]

        Unit("people").create().test.add("stuff").add("things").create()

        labels = Unit.many().labels()

        self.assertEqual(labels.id, "id")
        self.assertEqual(labels.label, ["name"])
        self.assertEqual(labels.parents, {})
        self.assertEqual(labels.format, ["fancy"])

        self.assertEqual(labels.ids, [1])
        self.assertEqual(labels.labels,{1: ["people"]})

        labels = Test.many().labels()

        self.assertEqual(labels.id, "id")
        self.assertEqual(labels.label, ["unit_id", "name"])

        self.assertEqual(labels.parents["unit_id"].id, "id")
        self.assertEqual(labels.parents["unit_id"].label, ["name"])
        self.assertEqual(labels.parents["unit_id"].parents, {})
        self.assertEqual(labels.parents["unit_id"].format, ["fancy"])

        self.assertEqual(labels.format, ["fancy", "shmancy"])

        self.assertEqual(labels.ids, [1, 2])
        self.assertEqual(labels.labels, {
            1: ["people", "stuff"],
            2: ["people", "things"]
        })

        Net(ip="1.2.3.4", subnet="1.2.3.0/24").create()

        self.assertEqual(Net.many().labels().labels, {
            1: ["1.2.3.4"]
        })

    def test_field_update(self):

        # Standard

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        clause = []
        values = []
        self.source.field_update(field, {"id": 1}, clause, values)
        self.assertEqual(clause, ['`id`=%s'])
        self.assertEqual(values, [1])

        # Non standard

        field = relations.Field(dict, name="id")
        self.source.field_init(field)
        clause = []
        values = []
        self.source.field_update(field, {"id": {"a": 1}}, clause, values)
        self.assertEqual(clause, ['`id`=%s'])
        self.assertEqual(values, ['{"a": 1}'])

        # Non existent

        field = relations.Field(dict, name="id")
        self.source.field_init(field)
        clause = []
        values = []
        self.source.field_update(field, {}, clause, values)
        self.assertEqual(clause, [])
        self.assertEqual(values, [])

    def test_model_update(self):

        cursor = self.source.connection.cursor()

        [cursor.execute(statement) for statement in Unit.define() + Test.define() + Case.define() + Meta.define() + Net.define()]

        Unit([["people"], ["stuff"]]).create()

        unit = Unit.many(id=2).set(name="things")

        self.assertEqual(unit.update(), 1)

        unit = Unit.one(2)

        unit.name = "thing"
        unit.test.add("moar")

        self.assertEqual(unit.update(), 1)
        self.assertEqual(unit.name, "thing")
        self.assertEqual(unit.test[0].id, 1)
        self.assertEqual(unit.test[0].name, "moar")

        Meta("yep", True, 1.1, [1, None], {"a": 1}).create()
        Meta.one(name="yep").set(flag=False, stuff=[], things={}).update()

        model = Meta.one(name="yep")
        self.assertEqual(model.flag, False)
        self.assertEqual(model.spend, 1.1)
        self.assertEqual(model.stuff, [])
        self.assertEqual(model.things, {})

        plain = Plain.one()
        self.assertRaisesRegex(relations.ModelError, "plain: nothing to update from", plain.update)

        ping = Net(ip="1.2.3.4", subnet="1.2.3.0/24").create()
        pong = Net(ip="5.6.7.8", subnet="5.6.7.0/24").create()

        Net.many().set(subnet="9.10.11.0/24").update()

        self.assertEqual(Net.one(ping.id).subnet.compressed, "9.10.11.0/24")
        self.assertEqual(Net.one(pong.id).subnet.compressed, "9.10.11.0/24")

        Net.one(ping.id).set(ip="13.14.15.16").update()
        self.assertEqual(Net.one(ping.id).ip.compressed, "13.14.15.16")
        self.assertEqual(Net.one(pong.id).ip.compressed, "5.6.7.8")

    def test_model_delete(self):

        cursor = self.source.connection.cursor()

        [cursor.execute(statement) for statement in Unit.define() + Test.define() + Case.define()]

        unit = Unit("people")
        unit.test.add("stuff").add("things")
        unit.create()

        self.assertEqual(Test.one(id=2).delete(), 1)
        self.assertEqual(len(Test.many()), 1)

        self.assertEqual(Unit.one(1).test.delete(), 1)
        self.assertEqual(Unit.one(1).retrieve().delete(), 1)
        self.assertEqual(len(Unit.many()), 0)
        self.assertEqual(len(Test.many()), 0)

        self.assertEqual(Test.many().delete(), 0)

        [cursor.execute(statement) for statement in Plain.define()]

        plain = Plain(0, "nope").create()
        self.assertRaisesRegex(relations.ModelError, "plain: nothing to delete from", plain.delete)

    def test_definition_convert(self):

        with open("ddl/general.json", 'w') as ddl_file:
            json.dump({
                "simple": Simple.thy().define(),
                "plain": Plain.thy().define()
            }, ddl_file)

        os.makedirs("ddl/sourced", exist_ok=True)

        self.source.definition_convert("ddl/general.json", "ddl/sourced")

        with open("ddl/sourced/general.sql", 'r') as ddl_file:
            self.assertEqual(ddl_file.read(), """CREATE TABLE IF NOT EXISTS `test_source`.`plain` (
  `simple_id` INTEGER,
  `name` VARCHAR(255) NOT NULL,
  UNIQUE `simple_id_name` (`simple_id`,`name`)
);

CREATE TABLE IF NOT EXISTS `test_source`.`simple` (
  `id` INTEGER AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE `name` (`name`)
);
""")

    def test_migration_convert(self):

        with open("ddl/general.json", 'w') as ddl_file:
            json.dump({
                "add": {"simple": Simple.thy().define()},
                "remove": {"simple": Simple.thy().define()},
                "change": {
                    "simple": {
                        "definition": Simple.thy().define(),
                        "migration": {
                            "source": "PyMySQLSource",
                            "table": "simples"
                        }
                    }
                }
            }, ddl_file)

        os.makedirs("ddl/sourced", exist_ok=True)

        self.source.migration_convert("ddl/general.json", "ddl/sourced")

        with open("ddl/sourced/general.sql", 'r') as ddl_file:
            self.assertEqual(ddl_file.read(), """CREATE TABLE IF NOT EXISTS `test_source`.`simple` (
  `id` INTEGER AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE `name` (`name`)
);

DROP TABLE IF EXISTS `test_source`.`simple`;

ALTER TABLE `test_source`.`simple`
  RENAME TO `test_source`.`simples`;
""")

    def test_execute(self):

        self.source.execute("")

        self.source.execute("""CREATE TABLE IF NOT EXISTS `test_source`.`simple` (
  `id` INTEGER AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE `name` (`name`)
);""")

        cursor = self.source.connection.cursor()

        cursor.execute("DESCRIBE `test_source`.`simple`")

        id = cursor.fetchone()
        self.assertEqual(id["Field"], "id")
        self.assertEqual(id["Type"], "int(11)")

        name = cursor.fetchone()
        self.assertEqual(name["Field"], "name")
        self.assertEqual(name["Type"], "varchar(255)")

    def test_migrate(self):

        migrations = relations.Migrations()

        migrations.generate([Unit])
        migrations.generate([Unit, Test])
        migrations.convert(self.source.name)

        self.assertTrue(self.source.migrate(f"ddl/{self.source.name}/{self.source.KIND}"))

        self.assertEqual(Unit.many().count(), 0)
        self.assertEqual(Test.many().count(), 0)

        self.assertFalse(self.source.migrate(f"ddl/{self.source.name}/{self.source.KIND}"))

        migrations.generate([Unit, Test, Case])
        migrations.convert(self.source.name)

        self.assertTrue(self.source.migrate(f"ddl/{self.source.name}/{self.source.KIND}"))

        self.assertEqual(Case.many().count(), 0)

        self.assertFalse(self.source.migrate(f"ddl/{self.source.name}/{self.source.KIND}"))

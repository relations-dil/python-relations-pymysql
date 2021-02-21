import unittest
import unittest.mock

import os
import pymysql.cursors

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

class Meta(SourceModel):
    id = int
    name = str
    flag = bool
    spend = float
    stuff = list
    things = dict

relations.OneToMany(Simple, Plain)

class Unit(SourceModel):
    id = int
    name = str

class Test(SourceModel):
    id = int
    unit_id = int
    name = str

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

    def tearDown(self):

        cursor = self.source.connection.cursor()
        cursor.execute("DROP DATABASE IF EXISTS `test_source`")

    @unittest.mock.patch("relations.SOURCES", {})
    @unittest.mock.patch("pymysql.connect", unittest.mock.MagicMock())
    def test___init__(self):

        source = relations_pymysql.Source("unit", "init", connection="corkneckshurn")
        self.assertFalse(source.created)
        self.assertEqual(source.name, "unit")
        self.assertEqual(source.database, "init")
        self.assertEqual(source.connection, "corkneckshurn")
        self.assertEqual(relations.SOURCES["unit"], source)

        source = relations_pymysql.Source("test", "init", host="db.com", extra="stuff")
        self.assertTrue(source.created)
        self.assertEqual(source.name, "test")
        self.assertEqual(source.database, "init")
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

        model = unittest.mock.MagicMock()
        model.DATABASE = None

        model.TABLE = "people"
        self.assertEqual(self.source.table(model), "`test_source`.`people`")

        model.DATABASE = "stuff"
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

        self.assertIsNone(model.DATABASE)
        self.assertEqual(model.TABLE, "check")
        self.assertEqual(model.QUERY.get(), "SELECT * FROM `test_source`.`check`")
        self.assertIsNone(model.DEFINITION)
        self.assertTrue(model._fields._names["id"].auto_increment)
        self.assertTrue(model._fields._names["id"].readonly)

    def test_field_define(self):

        def deffer():
            pass

        # Specific

        field = relations.Field(int, definition="id")
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["id"])

        # TINYINT

        field = relations.Field(bool, store="_flag")
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`_flag` TINYINT"])

        # TINYINT default

        field = relations.Field(bool, store="_flag", default=False)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`_flag` TINYINT NOT NULL DEFAULT 0"])

        # TINYINT function default

        field = relations.Field(bool, store="_flag", default=deffer)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`_flag` TINYINT NOT NULL"])

        # TINYINT none

        field = relations.Field(bool, store="_flag", none=False)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`_flag` TINYINT NOT NULL"])

        # INTEGER

        field = relations.Field(int, store="_id")
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`_id` INTEGER"])

        # INTEGER default

        field = relations.Field(int, store="_id", default=0)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`_id` INTEGER NOT NULL DEFAULT 0"])

        # INTEGER function default

        field = relations.Field(int, store="_id", default=deffer)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`_id` INTEGER NOT NULL"])

        # INTEGER none

        field = relations.Field(int, store="_id", none=False)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`_id` INTEGER NOT NULL"])

        # INTEGER auto_increment

        field = relations.Field(int, store="_id", auto_increment=True)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`_id` INTEGER AUTO_INCREMENT"])

        # INTEGER full

        field = relations.Field(int, store="_id", none=False, auto_increment=True, default=0)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`_id` INTEGER NOT NULL AUTO_INCREMENT DEFAULT 0"])

        # FLOAT

        field = relations.Field(float, store="spend")
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`spend` DOUBLE"])

        # FLOAT default

        field = relations.Field(float, store="spend", default=0.1)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`spend` DOUBLE NOT NULL DEFAULT 0.1"])

        # FLOAT function default

        field = relations.Field(float, store="spend", default=deffer)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`spend` DOUBLE NOT NULL"])

        # FLOAT none

        field = relations.Field(float, store="spend", none=False)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`spend` DOUBLE NOT NULL"])

        # VARCHAR

        field = relations.Field(str, name="name")
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`name` VARCHAR(255)"])

        # VARCHAR length

        field = relations.Field(str, name="name", length=32)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`name` VARCHAR(32)"])

        # VARCHAR default

        field = relations.Field(str, name="name", default='ya')
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`name` VARCHAR(255) NOT NULL DEFAULT 'ya'"])

        # VARCHAR function default

        field = relations.Field(str, name="name", default=deffer)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`name` VARCHAR(255) NOT NULL"])

        # VARCHAR none

        field = relations.Field(str, name="name", none=False)
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`name` VARCHAR(255) NOT NULL"])

        # VARCHAR full

        field = relations.Field(str, name="name", length=32, none=False, default='ya')
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ["`name` VARCHAR(32) NOT NULL DEFAULT 'ya'"])

        # JSON (list)

        field = relations.Field(list, name='stuff')
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['`stuff` JSON NOT NULL'])

        # JSON (dict)

        field = relations.Field(dict, name='things')
        self.source.field_init(field)
        definitions = []
        self.source.field_define(field, definitions)
        self.assertEqual(definitions, ['`things` JSON NOT NULL'])


    def test_model_define(self):

        class Simple(relations.Model):

            SOURCE = "PyMySQLSource"
            DEFINITION = "whatever"

            id = int
            name = str

            INDEX = "id"

        self.assertEqual(Simple.define(), "whatever")

        Simple.DEFINITION = None
        self.assertEqual(Simple.define(), """CREATE TABLE IF NOT EXISTS `test_source`.`simple` (
  `id` INTEGER AUTO_INCREMENT,
  `name` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE `name` (`name`),
  INDEX `id` (`id`)
)""")

        cursor = self.source.connection.cursor()
        cursor.execute(Simple.define())
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

        # readonly

        field = relations.Field(int, name="id", readonly=True)
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
        cursor.execute(Simple.define())
        cursor.execute(Plain.define())
        cursor.execute(Meta.define())

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

        yep = Meta("yep", True, 1.1, [1], {"a": 1}).create()
        cursor.execute("SELECT * FROM test_source.meta")
        self.assertEqual(cursor.fetchone(), {"id": 1, "name": "yep", "flag": True, "spend": 1.1, "stuff": '[1]', "things": '{"a": 1}'})

        cursor.close()

    def test_field_retrieve(self):

        # IN

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter([1, 2, 3], "in")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, "`id` IN (%s,%s,%s)")
        self.assertEqual(values, [1, 2, 3])

        # NOT IN

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter([1, 2, 3], "ne")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, "`id` NOT IN (%s,%s,%s)")
        self.assertEqual(values, [1, 2, 3])

        # =

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter(1)
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, "`id`=%s")
        self.assertEqual(values, [1])

        # >

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter(1, "gt")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, "`id`>%s")
        self.assertEqual(values, [1])

        # >=

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter(1, "ge")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, "`id`>=%s")
        self.assertEqual(values, [1])

        # <

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter(1, "lt")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, "`id`<%s")
        self.assertEqual(values, [1])

        # <=

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        field.filter(1, "le")
        query = relations.query.Query()
        values = []
        self.source.field_retrieve( field, query, values)
        self.assertEqual(query.wheres, "`id`<=%s")
        self.assertEqual(values, [1])

    def test_model_retrieve(self):

        model = Unit()

        cursor = self.source.connection.cursor()

        cursor.execute(Unit.define())
        cursor.execute(Test.define())
        cursor.execute(Case.define())
        cursor.execute(Meta.define())

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

        Meta("yep", True, 1.1, [1], {"a": 1}).create()
        model = Meta.one(name="yep")

        self.assertEqual(model.flag, True)
        self.assertEqual(model.spend, 1.1)
        self.assertEqual(model.stuff, [1])
        self.assertEqual(model.things, {"a": 1})

        self.assertEqual(Unit.many().name, ["people", "stuff"])
        self.assertEqual(Unit.many().sort("-name").name, ["stuff", "people"])

    def test_field_update(self):

        # Standard

        field = relations.Field(int, name="id")
        self.source.field_init(field)
        clause = []
        values = []
        field.value = 1
        self.source.field_update(field, clause, values)
        self.assertEqual(clause, ["`id`=%s"])
        self.assertEqual(values, [1])
        self.assertFalse(field.changed)

        # replace

        field = relations.Field(int, name="id", default=-1, replace=True)
        self.source.field_init(field)
        clause = []
        values = []
        field.value = 1
        self.source.field_update(field, clause, values)
        self.assertEqual(clause, ['`id`=%s'])
        self.assertEqual(values, [1])

        field.changed = False
        clause = []
        values = []
        self.source.field_update(field, clause, values)
        self.assertEqual(clause, ['`id`=%s'])
        self.assertEqual(values, [-1])

        # not changed

        field = relations.Field(int, name="id")
        clause = []
        values = []
        self.source.field_update(field, clause, values, changed=True)
        self.assertEqual(clause, [])
        self.assertEqual(values, [])
        self.assertFalse(field.changed)

        # readonly

        field = relations.Field(int, name="id", readonly=True)
        self.source.field_init(field)
        clause = []
        values = []
        field.value = 1
        self.source.field_update( field, clause, values)
        self.assertEqual(clause, [])
        self.assertEqual(values, [])

    def test_model_update(self):

        cursor = self.source.connection.cursor()

        cursor.execute(Unit.define())
        cursor.execute(Test.define())
        cursor.execute(Case.define())
        cursor.execute(Meta.define())

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

        Meta("yep", True, 1.1, [1], {"a": 1}).create()
        Meta.one(name="yep").set(flag=False, stuff=[], things={}).update()

        model = Meta.one(name="yep")
        self.assertEqual(model.flag, False)
        self.assertEqual(model.spend, 1.1)
        self.assertEqual(model.stuff, [])
        self.assertEqual(model.things, {})

        plain = Plain.one()
        self.assertRaisesRegex(relations.ModelError, "plain: nothing to update from", plain.update)

    def test_model_delete(self):

        cursor = self.source.connection.cursor()

        cursor.execute(Unit.define())
        cursor.execute(Test.define())
        cursor.execute(Case.define())

        unit = Unit("people")
        unit.test.add("stuff").add("things")
        unit.create()

        self.assertEqual(Test.one(id=2).delete(), 1)
        self.assertEqual(len(Test.many()), 1)

        self.assertEqual(Unit.one(1).test.delete(), 1)
        self.assertEqual(Unit.one(1).retrieve().delete(), 1)
        self.assertEqual(len(Unit.many()), 0)
        self.assertEqual(len(Test.many()), 0)

        cursor.execute(Plain.define())

        plain = Plain(0, "nope").create()
        self.assertRaisesRegex(relations.ModelError, "plain: nothing to delete from", plain.delete)

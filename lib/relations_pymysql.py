"""
Module for intersting with PyMySQL
"""

# pylint: disable=arguments-differ

import glob
import copy
import json

import pymysql
import pymysql.cursors

import relations
import relations.query

class Source(relations.Source): # pylint: disable=too-many-public-methods
    """
    PyMySQL Source
    """

    KIND = "mysql"

    RETRIEVE = {
        'eq': '=',
        'gt': '>',
        'gte': '>=',
        'lt': '<',
        'lte': '<='
    }

    database = None   # Database to use
    connection = None # Connection
    created = False   # If we created the connection

    def __init__(self, name, database, connection=None, **kwargs):

        self.database = database

        if connection is not None:
            self.connection = connection
        else:
            self.created = True
            self.connection = pymysql.connect(
                cursorclass=pymysql.cursors.DictCursor,
                **{name: arg for name, arg in kwargs.items() if name not in ["name", "database", "connection"]}
            )

    def __del__(self):

        if self.created and self.connection:
            self.connection.close()

    def table(self, model):
        """
        Get the full table name
        """

        if isinstance(model, dict):
            database = model.get("database")
            table = model['table']
        else:
            database = model.DATABASE
            table = model.TABLE

        name = []

        if database is not None:
            name.append(f"`{database}`")
        elif self.database is not None:
            name.append(f"`{self.database}`")

        name.append(f"`{table}`")

        return ".".join(name)

    @staticmethod
    def encode(model, values):
        """
        Encodes the fields in json if needed
        """
        for field in model._fields._order:
            if values.get(field.store) is not None and field.kind not in [bool, int, float, str]:
                values[field.store] = json.dumps(values[field.store])

        return values

    @staticmethod
    def decode(model, values):
        """
        Encodes the fields in json if needed
        """
        for field in model._fields._order:
            if values.get(field.store) is not None and field.kind not in [bool, int, float, str]:
                values[field.store] = json.loads(values[field.store])

        return values

    @staticmethod
    def walk(path):
        """
        Generates the JSON pathing for a field
        """

        if isinstance(path, str):
            path = path.split('__')

        places = []

        for place in path:

            if relations.INDEX.match(place):
                places.append(f"[{int(place)}]")
            elif place[0] == '_':
                places.append(f'."{place[1:]}"')
            else:
                places.append(f".{place}")

        return f"${''.join(places)}"

    def field_init(self, field):
        """
        Make sure there's auto_increment
        """

        self.ensure_attribute(field, "auto_increment")
        self.ensure_attribute(field, "definition")

    def model_init(self, model):
        """
        Init the model
        """

        self.record_init(model._fields)

        self.ensure_attribute(model, "DATABASE")
        self.ensure_attribute(model, "TABLE")
        self.ensure_attribute(model, "QUERY")
        self.ensure_attribute(model, "DEFINITION")

        model.UNDEFINE.append("QUERY")

        if model.TABLE is None:
            model.TABLE = model.NAME

        if model.QUERY is None:
            model.QUERY = relations.query.Query(selects='*', froms=self.table(model))

        if model._id is not None and model._fields._names[model._id].auto_increment is None:
            model._fields._names[model._id].auto_increment = True
            model._fields._names[model._id].auto = True

    def column_define(self, field):
        """
        Defines just the column for field
        """

        if field.get('definition') is not None:
            return field['definition']

        definition = [f"`{field['store']}`"]

        default = None

        if field['kind'] == 'bool':

            definition.append("TINYINT")

            if field.get('default') is not None:
                default = f"DEFAULT {int(field['default'])}"

        elif field['kind'] == 'int':

            definition.append("INTEGER")

            if field.get('default') is not None:
                default = f"DEFAULT {field['default']}"

        elif field['kind'] == 'float':

            definition.append("DOUBLE")

            if field.get('default') is not None:
                default = f"DEFAULT {field['default']}"

        elif field['kind'] == 'str':

            length = field['length'] if field.get('length') is not None else 255

            definition.append(f"VARCHAR({length})")

            if field.get('default') is not None:
                default = f"DEFAULT '{field['default']}'"

        else:

            definition.append("JSON")

        if not field['none']:
            definition.append("NOT NULL")

        if field.get('auto_increment'):
            definition.append("AUTO_INCREMENT")

        if default:
            definition.append(default)

        return " ".join(definition)

    def extract_define(self, store, path, kind):
        """
        Createa and extract store
        """

        definition = [f"`{store}__{path}`"]

        if kind == 'bool':
            definition.append("TINYINT")
        elif kind == 'int':
            definition.append("INTEGER")
        elif kind == 'float':
            definition.append("DOUBLE")
        elif kind == 'str':
            definition.append("VARCHAR(255)")
        else:
            definition.append("JSON")

        definition.append(f"AS (`{store}`->>'{self.walk(path)}')")

        return " ".join(definition)

    def field_define(self, field, definitions, extract=True):
        """
        Add what this field is the definition
        """

        if field.get('inject'):
            return

        definitions.append(self.column_define(field))

        if extract:
            for path in sorted(field.get('extract', {}).keys()):
                definitions.append(self.extract_define(field['store'], path, field['extract'][path]))

    @staticmethod
    def index_define(name, fields, unique=False):
        """
        Defines an index
        """

        return f"{'UNIQUE' if unique else 'INDEX'} `{name.replace('-', '_')}` (`{'`,`'.join(fields)}`)"

    def model_define(self, model):
        """
        Defines the model
        """

        if model.get('definition') is not None:
            return [model['definition']]

        definitions = []

        self.record_define(model['fields'], definitions)

        if model.get('id') is not None:
            definitions.append(f"PRIMARY KEY (`{model['id']}`)")

        for name in sorted(model['unique'].keys()):
            definitions.append(self.index_define(name, model['unique'][name], unique=True))

        for name in sorted(model['index'].keys()):
            definitions.append(self.index_define(name, model['index'][name]))

        sep = ',\n  '
        return [f"CREATE TABLE IF NOT EXISTS {self.table(model)} (\n  {sep.join(definitions)}\n)"]

    def field_add(self, migration, migrations):
        """
        add the field
        """

        if migration.get('inject'):
            return

        migrations.append(f"ADD {self.column_define(migration)}")

        for path in sorted(migration.get('extract', {}).keys()):
            migrations.append(f"ADD {self.extract_define(migration['store'], path, migration['extract'][path])}")

    def field_remove(self, definition, migrations):
        """
        remove the field
        """

        if definition.get('inject'):
            return

        migrations.append(f"DROP `{definition['store']}`")

        for path in sorted(definition.get('extract', {}).keys()):
            migrations.append(f"DROP `{definition['store']}__{path}`")

    def field_change(self, definition, migration, migrations):
        """
        change the field
        """

        if definition.get('inject'):
            return

        # This is a little heavy handed but simpler, any changes will be propagated

        migrations.append(f"CHANGE `{definition['store']}` {self.column_define({**definition, **migration})}")

        store = migration.get('store', definition['store'])
        extract = migration.get('extract', definition.get('extract', {}))

        # Remove all the ones that were there and now aren't

        for path in sorted(definition.get('extract', {}).keys()):
            if path not in extract:
                migrations.append(f"DROP `{definition['store']}__{path}`")

        # Add the ones that are new

        for path in sorted(extract.keys()):

            # Add the ones that are new

            if path not in definition.get("extract"):
                migrations.append(f"ADD {self.extract_define(store, path, migration['extract'][path])}")

            # if the field name or extract kind has changed, change

            elif definition['store'] != store or definition["extract"][path] != extract[path]:
                migrations.append(f"CHANGE `{definition['store']}__{path}` {self.extract_define(store, path, extract[path])}")

    def model_add(self, definition):
        """
        migrate the model
        """

        return self.model_define(definition)

    def model_remove(self, definition):
        """
        remove the model
        """

        return [f"DROP TABLE IF EXISTS {self.table(definition)}"]

    def model_change(self, definition, migration):
        """
        change the model
        """

        migrations = []

        definition_table = self.table(definition)
        migration_table = self.table({
            "database": migration.get("database", definition.get("database")),
            "table": migration.get("table", definition["table"])
        })

        if definition_table != migration_table:
            migrations.append(f"RENAME TO {migration_table}")

        self.record_change(definition['fields'], migration.get("fields", {}), migrations)

        for name in sorted(migration.get("unique", {}).get("add", {}).keys()):
            migrations.append(f"ADD {self.index_define(name, migration['unique']['add'][name], unique=True)}")

        for name in sorted(migration.get("unique", {}).get("remove", [])):
            migrations.append(f"DROP INDEX `{name.replace('-', '_')}`")

        for name in sorted(migration.get("unique", {}).get("rename", {}).keys()):
            migrations.append(f"RENAME INDEX `{name.replace('-', '_')}` TO `{migration['unique']['rename'][name].replace('-', '_')}`")

        for name in sorted(migration.get("index", {}).get("add", {}).keys()):
            migrations.append(f"ADD {self.index_define(name, migration['index']['add'][name])}")

        for name in sorted(migration.get("index", {}).get("remove", [])):
            migrations.append(f"DROP INDEX `{name.replace('-', '_')}`")

        for name in sorted(migration.get("index", {}).get("rename", {}).keys()):
            migrations.append(f"RENAME INDEX `{name.replace('-', '_')}` TO `{migration['index']['rename'][name].replace('-', '_')}`")

        sep = ',\n  '
        return [f"ALTER TABLE {definition_table}\n  {sep.join(migrations)}"]

    def field_create(self, field, fields, clause):
        """
        Adds values to clause if not auto
        """

        if not field.auto and not field.inject:
            fields.append(f"`{field.store}`")
            clause.append(f"%({field.store})s")

    def model_create(self, model):
        """
        Executes the create
        """

        cursor = self.connection.cursor()

        # Create the insert query

        fields = []
        clause = []

        self.record_create(model._fields, fields, clause)

        query = f"INSERT INTO {self.table(model)} ({','.join(fields)}) VALUES({','.join(clause)})"

        if not model._bulk and model._id is not None and model._fields._names[model._id].auto_increment:
            for creating in model._each("create"):
                cursor.execute(query, self.encode(creating, creating._record.create({})))
                creating[model._id] = cursor.lastrowid
        else:
            cursor.executemany(query, [
                self.encode(creating, creating._record.create({})) for creating in model._each("create")
            ])

        cursor.close()

        if not model._bulk:

            for creating in model._each("create"):
                for parent_child in creating.CHILDREN:
                    if creating._children.get(parent_child):
                        creating._children[parent_child].create()
                creating._action = "update"
                creating._record._action = "update"

            model._action = "update"

        else:

            model._models = []

        return model

    def field_retrieve(self, field, query, values): # pylint: disable=too-many-branches
        """
        Adds where caluse to query
        """

        for operator, value in (field.criteria or {}).items():

            if operator not in relations.Field.OPERATORS:

                path, operator = operator.rsplit("__", 1)

                if path in (field.extract or {}):

                    store = store = f'`{field.store}__{path}`'

                else:

                    values.append(self.walk(path))
                    store = f"`{field.store}`->>%s"

            else:

                store = f"`{field.store}`"

            if operator == "in":
                if value:
                    query.add(wheres=f"{store} IN ({','.join(['%s' for _ in value])})")
                    values.extend(value)
                else:
                    query.add(wheres="FALSE")
            elif operator == "ne":
                if value:
                    query.add(wheres=f"{store} NOT IN ({','.join(['%s' for _ in value])})")
                    values.extend(value)
                else:
                    query.add(wheres="TRUE")
            elif operator == "like":
                query.add(wheres=f'{store} LIKE %s')
                values.append(f"%{value}%")
            elif operator == "notlike":
                query.add(wheres=f'{store} NOT LIKE %s')
                values.append(f"%{value}%")
            elif operator == "null":
                query.add(wheres=f'{store} {"IS" if value else "IS NOT"} NULL')
            else:
                query.add(wheres=f"{store}{self.RETRIEVE[operator]}%s")
                values.append(value)

    @classmethod
    def model_like(cls, model, query, values):
        """
        Adds like information to the query
        """

        if model._like is None:
            return

        ors = []

        for name in model._label:

            path = name.split("__", 1)
            name = path.pop(0)

            field = model._fields._names[name]

            parent = False

            for relation in model.PARENTS.values():
                if field.name == relation.child_field:
                    parent = relation.Parent.many(like=model._like).limit(model._chunk)
                    if parent[relation.parent_field]:
                        ors.append(f'`{field.store}` IN ({",".join(["%s" for _ in parent[relation.parent_field]])})')
                        values.extend(parent[relation.parent_field])
                        model.overflow = model.overflow or parent.overflow
                    else:
                        parent = True

            if not parent:

                paths = path if path else field.label

                if paths:

                    for path in paths:

                        if path in (field.extract or {}):

                            ors.append(f'`{field.store}__{path}` LIKE %s')
                            values.append(f"%{model._like}%")

                        else:

                            ors.append(f"`{field.store}`->>%s LIKE %s")
                            values.append(cls.walk(path))
                            values.append(f"%{model._like}%")

                else:

                    ors.append(f'`{field.store}` LIKE %s')
                    values.append(f"%{model._like}%")

        query.add(wheres="(%s)" % " OR ".join(ors))

    @staticmethod
    def model_sort(model, query):
        """
        Adds sort information to the query
        """

        sort = model._sort or model._order

        if sort:
            order_bys = []
            for field in sort:
                order_bys.append(f'`{field[1:]}`' if field[0] == "+" else f'`{field[1:]}` DESC')
            query.add(order_bys=order_bys)

        model._sort = None

    @staticmethod
    def model_limit(model, query, values):
        """
        Adds sort informaiton to the query
        """

        if model._limit is None:
            return

        if model._limit is not None:
            if model._offset:
                query.add(limits="%s, %s")
                values.extend([model._offset, model._limit])
            else:
                query.add(limits="%s")
                values.append(model._limit)

    def model_count(self, model):
        """
        Executes the count
        """

        model._collate()

        cursor = self.connection.cursor()

        query = copy.deepcopy(model.QUERY)
        query.set(selects="COUNT(*) AS total")

        values = []

        self.record_retrieve(model._record, query, values)

        self.model_like(model, query, values)

        cursor.execute(query.get(), values)

        total = cursor.fetchone()["total"] if cursor.rowcount else 0

        cursor.close()

        return total

    def model_retrieve(self, model, verify=True):
        """
        Executes the retrieve
        """

        model._collate()

        cursor = self.connection.cursor()

        query = copy.deepcopy(model.QUERY)
        values = []

        self.record_retrieve(model._record, query, values)

        self.model_like(model, query, values)
        self.model_sort(model, query)
        self.model_limit(model, query, values)

        cursor.execute(query.get(), values)

        if model._mode == "one" and cursor.rowcount > 1:
            raise relations.ModelError(model, "more than one retrieved")

        if model._mode == "one" and model._role != "child":

            if cursor.rowcount < 1:

                if verify:
                    raise relations.ModelError(model, "none retrieved")
                return None

            model._record = model._build("update", _read=self.decode(model, cursor.fetchone()))

        else:

            model._models = []

            while len(model._models) < cursor.rowcount:
                model._models.append(model.__class__(_read=self.decode(model, cursor.fetchone())))

            if model._limit is not None:
                model.overflow = model.overflow or len(model._models) >= model._limit

            model._record = None

        model._action = "update"

        cursor.close()

        return model

    def model_labels(self, model):
        """
        Creates the labels structure
        """

        if model._action == "retrieve":
            self.model_retrieve(model)

        labels = relations.Labels(model)

        for labeling in model._each():
            labels.add(labeling)

        return labels

    def field_update(self, field, updates, clause, values):
        """
        Preps values from dict
        """

        if field.store in updates:
            clause.append(f"`{field.store}`=%s")
            if field.kind not in [bool, int, float, str] and updates[field.store] is not None:
                values.append(json.dumps(updates[field.store]))
            else:
                values.append(updates[field.store])

    def model_update(self, model):
        """
        Executes the update
        """

        cursor = self.connection.cursor()

        updated = 0

        # If the overall model is retrieving and the record has values set

        if model._action == "retrieve" and model._record._action == "update":

            # Build the SET clause first

            clause = []
            values = []

            self.record_update(model._record, model._record.mass({}), clause, values)

            # Build the WHERE clause next

            where = relations.query.Query()
            self.record_retrieve(model._record, where, values)

            query = f"UPDATE {self.table(model)} SET {relations.sql.assign_clause(clause)} {where.get()}"

            cursor.execute(query, values)

            updated = cursor.rowcount

        elif model._id:

            store = model._fields._names[model._id].store

            for updating in model._each("update"):

                clause = []
                values = []

                self.record_update(updating._record, updating._record.update({}), clause, values)

                if clause:

                    values.append(updating[model._id])

                    query = f"UPDATE {self.table(model)} SET {relations.sql.assign_clause(clause)} WHERE `{store}`=%s"

                    cursor.execute(query, values)

                for parent_child in updating.CHILDREN:
                    if updating._children.get(parent_child):
                        updating._children[parent_child].create().update()

                updated += cursor.rowcount

        else:

            raise relations.ModelError(model, "nothing to update from")

        return updated

    def model_delete(self, model):
        """
        Executes the delete
        """

        cursor = self.connection.cursor()

        if model._action == "retrieve":

            where = relations.query.Query()
            values = []
            self.record_retrieve(model._record, where, values)

            query = f"DELETE FROM {self.table(model)} {where.get()}"

        elif model._id:

            store = model._fields._names[model._id].store
            values = []

            for deleting in model._each():
                values.append(deleting[model._id])

            query = f"DELETE FROM {self.table(model)} WHERE `{store}` IN ({','.join(['%s'] * len(values))})"

        else:

            raise relations.ModelError(model, "nothing to delete from")

        cursor.execute(query, values)

        return cursor.rowcount

    def definition_convert(self, file_path, source_path):
        """"
        Converts a definition file to a MySQL definition file
        """

        definitions = []

        with open(file_path, "r") as definition_file:
            definition = json.load(definition_file)
            for name in sorted(definition.keys()):
                if definition[name]["source"] == self.name:
                    definitions.extend(self.model_define(definition[name]))

        if definitions:
            file_name = file_path.split("/")[-1].split('.')[0]
            with open(f"{source_path}/{file_name}.sql", "w") as source_file:
                source_file.write(";\n\n".join(definitions))
                source_file.write(";\n")

    def migration_convert(self, file_path, source_path):
        """"
        Converts a migration file to a source definition file
        """

        migrations = []

        with open(file_path, "r") as migration_file:
            migration = json.load(migration_file)

            for add in sorted(migration.get('add', {}).keys()):
                if migration['add'][add]["source"] == self.name:
                    migrations.extend(self.model_add(migration['add'][add]))

            for remove in sorted(migration.get('remove', {}).keys()):
                if migration['remove'][remove]["source"] == self.name:
                    migrations.extend(self.model_remove(migration['remove'][remove]))

            for change in sorted(migration.get('change', {}).keys()):
                if migration['change'][change]['definition']["source"] == self.name:
                    migrations.extend(
                        self.model_change(migration['change'][change]['definition'], migration['change'][change]['migration'])
                    )

        if migrations:
            file_name = file_path.split("/")[-1].split('.')[0]
            with open(f"{source_path}/{file_name}.sql", "w") as source_file:
                source_file.write(";\n\n".join(migrations))
                source_file.write(";\n")

    def execute(self, commands):
        """
        Execute one or more commands
        """

        if not isinstance(commands, list):
            commands = [commands]

        cursor = self.connection.cursor()

        for command in commands:
            if command.strip():
                cursor.execute(command)

        self.connection.commit()

        cursor.close()

    def migrate(self, source_path):
        """
        Migrate all the existing files to where we are
        """

        migrated = False

        cursor = self.connection.cursor()

        cursor.execute("""
            SELECT COUNT(*) AS `migrations`
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            LIMIT 1
        """, (self.database, "_relations_migrations"))

        migrations = cursor.fetchone()['migrations']

        migration_paths = sorted(glob.glob(f"{source_path}/migration-*.sql"))

        if not migrations:

            cursor.execute(f"""
                CREATE TABLE `{self.database}`.`_relations_migrations` (
                    `migration` VARCHAR(255) NOT NULL,
                    PRIMARY KEY (`migration`)
                );
            """)

            with open(f"{source_path}/definition.sql", 'r') as definition_file:
                self.execute(definition_file.read().split(";\n"))
                migrated = True

        else:

            cursor.execute(f"SELECT `migration` FROM `{self.database}`.`_relations_migrations` ORDER BY `migration`")

            migrations = [row['migration'] for row in cursor.fetchall()]

            for migration_path in migration_paths:
                if migration_path.rsplit("/migration-", 1)[-1].split('.')[0] not in migrations:
                    with open(migration_path, 'r') as migration_file:
                        self.execute(migration_file.read().split(";\n"))
                    migrated = True

        for migration_path in migration_paths:
            migration = migration_path.rsplit("/migration-", 1)[-1].split('.')[0]
            if not migrations or migration not in migrations:
                cursor.execute(
                    f"INSERT INTO `{self.database}`.`_relations_migrations` VALUES (%s)",
                    (migration, )
                )

        self.connection.commit()

        return migrated

from __future__ import annotations

import re
from decimal import Decimal

import pytest
from pretix.base.models import (
    Item,
    ItemVariation,
    Order,
    OrderPosition,
    Question,
    QuestionAnswer,
    QuestionOption,
)

from pretix_nocodb.sync import (
    MAX_COLUMN_TITLE_LENGTH,
    ORDER_KEY_FIELD,
    ORDER_LINK_FIELD,
    ORDERS_COLUMNS,
    PARTICIPANT_KEY_FIELD,
    PARTICIPANTS_COLUMNS,
    STATUS_OPTIONS,
    TABLE_ORDERS,
    TABLE_PARTICIPANTS,
    NocoDBSyncService,
)

pytestmark = pytest.mark.django_db


class FakeNocoDBClient:
    def __init__(self):
        self.bases: list[dict] = []
        self.tables: dict[str, dict] = {}
        self.views: dict[str, list[dict]] = {}
        self.view_columns: dict[str, list[dict]] = {}
        self.records: dict[str, list[dict]] = {}
        self.links: dict[str, list[tuple[str, int, str, int]]] = {}
        self.base_counter = 1
        self.table_counter = 1
        self.column_counter = 1
        self.view_counter = 1
        self.view_column_counter = 1
        self.record_counter = 1
        self.junction_counter = 1

    def list_bases(self, _workspace_id: str = "", *, _page_size: int = 200):
        return self.bases

    def create_base(self, title: str, *, workspace_id: str = ""):
        base = {"id": f"p_{self.base_counter}", "title": title, "workspace_id": workspace_id}
        self.base_counter += 1
        self.bases.append(base)
        return base

    def list_tables(self, base_id: str, *, _page_size: int = 200):
        return [
            {"id": table["id"], "title": table["title"]}
            for table in self.tables.values()
            if table["base_id"] == base_id
        ]

    def get_table(self, table_id: str):
        return self.tables[table_id]

    def create_table(self, base_id: str, *, title: str, columns: list[dict]):
        table_id = f"m_{self.table_counter}"
        self.table_counter += 1
        table = {"id": table_id, "base_id": base_id, "title": title, "columns": []}
        self.tables[table_id] = table
        self.records[table_id] = []
        view_id = f"v_{self.view_counter}"
        self.view_counter += 1
        self.views[table_id] = [{"id": view_id, "title": title, "type": 0}]
        self.view_columns[view_id] = []
        for column in columns:
            self.create_column(table_id, column)
        return table

    def _next_column_id(self) -> str:
        column_id = f"c_{self.column_counter}"
        self.column_counter += 1
        return column_id

    def _column_aliases(self, table_id: str) -> dict[str, str]:
        aliases = {}
        for column in self.tables[table_id]["columns"]:
            title = column.get("title")
            column_name = column.get("column_name")
            if title:
                aliases[title] = title
            if column_name:
                aliases[column_name] = title or column_name
        return aliases

    def _canonical_field(self, table_id: str, field: str) -> str:
        if field == "Id":
            return "Id"
        return self._column_aliases(table_id).get(field, field)

    def _unique_column_name(self, table_id: str, base_name: str) -> str:
        existing = {column.get("column_name") for column in self.tables[table_id]["columns"]}
        if base_name not in existing:
            return base_name
        index = 1
        while f"{base_name}{index}" in existing:
            index += 1
        return f"{base_name}{index}"

    def create_column(self, table_id: str, column: dict):
        created = {
            "id": self._next_column_id(),
            "fk_model_id": table_id,
            "title": column["title"],
            "column_name": column.get("column_name", column["title"]),
            "uidt": column["uidt"],
            "description": column.get("description"),
            "colOptions": column.get("colOptions"),
            "pv": bool(column.get("pv")),
            "rqd": bool(column.get("rqd")),
        }
        existing = self.tables[table_id]["columns"]
        if any(c["column_name"] == created["column_name"] for c in existing):
            raise ValueError("duplicate column")
        existing.append(created)
        for view in self.views.get(table_id, []):
            vc_id = f"vc_{self.view_column_counter}"
            self.view_column_counter += 1
            self.view_columns.setdefault(view["id"], []).append({
                "id": vc_id,
                "fk_view_id": view["id"],
                "fk_column_id": created["id"],
                "show": True,
            })
        return created

    def create_link_column(
        self,
        table_id: str,
        *,
        title: str,
        child_id: str,
        parent_id: str,
        relation_type: str = "mo",
    ):
        # v2 semantics: link column lives on `table_id` (== parent_id), references `child_id`.
        # A reciprocal Links column with the inverse type is created on `child_id`.
        assert parent_id == table_id
        related_table = self.tables[child_id]
        junction_id = f"j_{self.junction_counter}"
        self.junction_counter += 1
        inverse = {"mo": "om", "om": "mo", "mm": "mm", "oo": "oo"}[relation_type]
        link_column = {
            "id": self._next_column_id(),
            "fk_model_id": table_id,
            "title": title,
            "column_name": None,
            "uidt": "Links",
            "virtual": True,
            "colOptions": {
                "type": relation_type,
                "fk_related_model_id": child_id,
                "fk_mm_model_id": junction_id,
            },
        }
        reciprocal_column = {
            "id": self._next_column_id(),
            "fk_model_id": child_id,
            "title": self.tables[table_id]["title"],
            "column_name": None,
            "uidt": "Links",
            "virtual": True,
            "colOptions": {
                "type": inverse,
                "fk_related_model_id": table_id,
                "fk_mm_model_id": junction_id,
            },
        }
        self.tables[table_id]["columns"].append(link_column)
        related_table["columns"].append(reciprocal_column)
        self.links.setdefault(junction_id, [])
        return self.tables[table_id]

    def _find_link_column(self, link_column_id: str) -> dict:
        for table in self.tables.values():
            for column in table["columns"]:
                if column.get("id") == link_column_id and column.get("uidt") == "Links":
                    return column
        raise KeyError(link_column_id)

    def link_records(
        self,
        table_id: str,
        link_column_id: str,
        record_id: int,
        linked_id: int,
    ):
        link_col = self._find_link_column(link_column_id)
        junction = link_col["colOptions"]["fk_mm_model_id"]
        related_table_id = link_col["colOptions"]["fk_related_model_id"]
        entries = self.links.setdefault(junction, [])
        pair = (table_id, record_id, related_table_id, linked_id)
        if pair not in entries:
            entries.append(pair)
        return None

    def list_linked_records(
        self,
        table_id: str,
        link_column_id: str,
        record_id: int,
        *,
        fields=None,
        limit: int = 200,
    ):
        link_col = self._find_link_column(link_column_id)
        junction = link_col["colOptions"]["fk_mm_model_id"]
        related_table_id = link_col["colOptions"]["fk_related_model_id"]
        linked_ids: list[int] = []
        for src_table, src_id, tgt_table, tgt_id in self.links.get(junction, []):
            if src_table == table_id and src_id == record_id:
                linked_ids.append(tgt_id)
            elif tgt_table == table_id and tgt_id == record_id:
                linked_ids.append(src_id)
        related_records = [
            row for row in self.records[related_table_id] if row["Id"] in linked_ids
        ]
        if fields:
            related_records = [
                {
                    field: row.get(self._canonical_field(related_table_id, field))
                    for field in fields
                }
                for row in related_records
            ]
        return related_records[:limit]

    def update_column(self, column_id: str, payload: dict):
        for table in self.tables.values():
            for column in table["columns"]:
                if column["id"] == column_id:
                    column.update(payload)
                    return table
        raise KeyError(column_id)

    def set_primary_column(self, column_id: str):
        for table in self.tables.values():
            target = next((c for c in table["columns"] if c["id"] == column_id), None)
            if target is None:
                continue
            for column in table["columns"]:
                column["pv"] = column is target
            return table
        raise KeyError(column_id)

    def delete_column(self, column_id: str):
        for table in self.tables.values():
            for index, column in enumerate(table["columns"]):
                if column["id"] != column_id:
                    continue
                removed = table["columns"].pop(index)
                removed_keys = {
                    key for key in (removed.get("title"), removed.get("column_name")) if key
                }
                for record in self.records[table["id"]]:
                    for key in removed_keys:
                        record.pop(key, None)
                return True
        raise KeyError(column_id)

    def list_views(self, table_id: str) -> list[dict]:
        return list(self.views.get(table_id, []))

    def update_view(self, view_id: str, payload: dict) -> dict:
        for views in self.views.values():
            for view in views:
                if view["id"] == view_id:
                    view.update(payload)
                    return view
        raise KeyError(view_id)

    def list_view_columns(self, view_id: str) -> list[dict]:
        return list(self.view_columns.get(view_id, []))

    def update_view_column(self, view_id: str, view_column_id: str, payload: dict) -> dict:
        for vc in self.view_columns.get(view_id, []):
            if vc["id"] == view_column_id:
                vc.update(payload)
                return vc
        raise KeyError(view_column_id)

    def list_records(
        self,
        table_id: str,
        *,
        where: str | None = None,
        fields=None,
        offset: int = 0,
        limit: int = 200,
    ):
        records = list(self.records[table_id])
        if where:
            match = re.match(
                r'@\("(?P<field>[^"]+)",(?P<op>eq|in),(?P<rest>.+)\)$', where
            )
            assert match, where
            field = self._canonical_field(table_id, match.group("field"))
            op = match.group("op")
            rest = match.group("rest")
            if op == "eq":
                if rest.startswith('"') and rest.endswith('"'):
                    value = rest[1:-1].replace('\\"', '"').replace("\\\\", "\\")
                else:
                    value = int(rest)
                records = [record for record in records if record.get(field) == value]
            else:
                values = {int(part) for part in rest.split(",")}
                records = [record for record in records if record.get(field) in values]
        if fields:
            records = [
                {
                    field: record.get(self._canonical_field(table_id, field))
                    for field in fields
                }
                for record in records
            ]
        return records[offset : offset + limit]

    def create_records(self, table_id: str, records: list[dict]):
        aliases = self._column_aliases(table_id)
        created = []
        for record in records:
            unknown = set(record) - set(aliases)
            assert not unknown, f"unknown columns: {unknown}"
            stored = {aliases[key]: value for key, value in record.items()}
            stored["Id"] = self.record_counter
            self.record_counter += 1
            self.records[table_id].append(stored)
            created.append({"Id": stored["Id"]})
        return created

    def update_records(self, table_id: str, records: list[dict]):
        aliases = self._column_aliases(table_id)
        updated = []
        for record in records:
            unknown = set(record) - set(aliases) - {"Id"}
            assert not unknown, f"unknown columns: {unknown}"
            target = next(row for row in self.records[table_id] if row["Id"] == record["Id"])
            target.update({aliases[key]: value for key, value in record.items() if key != "Id"})
            updated.append({"Id": record["Id"]})
        return updated

    def delete_records(self, table_id: str, records: list[dict]):
        ids = {record["Id"] for record in records}
        self.records[table_id] = [
            record for record in self.records[table_id] if record["Id"] not in ids
        ]
        return [{"Id": record_id} for record_id in ids]


class MissingColumnNameResponseClient(FakeNocoDBClient):
    def create_column(self, table_id: str, column: dict):
        super().create_column(table_id, column)
        return self.tables[table_id]


def _attach_base(event, client) -> str:
    base = client.create_base("pretix")
    event.settings.set("plugin_nocodb_base_id", base["id"])
    return base["id"]


def test_sync_creates_schema_before_participant_rows(event, order):
    item = Item.objects.create(
        event=event,
        name="Conference ticket",
        default_price=Decimal("13.37"),
    )
    question = Question.objects.create(
        event=event,
        question="T-Shirt size",
        type=Question.TYPE_CHOICE,
        required=False,
        identifier="TSHIRT",
    )
    option = QuestionOption.objects.create(question=question, identifier="SIZE_L", answer="L")
    question.items.add(item)

    position = OrderPosition.objects.create(
        order=order,
        item=item,
        price=Decimal("13.37"),
        attendee_name_cached="Ada Lovelace",
        attendee_email="ada@example.org",
    )
    QuestionAnswer.objects.create(orderposition=position, question=question, answer="L")
    position.answers.get(question=question).options.add(option)

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)

    service.sync_order(order)

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    question_columns = {column["column_name"] for column in participants_table["columns"]}
    assert "q_TSHIRT" in question_columns
    assert any(column["title"] == "T-Shirt size" for column in participants_table["columns"])

    ticket_row = client.records[participants_table["id"]][0]
    assert ticket_row["T-Shirt size"] == "L"
    assert ticket_row["answers_json"]["TSHIRT"]["option_identifiers"] == ["SIZE_L"]


def test_sync_links_participants_to_orders(event, order):
    item = Item.objects.create(
        event=event,
        name="Regular ticket",
        default_price=Decimal("10.00"),
    )
    OrderPosition.objects.create(
        order=order,
        item=item,
        price=Decimal("10.00"),
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)

    service.sync_order(order)

    orders_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_ORDERS
    )
    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    link_col = next(
        column
        for column in participants_table["columns"]
        if column.get("uidt") == "Links" and column.get("title") == ORDER_LINK_FIELD
    )
    assert link_col["colOptions"]["type"] == "mo"
    assert link_col["colOptions"]["fk_related_model_id"] == orders_table["id"]

    order_row = client.records[orders_table["id"]][0]
    ticket_row = client.records[participants_table["id"]][0]
    linked = client.list_linked_records(participants_table["id"], link_col["id"], ticket_row["Id"])
    assert len(linked) == 1
    assert linked[0]["Id"] == order_row["Id"]


def test_sync_updates_existing_question_column_title(event):
    question = Question.objects.create(
        event=event,
        question="Nickname",
        type=Question.TYPE_STRING,
        required=False,
        identifier="NICK",
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)

    service.sync_schema()
    question.question = "Display name"
    question.save(update_fields=["question"])
    service.sync_schema()

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    question_column = next(
        column for column in participants_table["columns"] if column.get("column_name") == "q_NICK"
    )
    assert question_column["title"] == "Display name"


def test_sync_truncates_long_question_titles_for_nocodb(event):
    question = Question.objects.create(
        event=event,
        question="I understand the retreat rules and safety requirements. " * 8,
        type=Question.TYPE_BOOLEAN,
        required=True,
        identifier="LONGTITLE",
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)

    service.sync_schema()

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    question_column = next(
        column
        for column in participants_table["columns"]
        if column.get("column_name") == "q_LONGTITLE"
    )

    assert len(question_column["title"]) <= MAX_COLUMN_TITLE_LENGTH
    assert question_column["title"].endswith("... (LONGTITLE)")
    assert question_column["description"] == (
        f"pretix question {question.identifier}: {question.question}"
    )


def test_sync_removes_legacy_order_code_and_matches_participants_by_link(event, order):
    item = Item.objects.create(
        event=event,
        name="Regular ticket",
        default_price=Decimal("10.00"),
    )
    position = OrderPosition.objects.create(
        order=order,
        item=item,
        price=Decimal("10.00"),
        attendee_name_cached="Existing attendee",
    )

    client = FakeNocoDBClient()
    base = client.create_base("pretix")
    orders_table = client.create_table(base["id"], title=TABLE_ORDERS, columns=ORDERS_COLUMNS)
    participants_table = client.create_table(
        base["id"],
        title=TABLE_PARTICIPANTS,
        columns=PARTICIPANTS_COLUMNS,
    )
    legacy_order_code = {
        "title": "order_code",
        "column_name": "order_code",
        "uidt": "SingleLineText",
    }
    client.create_column(participants_table["id"], legacy_order_code)
    client.create_link_column(
        participants_table["id"],
        title=ORDER_LINK_FIELD,
        child_id=orders_table["id"],
        parent_id=participants_table["id"],
    )
    order_row_id = client.create_records(
        orders_table["id"],
        [{ORDER_KEY_FIELD: str(order.code)}],
    )[0]["Id"]
    ticket_row_id = client.create_records(
        participants_table["id"],
        [
            {
                "pretix_position_id": position.pk,
                "order_code": "WRONG-CODE",
            }
        ],
    )[0]["Id"]
    link_col = next(
        column
        for column in client.tables[participants_table["id"]]["columns"]
        if column.get("uidt") == "Links" and column.get("title") == ORDER_LINK_FIELD
    )
    client.link_records(participants_table["id"], link_col["id"], ticket_row_id, order_row_id)

    service = NocoDBSyncService(event, client=client)
    service.config.base_id = base["id"]
    service.sync_order(order)

    updated_participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    assert all(
        column.get("column_name") != "order_code"
        for column in updated_participants_table["columns"]
    )

    ticket_rows = client.records[updated_participants_table["id"]]
    assert len(ticket_rows) == 1
    assert ticket_rows[0]["Order status"] == "pending"
    assert "order_code" not in ticket_rows[0]


def test_sync_upgrades_country_question_to_single_select(event, order):
    item = Item.objects.create(
        event=event,
        name="Regular ticket",
        default_price=Decimal("10.00"),
    )
    question = Question.objects.create(
        event=event,
        question="Country",
        type=Question.TYPE_COUNTRYCODE,
        required=False,
        identifier="COUNTRY",
    )
    question.items.add(item)

    position = OrderPosition.objects.create(
        order=order,
        item=item,
        price=Decimal("10.00"),
        attendee_name_cached="Ada Lovelace",
    )
    QuestionAnswer.objects.create(orderposition=position, question=question, answer="DE")

    client = FakeNocoDBClient()
    base = client.create_base("pretix")
    orders_table = client.create_table(base["id"], title=TABLE_ORDERS, columns=ORDERS_COLUMNS)
    participants_table = client.create_table(
        base["id"],
        title=TABLE_PARTICIPANTS,
        columns=PARTICIPANTS_COLUMNS,
    )
    client.create_link_column(
        participants_table["id"],
        title=ORDER_LINK_FIELD,
        child_id=orders_table["id"],
        parent_id=participants_table["id"],
    )
    client.create_column(
        participants_table["id"],
        {
            "title": "Country",
            "column_name": "q_COUNTRY",
            "uidt": "SingleLineText",
            "description": "pretix question COUNTRY: Country",
        },
    )

    service = NocoDBSyncService(event, client=client)
    service.config.base_id = base["id"]
    service.sync_order(order)

    updated_participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    country_column = next(
        column
        for column in updated_participants_table["columns"]
        if column.get("column_name") == "q_COUNTRY"
    )
    assert country_column["uidt"] == "SingleSelect"
    option_titles = [option["title"] for option in country_column["colOptions"]["options"]]
    assert "Germany" in option_titles

    ticket_row = client.records[updated_participants_table["id"]][0]
    assert ticket_row["Country"] == "Germany"


def test_sync_choice_question_becomes_single_select(event, order):
    item = Item.objects.create(
        event=event,
        name="Workshop ticket",
        default_price=Decimal("10.00"),
    )
    question = Question.objects.create(
        event=event,
        question="T-Shirt size",
        type=Question.TYPE_CHOICE,
        required=False,
        identifier="TSHIRT",
    )
    option_s = QuestionOption.objects.create(question=question, identifier="SZ_S", answer="S")
    QuestionOption.objects.create(question=question, identifier="SZ_M", answer="M")
    question.items.add(item)

    position = OrderPosition.objects.create(
        order=order,
        item=item,
        price=Decimal("10.00"),
        attendee_name_cached="Ada Lovelace",
    )
    answer = QuestionAnswer.objects.create(orderposition=position, question=question, answer="S")
    answer.options.add(option_s)

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)

    service.sync_order(order)

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    column = next(
        col for col in participants_table["columns"] if col.get("column_name") == "q_TSHIRT"
    )
    assert column["uidt"] == "SingleSelect"
    option_titles = [option["title"] for option in column["colOptions"]["options"]]
    assert option_titles == ["S", "M"]

    ticket_row = client.records[participants_table["id"]][0]
    assert ticket_row["T-Shirt size"] == "S"


def test_sync_choice_multiple_question_becomes_multi_select(event, order):
    item = Item.objects.create(
        event=event,
        name="Workshop ticket",
        default_price=Decimal("10.00"),
    )
    question = Question.objects.create(
        event=event,
        question="Preferred tracks",
        type=Question.TYPE_CHOICE_MULTIPLE,
        required=False,
        identifier="TRACKS",
    )
    option_a = QuestionOption.objects.create(question=question, identifier="TR_A", answer="Alpha")
    option_b = QuestionOption.objects.create(question=question, identifier="TR_B", answer="Beta")
    QuestionOption.objects.create(question=question, identifier="TR_C", answer="Gamma")
    question.items.add(item)

    position = OrderPosition.objects.create(
        order=order,
        item=item,
        price=Decimal("10.00"),
        attendee_name_cached="Ada Lovelace",
    )
    answer = QuestionAnswer.objects.create(
        orderposition=position, question=question, answer="Alpha, Beta"
    )
    answer.options.add(option_a, option_b)

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)

    service.sync_order(order)

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    column = next(
        col for col in participants_table["columns"] if col.get("column_name") == "q_TRACKS"
    )
    assert column["uidt"] == "MultiSelect"
    option_titles = [option["title"] for option in column["colOptions"]["options"]]
    assert option_titles == ["Alpha", "Beta", "Gamma"]

    ticket_row = client.records[participants_table["id"]][0]
    assert ticket_row["Preferred tracks"] == "Alpha,Beta"


def test_sync_handles_create_column_responses_without_column_name(event, order):
    item = Item.objects.create(
        event=event,
        name="Workshop ticket",
        default_price=Decimal("42.00"),
    )
    question = Question.objects.create(
        event=event,
        question="Company",
        type=Question.TYPE_STRING,
        required=False,
        identifier="COMPANY",
    )
    question.items.add(item)

    position = OrderPosition.objects.create(
        order=order,
        item=item,
        price=Decimal("42.00"),
        attendee_name_cached="Grace Hopper",
    )
    QuestionAnswer.objects.create(orderposition=position, question=question, answer="Acme")

    client = MissingColumnNameResponseClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)

    service.sync_order(order)

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    question_columns = {column["column_name"] for column in participants_table["columns"]}
    assert "q_COMPANY" in question_columns

    ticket_row = client.records[participants_table["id"]][0]
    assert ticket_row["Company"] == "Acme"


def test_sync_upgrades_item_and_variation_columns_to_single_select(event, order):
    item = Item.objects.create(
        event=event, name="Conference ticket", default_price=Decimal("13.37"),
    )
    variation = ItemVariation.objects.create(item=item, value="Early bird")
    ItemVariation.objects.create(item=item, value="Regular")
    OrderPosition.objects.create(
        order=order,
        item=item,
        variation=variation,
        price=Decimal("13.37"),
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)
    service.sync_order(order)

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    item_column = next(
        column
        for column in participants_table["columns"]
        if column.get("column_name") == "item_name"
    )
    variation_column = next(
        column
        for column in participants_table["columns"]
        if column.get("column_name") == "variation_name"
    )
    assert item_column["uidt"] == "SingleSelect"
    assert item_column["title"] == "Item name"
    assert [opt["title"] for opt in item_column["colOptions"]["options"]] == [
        "Conference ticket",
    ]
    assert variation_column["uidt"] == "SingleSelect"
    assert variation_column["title"] == "Variation name"
    assert [opt["title"] for opt in variation_column["colOptions"]["options"]] == [
        "Early bird",
        "Regular",
    ]

    ticket_row = client.records[participants_table["id"]][0]
    assert ticket_row["Item name"] == "Conference ticket"
    assert ticket_row["Variation name"] == "Early bird"


def test_sync_extracts_attendee_name_parts(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(
        order=order,
        item=item,
        price=Decimal("10"),
        attendee_name_cached="Ada Lovelace",
        attendee_name_parts={
            "_scheme": "given_family",
            "given_name": "Ada",
            "family_name": "Lovelace",
        },
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)
    service.sync_order(order)

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    column_names = {column["column_name"] for column in participants_table["columns"]}
    assert {"attendee_given_name", "attendee_family_name"} <= column_names

    ticket_row = client.records[participants_table["id"]][0]
    assert ticket_row["attendee_given_name"] == "Ada"
    assert ticket_row["attendee_family_name"] == "Lovelace"


def test_sync_backfills_attendee_name_part_columns_on_legacy_table(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(
        order=order,
        item=item,
        price=Decimal("10"),
        attendee_name_cached="Ada Lovelace",
        attendee_name_parts={"given_name": "Ada", "family_name": "Lovelace"},
    )

    client = FakeNocoDBClient()
    base = client.create_base("pretix")
    client.create_table(base["id"], title=TABLE_ORDERS, columns=ORDERS_COLUMNS)
    legacy_columns = [
        spec
        for spec in PARTICIPANTS_COLUMNS
        if spec["column_name"] not in {"attendee_given_name", "attendee_family_name"}
    ]
    client.create_table(base["id"], title=TABLE_PARTICIPANTS, columns=legacy_columns)

    service = NocoDBSyncService(event, client=client)
    service.config.base_id = base["id"]
    service.sync_order(order)

    updated_participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    column_names = {column["column_name"] for column in updated_participants_table["columns"]}
    assert {"attendee_given_name", "attendee_family_name"} <= column_names

    ticket_row = client.records[updated_participants_table["id"]][0]
    assert ticket_row["attendee_given_name"] == "Ada"
    assert ticket_row["attendee_family_name"] == "Lovelace"


def test_sync_upgrades_status_and_currency_to_single_select(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )

    client = FakeNocoDBClient()
    base = client.create_base("pretix")
    client.create_table(base["id"], title=TABLE_ORDERS, columns=ORDERS_COLUMNS)
    client.create_table(base["id"], title=TABLE_PARTICIPANTS, columns=PARTICIPANTS_COLUMNS)

    service = NocoDBSyncService(event, client=client)
    service.config.base_id = base["id"]
    service.sync_order(order)

    orders_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_ORDERS
    )
    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )

    status_column = next(
        column for column in orders_table["columns"] if column.get("column_name") == "status"
    )
    assert status_column["uidt"] == "SingleSelect"
    assert status_column["title"] == "Status"
    assert [opt["title"] for opt in status_column["colOptions"]["options"]] == STATUS_OPTIONS

    currency_column = next(
        column for column in orders_table["columns"] if column.get("column_name") == "currency"
    )
    assert currency_column["uidt"] == "SingleSelect"
    assert currency_column["title"] == "Currency"
    assert [opt["title"] for opt in currency_column["colOptions"]["options"]] == [
        str(event.currency),
    ]

    order_status_column = next(
        column
        for column in participants_table["columns"]
        if column.get("column_name") == "order_status"
    )
    assert order_status_column["uidt"] == "SingleSelect"
    assert order_status_column["title"] == "Order status"
    assert [opt["title"] for opt in order_status_column["colOptions"]["options"]] == (
        STATUS_OPTIONS
    )

    order_row = client.records[orders_table["id"]][0]
    assert order_row["Status"] == "pending"
    assert order_row["Currency"] == str(event.currency)
    ticket_row = client.records[participants_table["id"]][0]
    assert ticket_row["Order status"] == "pending"


def test_sync_promotes_attendee_name_as_primary_value(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )

    client = FakeNocoDBClient()
    base = client.create_base("pretix")
    client.create_table(base["id"], title=TABLE_ORDERS, columns=ORDERS_COLUMNS)
    legacy_tickets_columns = []
    for spec in PARTICIPANTS_COLUMNS:
        adjusted = dict(spec)
        if adjusted["column_name"] == PARTICIPANT_KEY_FIELD:
            adjusted["pv"] = True
        elif adjusted["column_name"] == "attendee_name":
            adjusted.pop("pv", None)
        legacy_tickets_columns.append(adjusted)
    client.create_table(base["id"], title=TABLE_PARTICIPANTS, columns=legacy_tickets_columns)

    service = NocoDBSyncService(event, client=client)
    service.config.base_id = base["id"]
    service.sync_order(order)

    updated_participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    primary_columns = [
        column for column in updated_participants_table["columns"] if column.get("pv")
    ]
    assert len(primary_columns) == 1
    assert primary_columns[0]["column_name"] == "attendee_name"


def test_sync_links_orphan_participant_rows_without_duplicating(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    position = OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )

    client = FakeNocoDBClient()
    base = client.create_base("pretix")
    orders_table = client.create_table(base["id"], title=TABLE_ORDERS, columns=ORDERS_COLUMNS)
    participants_table = client.create_table(
        base["id"],
        title=TABLE_PARTICIPANTS,
        columns=PARTICIPANTS_COLUMNS,
    )
    client.create_link_column(
        participants_table["id"],
        title=ORDER_LINK_FIELD,
        child_id=orders_table["id"],
        parent_id=participants_table["id"],
    )
    order_row_id = client.create_records(
        orders_table["id"], [{ORDER_KEY_FIELD: str(order.code)}],
    )[0]["Id"]
    # Orphan: participant exists with the right position id but no link
    orphan_id = client.create_records(
        participants_table["id"], [{PARTICIPANT_KEY_FIELD: position.pk}],
    )[0]["Id"]

    service = NocoDBSyncService(event, client=client)
    service.config.base_id = base["id"]
    service.sync_order(order)

    updated_participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    rows = [
        row
        for row in client.records[updated_participants_table["id"]]
        if row.get(PARTICIPANT_KEY_FIELD)
    ]
    assert len(rows) == 1
    assert rows[0]["Id"] == orphan_id

    link_col = next(
        column
        for column in updated_participants_table["columns"]
        if column.get("uidt") == "Links" and column.get("title") == ORDER_LINK_FIELD
    )
    linked = client.list_linked_records(
        updated_participants_table["id"], link_col["id"], orphan_id,
    )
    assert [row["Id"] for row in linked] == [order_row_id]


def test_sync_deletes_duplicate_participant_rows(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    position = OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )

    client = FakeNocoDBClient()
    base = client.create_base("pretix")
    orders_table = client.create_table(base["id"], title=TABLE_ORDERS, columns=ORDERS_COLUMNS)
    participants_table = client.create_table(
        base["id"],
        title=TABLE_PARTICIPANTS,
        columns=PARTICIPANTS_COLUMNS,
    )
    client.create_link_column(
        participants_table["id"],
        title=ORDER_LINK_FIELD,
        child_id=orders_table["id"],
        parent_id=participants_table["id"],
    )
    order_row_id = client.create_records(
        orders_table["id"], [{ORDER_KEY_FIELD: str(order.code)}],
    )[0]["Id"]
    first_id = client.create_records(
        participants_table["id"], [{PARTICIPANT_KEY_FIELD: position.pk}],
    )[0]["Id"]
    second_id = client.create_records(
        participants_table["id"], [{PARTICIPANT_KEY_FIELD: position.pk}],
    )[0]["Id"]
    link_col = next(
        column
        for column in client.tables[participants_table["id"]]["columns"]
        if column.get("uidt") == "Links" and column.get("title") == ORDER_LINK_FIELD
    )
    client.link_records(participants_table["id"], link_col["id"], second_id, order_row_id)

    service = NocoDBSyncService(event, client=client)
    service.config.base_id = base["id"]
    service.sync_order(order)

    updated_participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    rows = client.records[updated_participants_table["id"]]
    assert len(rows) == 1
    # The linked row is preferred over the orphan during dedup.
    assert rows[0]["Id"] == second_id
    assert first_id not in {row["Id"] for row in rows}


def test_delete_order_removes_order_and_participant_rows(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    first_position = OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )
    second_position = OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Grace",
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)
    service.sync_order(order)

    service.delete_order(
        str(order.code),
        position_ids=[first_position.pk, second_position.pk],
    )

    orders_table = next(table for table in client.tables.values() if table["title"] == TABLE_ORDERS)
    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    assert client.records[orders_table["id"]] == []
    assert client.records[participants_table["id"]] == []


def test_prune_deleted_rows_removes_stale_orders_and_participants(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    current_position = OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )
    stale_order = Order.objects.create(
        code="STALE1",
        event=event,
        email="stale@example.org",
        status=Order.STATUS_PENDING,
        datetime=order.datetime,
        expires=order.expires,
        total=Decimal("10.00"),
        sales_channel=order.sales_channel,
    )
    stale_position = OrderPosition.objects.create(
        order=stale_order,
        item=item,
        price=Decimal("10"),
        attendee_name_cached="Grace",
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)
    service.sync_order(order)
    service.sync_order(stale_order)

    service.prune_deleted_rows(
        active_order_codes={str(order.code)},
        active_position_ids={current_position.pk},
    )

    orders_table = next(table for table in client.tables.values() if table["title"] == TABLE_ORDERS)
    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    assert [row[ORDER_KEY_FIELD] for row in client.records[orders_table["id"]]] == [str(order.code)]
    assert [
        row[PARTICIPANT_KEY_FIELD] for row in client.records[participants_table["id"]]
    ] == [current_position.pk]
    assert stale_position.pk not in {
        row[PARTICIPANT_KEY_FIELD] for row in client.records[participants_table["id"]]
    }


def test_sync_skips_when_base_id_missing(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(order=order, item=item, price=Decimal("10"))

    client = FakeNocoDBClient()
    service = NocoDBSyncService(event, client=client)

    assert service.sync_schema() is None
    service.sync_order(order)

    assert client.bases == []
    assert client.tables == {}


def test_sync_uses_stable_tables(event):
    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)

    schema = service.sync_schema()

    assert schema is not None
    assert {table["title"] for table in client.tables.values()} == {
        TABLE_ORDERS,
        TABLE_PARTICIPANTS,
    }


def test_sync_renames_default_views_to_all(event):
    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)

    service.sync_schema()

    for table_id, views in client.views.items():
        assert views[0]["title"] == "All", (
            f"default view of table {client.tables[table_id]['title']!r} should be 'All'"
        )


def test_sync_hides_non_essential_participant_columns(event, order):
    question = Question.objects.create(
        event=event, question="Company", type=Question.TYPE_STRING, required=False, identifier="CO",
    )
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    question.items.add(item)
    OrderPosition.objects.create(
        order=order,
        item=item,
        price=Decimal("10"),
        attendee_name_cached="Ada",
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)

    service.sync_schema()

    participants_table = next(t for t in client.tables.values() if t["title"] == TABLE_PARTICIPANTS)
    view_id = client.views[participants_table["id"]][0]["id"]
    col_by_id = {c["id"]: c for c in participants_table["columns"]}

    for vc in client.view_columns[view_id]:
        col = col_by_id.get(vc["fk_column_id"])
        if col is None:
            continue
        col_name = col.get("column_name") or ""
        expected_show = col_name == "attendee_name" or col_name.startswith("q_")
        assert vc["show"] == expected_show, (
            f"column {col_name!r}: show={vc['show']}, expected {expected_show}"
        )

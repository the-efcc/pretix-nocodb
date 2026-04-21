from __future__ import annotations

import re
from decimal import Decimal

import pytest
from pretix.base.models import Item, OrderPosition, Question, QuestionAnswer, QuestionOption

from pretix_nocodb.sync import (
    ORDER_KEY_FIELD,
    ORDER_LINK_FIELD,
    ORDERS_COLUMNS,
    TABLE_ORDERS,
    TABLE_TICKETS,
    TICKETS_COLUMNS,
    NocoDBSyncService,
)

pytestmark = pytest.mark.django_db


class FakeNocoDBClient:
    def __init__(self):
        self.bases: list[dict] = []
        self.tables: dict[str, dict] = {}
        self.records: dict[str, list[dict]] = {}
        self.base_counter = 1
        self.table_counter = 1
        self.column_counter = 1
        self.record_counter = 1

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
        }
        existing = self.tables[table_id]["columns"]
        if any(c["column_name"] == created["column_name"] for c in existing):
            raise ValueError("duplicate column")
        existing.append(created)
        return created

    def create_link_column(
        self,
        table_id: str,
        *,
        title: str,
        child_id: str,
        parent_id: str,
        relation_type: str = "bt",
    ):
        related_table = self.tables[parent_id]
        parent_column = next(
            (
                column
                for column in related_table["columns"]
                if column.get("pv") or column.get("pk")
            ),
            related_table["columns"][0],
        )
        child_fk_name = self._unique_column_name(table_id, f"{related_table['title']}_id")
        child_fk_column = {
            "id": self._next_column_id(),
            "fk_model_id": child_id,
            "title": child_fk_name,
            "column_name": child_fk_name,
            "uidt": "ForeignKey",
            "description": None,
        }
        link_column = {
            "id": self._next_column_id(),
            "fk_model_id": child_id,
            "title": title,
            "column_name": None,
            "uidt": "LinkToAnotherRecord",
            "virtual": True,
            "colOptions": {
                "type": relation_type,
                "fk_related_model_id": parent_id,
                "fk_child_column_id": child_fk_column["id"],
                "fk_parent_column_id": parent_column["id"],
            },
        }
        self.tables[table_id]["columns"].append(child_fk_column)
        self.tables[table_id]["columns"].append(link_column)
        return self.tables[table_id]

    def update_column(self, column_id: str, payload: dict):
        for table in self.tables.values():
            for column in table["columns"]:
                if column["id"] == column_id:
                    column.update(payload)
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

    def list_records(
        self,
        table_id: str,
        *,
        where: str | None = None,
        fields=None,
        limit: int = 200,
    ):
        records = list(self.records[table_id])
        if where:
            match = re.match(r'@\("(?P<field>[^"]+)",eq,(?P<value>.+)\)$', where)
            assert match, where
            field = self._canonical_field(table_id, match.group("field"))
            raw_value = match.group("value")
            if raw_value.startswith('"') and raw_value.endswith('"'):
                value = raw_value[1:-1].replace('\\"', '"').replace("\\\\", "\\")
            else:
                value = int(raw_value)
            records = [record for record in records if record.get(field) == value]
        if fields:
            records = [
                {
                    field: record.get(self._canonical_field(table_id, field))
                    for field in fields
                }
                for record in records
            ]
        return records[:limit]

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


def test_sync_creates_schema_before_ticket_rows(event, order):
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
    service = NocoDBSyncService(event, client=client)

    service.sync_order(order)

    tickets_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_TICKETS
    )
    question_columns = {column["column_name"] for column in tickets_table["columns"]}
    assert "q_TSHIRT" in question_columns
    assert any(column["title"] == "T-Shirt size" for column in tickets_table["columns"])

    ticket_row = client.records[tickets_table["id"]][0]
    assert ticket_row["T-Shirt size"] == "L"
    assert ticket_row["answers_json"]["TSHIRT"]["option_identifiers"] == ["SIZE_L"]


def test_sync_links_tickets_to_orders(event, order):
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
    service = NocoDBSyncService(event, client=client)

    service.sync_order(order)

    orders_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_ORDERS
    )
    tickets_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_TICKETS
    )
    assert any(column["title"] == ORDER_LINK_FIELD for column in tickets_table["columns"])
    assert any(column["column_name"] == "orders_id" for column in tickets_table["columns"])

    order_row = client.records[orders_table["id"]][0]
    ticket_row = client.records[tickets_table["id"]][0]
    assert ticket_row["orders_id"] == order_row["Id"]


def test_sync_updates_existing_question_column_title(event):
    question = Question.objects.create(
        event=event,
        question="Nickname",
        type=Question.TYPE_STRING,
        required=False,
        identifier="NICK",
    )

    client = FakeNocoDBClient()
    service = NocoDBSyncService(event, client=client)

    service.sync_schema()
    question.question = "Display name"
    question.save(update_fields=["question"])
    service.sync_schema()

    tickets_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_TICKETS
    )
    question_column = next(
        column for column in tickets_table["columns"] if column.get("column_name") == "q_NICK"
    )
    assert question_column["title"] == "Display name"


def test_sync_removes_legacy_order_code_and_matches_tickets_by_fk(event, order):
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
    tickets_table = client.create_table(base["id"], title=TABLE_TICKETS, columns=TICKETS_COLUMNS)
    legacy_order_code = {
        "title": "order_code",
        "column_name": "order_code",
        "uidt": "SingleLineText",
    }
    client.create_column(tickets_table["id"], legacy_order_code)
    client.create_link_column(
        tickets_table["id"],
        title=ORDER_LINK_FIELD,
        child_id=tickets_table["id"],
        parent_id=orders_table["id"],
    )
    order_row_id = client.create_records(
        orders_table["id"],
        [{ORDER_KEY_FIELD: str(order.code)}],
    )[0]["Id"]
    client.create_records(
        tickets_table["id"],
        [
            {
                "pretix_position_id": position.pk,
                "orders_id": order_row_id,
                "order_code": "WRONG-CODE",
            }
        ],
    )

    service = NocoDBSyncService(event, client=client)
    service.config.base_id = base["id"]
    service.sync_order(order)

    updated_tickets_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_TICKETS
    )
    assert all(
        column.get("column_name") != "order_code" for column in updated_tickets_table["columns"]
    )

    ticket_rows = client.records[updated_tickets_table["id"]]
    assert len(ticket_rows) == 1
    assert ticket_rows[0]["orders_id"] == order_row_id
    assert ticket_rows[0]["order_status"] == "pending"
    assert "order_code" not in ticket_rows[0]


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
    service = NocoDBSyncService(event, client=client)

    service.sync_order(order)

    tickets_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_TICKETS
    )
    question_columns = {column["column_name"] for column in tickets_table["columns"]}
    assert "q_COMPANY" in question_columns

    ticket_row = client.records[tickets_table["id"]][0]
    assert ticket_row["Company"] == "Acme"


def test_sync_uses_stable_tables(event):
    client = FakeNocoDBClient()
    service = NocoDBSyncService(event, client=client)

    schema = service.sync_schema()

    assert schema is not None
    assert {table["title"] for table in client.tables.values()} == {
        TABLE_ORDERS,
        TABLE_TICKETS,
    }

from __future__ import annotations

import re
from decimal import Decimal

from pretix.base.models import Item, OrderPosition, Question, QuestionAnswer, QuestionOption

from pretix_nocodb.sync import TABLE_ORDERS, TABLE_QUESTIONS, TABLE_TICKETS, NocoDBSyncService


class FakeNocoDBClient:
    def __init__(self):
        self.bases: list[dict] = []
        self.tables: dict[str, dict] = {}
        self.records: dict[str, list[dict]] = {}
        self.base_counter = 1
        self.table_counter = 1
        self.column_counter = 1
        self.record_counter = 1

    def list_bases(self, workspace_id: str = "", *, page_size: int = 200):
        return self.bases

    def create_base(self, title: str, *, workspace_id: str = ""):
        base = {"id": f"p_{self.base_counter}", "title": title, "workspace_id": workspace_id}
        self.base_counter += 1
        self.bases.append(base)
        return base

    def list_tables(self, base_id: str, *, page_size: int = 200):
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

    def create_column(self, table_id: str, column: dict):
        created = {
            "id": f"c_{self.column_counter}",
            "fk_model_id": table_id,
            "title": column["title"],
            "column_name": column.get("column_name", column["title"]),
            "uidt": column["uidt"],
            "description": column.get("description"),
        }
        self.column_counter += 1
        existing = self.tables[table_id]["columns"]
        if any(c["column_name"] == created["column_name"] for c in existing):
            raise ValueError("duplicate column")
        existing.append(created)
        return created

    def list_records(self, table_id: str, *, where: str | None = None, fields=None, limit: int = 200):
        records = list(self.records[table_id])
        if where:
            match = re.match(r'@\("(?P<field>[^"]+)",eq,(?P<value>.+)\)$', where)
            assert match, where
            field = match.group("field")
            raw_value = match.group("value")
            if raw_value.startswith('"') and raw_value.endswith('"'):
                value = raw_value[1:-1].replace('\\"', '"').replace('\\\\', '\\')
            else:
                value = int(raw_value)
            records = [record for record in records if record.get(field) == value]
        if fields:
            records = [{field: record.get(field) for field in fields} for record in records]
        return records[:limit]

    def create_records(self, table_id: str, records: list[dict]):
        columns = {column["title"] for column in self.tables[table_id]["columns"]}
        created = []
        for record in records:
            unknown = set(record) - columns
            assert not unknown, f"unknown columns: {unknown}"
            stored = dict(record)
            stored["Id"] = self.record_counter
            self.record_counter += 1
            self.records[table_id].append(stored)
            created.append({"Id": stored["Id"]})
        return created

    def update_records(self, table_id: str, records: list[dict]):
        columns = {column["title"] for column in self.tables[table_id]["columns"]}
        updated = []
        for record in records:
            unknown = set(record) - columns - {"Id"}
            assert not unknown, f"unknown columns: {unknown}"
            target = next(row for row in self.records[table_id] if row["Id"] == record["Id"])
            target.update(record)
            updated.append({"Id": record["Id"]})
        return updated

    def delete_records(self, table_id: str, records: list[dict]):
        ids = {record["Id"] for record in records}
        self.records[table_id] = [record for record in self.records[table_id] if record["Id"] not in ids]
        return [{"Id": record_id} for record_id in ids]


def test_sync_creates_schema_before_ticket_rows(event, order):
    item = Item.objects.create(event=event, name="Conference ticket", default_price=Decimal("13.37"))
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

    tickets_table = next(table for table in client.tables.values() if table["title"] == TABLE_TICKETS)
    question_columns = {column["column_name"] for column in tickets_table["columns"]}
    assert "q_TSHIRT" in question_columns

    ticket_row = client.records[tickets_table["id"]][0]
    assert ticket_row["q_TSHIRT"] == "L"
    assert ticket_row["answers_json"]["TSHIRT"]["option_identifiers"] == ["SIZE_L"]


def test_sync_marks_removed_questions_inactive(event):
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
    question.delete()
    service.sync_schema()

    questions_table = next(table for table in client.tables.values() if table["title"] == TABLE_QUESTIONS)
    rows = client.records[questions_table["id"]]
    assert len(rows) == 1
    assert rows[0]["question_identifier"] == "NICK"
    assert rows[0]["active"] is False


def test_sync_uses_stable_tables(event):
    client = FakeNocoDBClient()
    service = NocoDBSyncService(event, client=client)

    schema = service.sync_schema()

    assert schema is not None
    assert {table["title"] for table in client.tables.values()} == {
        TABLE_ORDERS,
        TABLE_TICKETS,
        TABLE_QUESTIONS,
    }

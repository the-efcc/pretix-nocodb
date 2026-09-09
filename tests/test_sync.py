from __future__ import annotations

import re
from decimal import Decimal

import pytest
from pretix.base.models import (
    Event,
    Item,
    ItemVariation,
    Order,
    OrderPosition,
    Question,
    QuestionAnswer,
    QuestionOption,
)

from pretix_nocodb.client import NocoDBAPIError
from pretix_nocodb.sync import (
    EVENT_FIELD,
    MAX_COLUMN_TITLE_LENGTH,
    ORDER_CODE_FIELD,
    PARTICIPANT_KEY_FIELD,
    PARTICIPANTS_COLUMNS,
    STATUS_OPTIONS,
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
        self.base_counter = 1
        self.table_counter = 1
        self.column_counter = 1
        self.view_counter = 1
        self.view_column_counter = 1
        self.record_counter = 1

    def list_bases(self, _workspace_id: str = "", *, _page_size: int = 200):
        return self.bases

    def create_base(self, title: str, *, workspace_id: str = ""):
        base = {"id": f"p_{self.base_counter}", "title": title, "workspace_id": workspace_id}
        self.base_counter += 1
        self.bases.append(base)
        return base

    def duplicate_base(
        self,
        base_id: str,
        *,
        exclude_data: bool = True,
        exclude_views: bool = False,
    ):
        source = next(base for base in self.bases if base["id"] == base_id)
        new_base = self.create_base(f"{source['title']} copy")
        new_base_id = new_base["id"]
        for src_table in [t for t in self.tables.values() if t["base_id"] == base_id]:
            new_table_id = f"m_{self.table_counter}"
            self.table_counter += 1
            col_id_map: dict[str, str] = {}
            new_columns = []
            for column in src_table["columns"]:
                new_column_id = self._next_column_id()
                col_id_map[column["id"]] = new_column_id
                new_columns.append({**column, "id": new_column_id, "fk_model_id": new_table_id})
            self.tables[new_table_id] = {
                "id": new_table_id,
                "base_id": new_base_id,
                "title": src_table["title"],
                "columns": new_columns,
            }
            self.records[new_table_id] = (
                [] if exclude_data else [dict(r) for r in self.records[src_table["id"]]]
            )
            self.views[new_table_id] = []
            source_views = self.views.get(src_table["id"], [])
            if exclude_views:
                # NocoDB always keeps the default view; only extra ones are dropped.
                source_views = source_views[:1]
            for view in source_views:
                new_view_id = f"v_{self.view_counter}"
                self.view_counter += 1
                self.views[new_table_id].append({**view, "id": new_view_id})
                self.view_columns[new_view_id] = [
                    {
                        "id": f"vc_{self._next_view_column_id()}",
                        "fk_view_id": new_view_id,
                        "fk_column_id": col_id_map.get(vc["fk_column_id"], vc["fk_column_id"]),
                        "show": vc["show"],
                    }
                    for vc in self.view_columns.get(view["id"], [])
                ]
        return {"id": f"job_{new_base_id}", "base_id": new_base_id}

    def _next_view_column_id(self) -> int:
        value = self.view_column_counter
        self.view_column_counter += 1
        return value

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

    def _reject_duplicate_options(self, payload: dict):
        titles = [
            str(option.get("title"))
            for option in (payload.get("colOptions") or {}).get("options", [])
        ]
        if len(titles) != len(set(titles)):
            raise NocoDBAPIError(
                "NocoDB API error: HTTP 400 - Duplicates are not allowed!",
                status_code=400,
                payload={"msg": "Duplicates are not allowed!"},
            )

    def create_column(self, table_id: str, column: dict):
        self._reject_duplicate_options(column)
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

    def update_column(self, column_id: str, payload: dict):
        self._reject_duplicate_options(payload)
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


def test_sync_records_order_code_on_participant(event, order):
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

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    ticket_row = client.records[participants_table["id"]][0]
    assert ticket_row[ORDER_CODE_FIELD] == str(order.code)


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
    participants_table = client.create_table(
        base["id"],
        title=TABLE_PARTICIPANTS,
        columns=PARTICIPANTS_COLUMNS,
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


def test_sync_keeps_select_options_for_removed_items(event):
    early = Item.objects.create(event=event, name="Early bird", default_price=Decimal("8"))
    Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)
    service.sync_schema()

    early.delete()
    service.sync_schema()

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    item_column = next(
        column
        for column in participants_table["columns"]
        if column.get("column_name") == "item_name"
    )
    # The removed item's option must survive: dropping it would clear the
    # value from historical rows in NocoDB.
    assert [opt["title"] for opt in item_column["colOptions"]["options"]] == [
        "Early bird",
        "Regular",
    ]


def test_sync_drops_select_options_duplicated_in_nocodb(event):
    item = Item.objects.create(event=event, name="Conference ticket", default_price=Decimal("8"))
    ItemVariation.objects.create(item=item, value="Sunday & Monday nights")

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)
    service.sync_schema()

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    variation_column = next(
        column
        for column in participants_table["columns"]
        if column.get("column_name") == "variation_name"
    )
    # NocoDB handed the option back a second time under its own id; sending it
    # back as-is is what NocoDB then rejects as a duplicate.
    options = variation_column["colOptions"]["options"]
    options.append({**options[0], "id": "duplicate"})

    ItemVariation.objects.create(item=item, value="Friday & Saturday nights")
    service.sync_schema()

    assert [opt["title"] for opt in variation_column["colOptions"]["options"]] == [
        "Sunday & Monday nights",
        "Friday & Saturday nights",
    ]


def test_sync_drops_question_options_duplicated_in_nocodb(event):
    question = Question.objects.create(
        event=event,
        question="T-Shirt size",
        type=Question.TYPE_CHOICE,
        required=False,
        identifier="TSHIRT",
    )
    QuestionOption.objects.create(question=question, identifier="SZ_S", answer="S")

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
        if column.get("column_name") == "q_TSHIRT"
    )
    options = question_column["colOptions"]["options"]
    options.append({**options[0], "id": "duplicate"})

    QuestionOption.objects.create(question=question, identifier="SZ_M", answer="M")
    service.sync_schema()

    assert [opt["title"] for opt in question_column["colOptions"]["options"]] == ["S", "M"]


def test_sync_keeps_choice_options_removed_from_question(event):
    question = Question.objects.create(
        event=event,
        question="T-Shirt size",
        type=Question.TYPE_CHOICE,
        required=False,
        identifier="TSHIRT",
    )
    QuestionOption.objects.create(question=question, identifier="SZ_S", answer="S")
    removed = QuestionOption.objects.create(question=question, identifier="SZ_M", answer="M")

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)
    service.sync_schema()

    removed.delete()
    QuestionOption.objects.create(question=question, identifier="SZ_L", answer="L")
    service.sync_schema()

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    column = next(
        col for col in participants_table["columns"] if col.get("column_name") == "q_TSHIRT"
    )
    assert [opt["title"] for opt in column["colOptions"]["options"]] == ["S", "M", "L"]


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


def test_sync_upgrades_order_status_to_single_select(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )

    client = FakeNocoDBClient()
    base = client.create_base("pretix")
    client.create_table(base["id"], title=TABLE_PARTICIPANTS, columns=PARTICIPANTS_COLUMNS)

    service = NocoDBSyncService(event, client=client)
    service.config.base_id = base["id"]
    service.sync_order(order)

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )

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

    ticket_row = client.records[participants_table["id"]][0]
    assert ticket_row["Order status"] == "pending"


def test_sync_promotes_attendee_name_as_primary_value(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )

    client = FakeNocoDBClient()
    base = client.create_base("pretix")
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


def test_sync_deduplicates_existing_participant_rows(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    position = OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )

    client = FakeNocoDBClient()
    base = client.create_base("pretix")
    participants_table = client.create_table(
        base["id"],
        title=TABLE_PARTICIPANTS,
        columns=PARTICIPANTS_COLUMNS,
    )
    first_id = client.create_records(
        participants_table["id"], [{PARTICIPANT_KEY_FIELD: position.pk}],
    )[0]["Id"]
    second_id = client.create_records(
        participants_table["id"], [{PARTICIPANT_KEY_FIELD: position.pk}],
    )[0]["Id"]

    service = NocoDBSyncService(event, client=client)
    service.config.base_id = base["id"]
    service.sync_order(order)

    updated_participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    rows = client.records[updated_participants_table["id"]]
    assert len(rows) == 1
    assert rows[0]["Id"] == first_id
    assert second_id not in {row["Id"] for row in rows}


def test_sync_batches_existing_row_lookup_for_large_orders(event, order, monkeypatch):
    monkeypatch.setattr("pretix_nocodb.sync.WHERE_IN_BATCH_SIZE", 2)
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    positions = [
        OrderPosition.objects.create(
            order=order, item=item, price=Decimal("10"), attendee_name_cached=f"Attendee {i}",
        )
        for i in range(5)
    ]

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)
    service.sync_order(order)
    # The second sync must find every existing row across batches and update
    # instead of creating duplicates.
    service.sync_order(order)

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    rows = client.records[participants_table["id"]]
    assert sorted(row[PARTICIPANT_KEY_FIELD] for row in rows) == sorted(
        position.pk for position in positions
    )


def test_delete_order_removes_participant_rows(event, order):
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

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    assert client.records[participants_table["id"]] == []


def test_delete_order_removes_rows_by_order_code_fallback(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)
    service.sync_order(order)

    # No position_ids supplied; sync must still wipe rows tagged with the order code.
    service.delete_order(str(order.code))

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    assert client.records[participants_table["id"]] == []


def test_prune_deleted_rows_removes_stale_participants(event, order):
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

    service.prune_deleted_rows(active_position_ids={current_position.pk})

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    assert [
        row[PARTICIPANT_KEY_FIELD] for row in client.records[participants_table["id"]]
    ] == [current_position.pk]
    assert stale_position.pk not in {
        row[PARTICIPANT_KEY_FIELD] for row in client.records[participants_table["id"]]
    }


def test_sync_tags_rows_with_the_event(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)
    service.sync_order(order)

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    ticket_row = client.records[participants_table["id"]][0]
    assert ticket_row[EVENT_FIELD] == f"{event.organizer.slug}/{event.slug}"


def test_prune_deleted_rows_only_targets_this_events_rows(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    position = OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)
    service.sync_order(order)

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    client.create_records(
        participants_table["id"],
        [
            {PARTICIPANT_KEY_FIELD: 99991, EVENT_FIELD: "other/event"},
            {PARTICIPANT_KEY_FIELD: 99992},  # legacy row without an event tag
        ],
    )

    service.prune_deleted_rows(active_position_ids={position.pk})

    remaining = {
        row[PARTICIPANT_KEY_FIELD] for row in client.records[participants_table["id"]]
    }
    assert remaining == {position.pk, 99991, 99992}


def test_delete_order_fallback_skips_other_events_rows(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)
    service.sync_order(order)

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    # Same order code, but owned by a different event sharing the base.
    client.create_records(
        participants_table["id"],
        [
            {
                PARTICIPANT_KEY_FIELD: 55555,
                ORDER_CODE_FIELD: str(order.code),
                EVENT_FIELD: "other/event",
            }
        ],
    )

    service.delete_order(str(order.code))

    rows = client.records[participants_table["id"]]
    assert [row[PARTICIPANT_KEY_FIELD] for row in rows] == [55555]


def test_sync_order_reuses_provided_schema(event, order, monkeypatch):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(
        order=order, item=item, price=Decimal("10"), attendee_name_cached="Ada",
    )

    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)
    schema = service.sync_schema()

    def fail_on_resync():
        raise AssertionError("sync_order must not re-run the schema sync")

    monkeypatch.setattr(service, "sync_schema", fail_on_resync)
    service.sync_order(order, schema=schema)

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    assert len(client.records[participants_table["id"]]) == 1


def test_sync_creates_base_when_none_configured(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(order=order, item=item, price=Decimal("10"))

    client = FakeNocoDBClient()
    service = NocoDBSyncService(event, client=client)

    service.sync_order(order)

    assert len(client.bases) == 1
    base = client.bases[0]
    assert base["title"] == str(event.name)
    assert base["workspace_id"] == "workspace-1"
    assert event.settings.get("plugin_nocodb_base_id") == base["id"]

    participants_table = next(
        table for table in client.tables.values() if table["title"] == TABLE_PARTICIPANTS
    )
    assert len(client.records[participants_table["id"]]) == 1


def test_sync_never_adopts_a_same_titled_base(event):
    client = FakeNocoDBClient()
    # Another event's base that happens to carry the same name.
    other = client.create_base(str(event.name))

    service = NocoDBSyncService(event, client=client)
    service.sync_schema()

    # A fresh base must be created; adopting the existing one would silently
    # merge two events into one participants table.
    assert len(client.bases) == 2
    assert event.settings.get("plugin_nocodb_base_id") != other["id"]

    # Subsequent syncs reuse the persisted base instead of creating more.
    NocoDBSyncService(event, client=client).sync_schema()
    assert len(client.bases) == 2


def test_sync_picks_up_base_persisted_after_service_init(event):
    client = FakeNocoDBClient()
    service = NocoDBSyncService(event, client=client)

    # A concurrent first sync persists a base id after this service was built.
    concurrent = client.create_base(str(event.name))
    event.settings.set("plugin_nocodb_base_id", concurrent["id"])

    service.sync_schema()

    assert len(client.bases) == 1
    assert event.settings.get("plugin_nocodb_base_id") == concurrent["id"]


def test_sync_ignores_participants_table_from_a_different_base(event, order):
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(order=order, item=item, price=Decimal("10"))

    client = FakeNocoDBClient()

    # First sync into the original base persists its participants table id.
    old_base = _attach_base(event, client)
    NocoDBSyncService(event, client=client).sync_order(order)
    old_table_id = event.settings.get("plugin_nocodb_participants_table_id")
    assert client.tables[old_table_id]["base_id"] == old_base

    # The event is repointed at a new base. Table ids are global in NocoDB, so
    # the stale id still resolves via get_table; the sync must not adopt it.
    new_base = client.create_base(str(event.name))
    event.settings.set("plugin_nocodb_base_id", new_base["id"])

    NocoDBSyncService(event, client=client).sync_order(order)

    new_table_id = event.settings.get("plugin_nocodb_participants_table_id")
    assert new_table_id != old_table_id
    assert client.tables[new_table_id]["base_id"] == new_base["id"]
    # The old base's table is left untouched; rows land in the new base only.
    assert len(client.records[old_table_id]) == 1
    assert len(client.records[new_table_id]) == 1


def test_sync_duplicates_source_base_instead_of_creating(event, order):
    client = FakeNocoDBClient()

    # The first event owns a "template" base: run a sync so it exists, then give
    # its participants table a hand-added column and a data row.
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(order=order, item=item, price=Decimal("10"))
    NocoDBSyncService(event, client=client).sync_order(order)

    template_base_id = event.settings.get("plugin_nocodb_base_id")
    template_table = next(t for t in client.tables.values() if t["base_id"] == template_base_id)
    client.create_column(
        template_table["id"],
        {"title": "Extra", "column_name": "extra_custom", "uidt": "SingleLineText"},
    )
    assert len(client.records[template_table["id"]]) == 1

    # A copied event points at that base and opts to duplicate it.
    copy = Event.objects.create(
        organizer=event.organizer,
        name="Copy",
        slug="copy",
        date_from=event.date_from,
        live=True,
        plugins="pretix_nocodb",
    )
    copy.settings.set("plugin_nocodb_enabled", True)
    copy.settings.set("plugin_nocodb_api_url", "https://app.nocodb.test")
    copy.settings.set("plugin_nocodb_api_token", "test-secret")
    copy.settings.set("plugin_nocodb_source_base_id", template_base_id)
    copy.settings.set("plugin_nocodb_base_creation_mode", "duplicate")

    NocoDBSyncService(copy, client=client).sync_schema()

    new_base_id = copy.settings.get("plugin_nocodb_base_id")
    assert new_base_id
    assert new_base_id != template_base_id

    new_tables = [t for t in client.tables.values() if t["base_id"] == new_base_id]
    # The participants table is adopted from the duplicate, not created anew.
    assert len(new_tables) == 1
    new_table = new_tables[0]
    assert new_table["title"] == TABLE_PARTICIPANTS
    assert copy.settings.get("plugin_nocodb_participants_table_id") == new_table["id"]
    # The hand-added column carried over from the template...
    assert any(c["column_name"] == "extra_custom" for c in new_table["columns"])
    # ...but the template's data did not (structure-only duplication).
    assert client.records[new_table["id"]] == []


def test_sync_creates_fresh_base_when_duplicate_mode_has_no_source(event):
    # Duplicate mode selected but no source base recorded: fall back to creating.
    event.settings.set("plugin_nocodb_base_creation_mode", "duplicate")

    client = FakeNocoDBClient()
    NocoDBSyncService(event, client=client).sync_schema()

    assert len(client.bases) == 1
    assert client.bases[0]["title"] == str(event.name)


def test_sync_skips_when_disabled(event, order):
    event.settings.set("plugin_nocodb_enabled", False)
    item = Item.objects.create(event=event, name="Regular", default_price=Decimal("10"))
    OrderPosition.objects.create(order=order, item=item, price=Decimal("10"))

    client = FakeNocoDBClient()
    service = NocoDBSyncService(event, client=client)

    assert service.sync_schema() is None
    service.sync_order(order)

    assert client.bases == []
    assert client.tables == {}


def test_sync_creates_only_participants_table(event):
    client = FakeNocoDBClient()
    _attach_base(event, client)
    service = NocoDBSyncService(event, client=client)

    schema = service.sync_schema()

    assert schema is not None
    assert {table["title"] for table in client.tables.values()} == {TABLE_PARTICIPANTS}


def test_sync_renames_default_view_to_all(event):
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

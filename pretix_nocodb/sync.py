from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from django.utils.timezone import is_naive, make_naive
from django_countries import countries
from i18nfield.strings import LazyI18nString
from pretix.base.models import Order, Question, QuestionAnswer

from .client import NocoDBAPIError, NocoDBClient
from .plugin_settings import NocoDBConfig, settings_for_event

TABLE_PARTICIPANTS = "Participants"
MAX_COLUMN_TITLE_LENGTH = 255

PARTICIPANT_KEY_FIELD = "pretix_position_id"
ORDER_CODE_FIELD = "pretix_order_code"
SELECT_OPTION_COLOR = "#1f3a5f"
STATUS_OPTIONS = ["pending", "paid", "expired", "canceled"]
RECORD_PAGE_SIZE = 200


def _column(
    title: str,
    uidt: str,
    *,
    column_name: str | None = None,
    description: str | None = None,
    pv: bool = False,
    rqd: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "title": title,
        "column_name": column_name or title,
        "uidt": uidt,
    }
    if description:
        payload["description"] = description
    if pv:
        payload["pv"] = True
    if rqd:
        payload["rqd"] = True
    return payload


PARTICIPANTS_COLUMNS = [
    _column(PARTICIPANT_KEY_FIELD, "Number", rqd=True),
    _column(ORDER_CODE_FIELD, "SingleLineText"),
    _column("order_status", "SingleLineText"),
    _column("positionid", "Number"),
    _column("pretix_item_id", "Number"),
    _column("pretix_variation_id", "Number"),
    _column("item_name", "SingleLineText"),
    _column("variation_name", "SingleLineText"),
    _column("attendee_name", "SingleLineText", pv=True),
    _column("attendee_given_name", "SingleLineText"),
    _column("attendee_family_name", "SingleLineText"),
    _column("attendee_email", "Email"),
    _column("seat", "SingleLineText"),
    _column("canceled", "Checkbox"),
    _column("valid_from", "DateTime"),
    _column("valid_until", "DateTime"),
    _column("checkin_count", "Number"),
    _column("answers_json", "JSON"),
    _column("raw_json", "JSON"),
]


@dataclass(slots=True)
class TableState:
    id: str
    columns: list[dict[str, Any]]
    columns_by_id: dict[str, dict[str, Any]]
    columns_by_name: dict[str, dict[str, Any]]
    columns_by_title: dict[str, dict[str, Any]]


@dataclass(slots=True)
class SchemaState:
    participants_table_id: str
    question_columns: dict[str, str]


class NocoDBSyncService:
    def __init__(self, event, client: NocoDBClient | None = None) -> None:
        self.event = event
        self.config = NocoDBConfig.from_event(event)
        if client is not None:
            self.client = client
        elif self.config.can_sync:
            self.client = NocoDBClient(self.config.api_url, self.config.api_token)
        else:
            self.client = None

    def _get_client(self) -> NocoDBClient:
        assert self.client is not None
        return self.client

    def sync_schema(self) -> SchemaState | None:
        if not self.config.can_sync or self.client is None:
            return None

        base_id = self._ensure_base()
        participants_table_id = self._ensure_participants_table(base_id)
        participants_table = self._fetch_table_state(participants_table_id)
        participants_table = self._ensure_static_columns(participants_table, PARTICIPANTS_COLUMNS)

        questions = list(
            self.event.questions.prefetch_related("items", "options").order_by("position", "pk")
        )
        question_titles = self._question_titles(questions)
        question_columns: dict[str, str] = {}

        for question in questions:
            question_identifier = str(question.identifier)
            column_name = self._question_column_name(question.identifier)
            question_title = question_titles[question_identifier]
            column = participants_table.columns_by_name.get(column_name)
            if not column:
                column = self._create_question_column(
                    participants_table.id,
                    question,
                    title=question_title,
                )
                self._upsert_table_state_column(participants_table, column)
            elif self._question_column_needs_update(column, question_title, question):
                column = self._update_question_column(column, question, title=question_title)
                self._upsert_table_state_column(participants_table, column)
            question_columns[question_identifier] = column["title"]

        item_names, variation_names = self._collect_item_options()
        participants_table = self._ensure_select_column(
            participants_table, "item_name", "Item name", item_names,
        )
        participants_table = self._ensure_select_column(
            participants_table, "variation_name", "Variation name", variation_names,
        )
        participants_table = self._ensure_select_column(
            participants_table, "order_status", "Order status", STATUS_OPTIONS,
        )

        participants_table = self._ensure_primary_value(participants_table, "attendee_name")

        participants_view_id = self._ensure_view_title(participants_table.id, "All")
        if participants_view_id and not self._participants_view_defaults_applied(
            participants_view_id,
        ):
            self._ensure_participants_view_columns(participants_table, participants_view_id)
            self._persist_setting("participants_view_defaults_view_id", participants_view_id)

        return SchemaState(
            participants_table_id=participants_table.id,
            question_columns=question_columns,
        )

    def sync_order(self, order: Order, schema: SchemaState | None = None) -> None:
        if schema is None:
            schema = self.sync_schema()
        if schema is None:
            return

        self._upsert_participants(schema, order)

    def delete_order(self, order_code: str, *, position_ids: list[int] | None = None) -> None:
        if not self.config.can_sync or self.client is None:
            return
        if not self.config.participants_table_id:
            return

        participant_ids: set[int] = set()
        unique_positions = sorted({int(position_id) for position_id in position_ids or []})
        if unique_positions:
            for start in range(0, len(unique_positions), 100):
                batch = unique_positions[start : start + 100]
                for row in self._list_all_records(
                    self.config.participants_table_id,
                    fields=["Id", PARTICIPANT_KEY_FIELD],
                    where=self._where_in(PARTICIPANT_KEY_FIELD, batch),
                ):
                    row_id = row.get("Id")
                    if row_id is not None:
                        participant_ids.add(int(row_id))

        # Fall back to the order code so stray rows linked to the deleted order
        # are removed even if their position id wasn't supplied.
        for row in self._list_all_records(
            self.config.participants_table_id,
            fields=["Id", ORDER_CODE_FIELD],
            where=self._where_equals(ORDER_CODE_FIELD, order_code),
        ):
            row_id = row.get("Id")
            if row_id is not None:
                participant_ids.add(int(row_id))

        self._delete_record_ids(self.config.participants_table_id, list(participant_ids))

    def prune_deleted_rows(self, *, active_position_ids: set[int]) -> None:
        if not self.config.can_sync or self.client is None:
            return
        if not self.config.participants_table_id:
            return

        stale_participant_ids: list[int] = []
        for row in self._list_all_records(
            self.config.participants_table_id,
            fields=["Id", PARTICIPANT_KEY_FIELD],
        ):
            position_id = row.get(PARTICIPANT_KEY_FIELD)
            if position_id is None or int(position_id) in active_position_ids:
                continue
            row_id = row.get("Id")
            if row_id is not None:
                stale_participant_ids.append(int(row_id))

        self._delete_record_ids(self.config.participants_table_id, stale_participant_ids)

    def _ensure_base(self) -> str:
        return self.config.base_id

    def _ensure_participants_table(self, base_id: str) -> str:
        client = self._get_client()
        existing_tables = {table.get("title"): table for table in client.list_tables(base_id)}

        table_id = ""
        if self.config.participants_table_id:
            try:
                table = client.get_table(self.config.participants_table_id)
            except NocoDBAPIError:
                table = None
            else:
                table_id = table["id"]

        if not table_id and TABLE_PARTICIPANTS in existing_tables:
            table_id = existing_tables[TABLE_PARTICIPANTS]["id"]

        if not table_id:
            table = client.create_table(
                base_id, title=TABLE_PARTICIPANTS, columns=PARTICIPANTS_COLUMNS
            )
            table_id = table["id"]

        self._persist_setting("participants_table_id", table_id)
        self.config.participants_table_id = table_id
        return table_id

    def _fetch_table_state(self, table_id: str) -> TableState:
        client = self._get_client()
        table = client.get_table(table_id)
        columns = table.get("columns", [])
        columns_by_name = {
            column["column_name"]: column
            for column in columns
            if column.get("column_name")
        }
        return TableState(
            id=table["id"],
            columns=columns,
            columns_by_id={column["id"]: column for column in columns if column.get("id")},
            columns_by_name=columns_by_name,
            columns_by_title={
                column["title"]: column
                for column in columns
                if column.get("title")
            },
        )

    def _create_question_column(
        self,
        table_id: str,
        question: Question,
        *,
        title: str,
    ) -> dict[str, Any]:
        client = self._get_client()
        payload = self._question_column_payload(question, title=title)
        create_error: NocoDBAPIError | None = None
        try:
            client.create_column(table_id, payload)
        except NocoDBAPIError as exc:
            create_error = exc

        refreshed = self._fetch_table_state(table_id)
        column = refreshed.columns_by_name.get(payload["column_name"])
        if column:
            return column
        if create_error is not None:
            raise RuntimeError(
                f"Question column {payload['column_name']} was not created: "
                f"status={create_error.status_code} payload={create_error.payload!r}"
            ) from create_error
        raise RuntimeError(f"Question column {payload['column_name']} was not created")

    def _update_question_column(
        self,
        column: dict[str, Any],
        question: Question,
        *,
        title: str,
    ) -> dict[str, Any]:
        client = self._get_client()
        desired = self._question_column_payload(question, title=title)
        client.update_column(
            column["id"],
            {
                "title": desired["title"],
                "description": desired["description"],
                "uidt": desired["uidt"],
                **(
                    {"colOptions": desired["colOptions"]}
                    if desired.get("colOptions") is not None
                    else {}
                ),
            },
        )
        refreshed = self._fetch_table_state(column["fk_model_id"])
        updated = refreshed.columns_by_name.get(self._question_column_name(question.identifier))
        if updated:
            return updated
        raise RuntimeError(f"Question column {column['id']} was not updated")

    def _question_column_needs_update(
        self,
        column: dict[str, Any],
        title: str,
        question: Question,
    ) -> bool:
        desired = self._question_column_payload(question, title=title)
        return (
            column.get("title") != desired["title"]
            or column.get("description") != desired["description"]
            or column.get("uidt") != desired["uidt"]
            or self._column_option_titles(column) != self._column_option_titles(desired)
        )

    def _question_column_payload(self, question: Question, *, title: str) -> dict[str, Any]:
        payload = _column(
            title,
            self._question_uidt(question),
            column_name=self._question_column_name(question.identifier),
            description=self._question_description(question),
        )
        options = self._question_select_options(question)
        if options is not None:
            payload["colOptions"] = {"options": options}
        return payload

    def _question_select_options(self, question: Question) -> list[dict[str, str]] | None:
        question_obj = cast(Any, question)
        if question_obj.type == Question.TYPE_COUNTRYCODE:
            country_names = sorted(str(name) for _, name in countries)
            return [
                {"title": country_name, "color": SELECT_OPTION_COLOR}
                for country_name in country_names
            ]
        if question_obj.type in (Question.TYPE_CHOICE, Question.TYPE_CHOICE_MULTIPLE):
            seen: set[str] = set()
            options: list[dict[str, str]] = []
            for option in question_obj.options.all():
                title = self._option_label(option)
                if not title or title in seen:
                    continue
                seen.add(title)
                options.append({"title": title, "color": SELECT_OPTION_COLOR})
            return options
        return None

    def _option_label(self, option: Any) -> str:
        # Comma is the MultiSelect separator in NocoDB; replace to avoid splitting the option.
        return self._i18n_to_str(option.answer).strip().replace(",", " ")

    def _column_option_titles(self, column: dict[str, Any]) -> list[str]:
        return [
            str(option.get("title"))
            for option in column.get("colOptions", {}).get("options", [])
            if option.get("title")
        ]

    def _collect_item_options(self) -> tuple[list[str], list[str]]:
        items = list(
            cast(Any, self.event).items.prefetch_related("variations").order_by("position", "pk")
        )
        item_names: list[str] = []
        variation_names: list[str] = []
        seen_items: set[str] = set()
        seen_variations: set[str] = set()
        for item in items:
            name = self._i18n_to_str(item.name).strip()
            if name and name not in seen_items:
                seen_items.add(name)
                item_names.append(name)
            for variation in item.variations.all():
                vname = self._i18n_to_str(variation.value).strip()
                if vname and vname not in seen_variations:
                    seen_variations.add(vname)
                    variation_names.append(vname)
        return item_names, variation_names

    def _ensure_select_column(
        self,
        table_state: TableState,
        column_name: str,
        title: str,
        options: list[str],
    ) -> TableState:
        column = table_state.columns_by_name.get(column_name)
        if column is None:
            return table_state
        desired_options = [
            {"title": option, "color": SELECT_OPTION_COLOR} for option in options
        ]
        if (
            column.get("uidt") == "SingleSelect"
            and column.get("title") == title
            and self._column_option_titles(column) == options
        ):
            return table_state

        client = self._get_client()
        client.update_column(
            column["id"],
            {
                "title": title,
                "uidt": "SingleSelect",
                "colOptions": {"options": desired_options},
            },
        )
        return self._fetch_table_state(table_state.id)

    def _ensure_static_columns(
        self,
        table_state: TableState,
        expected_columns: list[dict[str, Any]],
    ) -> TableState:
        client = self._get_client()
        created_any = False
        for spec in expected_columns:
            column_name = spec.get("column_name") or spec.get("title")
            if column_name in table_state.columns_by_name:
                continue
            client.create_column(table_state.id, spec)
            created_any = True
        if created_any:
            return self._fetch_table_state(table_state.id)
        return table_state

    def _ensure_view_title(self, table_id: str, title: str) -> str | None:
        client = self._get_client()
        views = client.list_views(table_id)
        if not views:
            return None
        view = views[0]
        if view.get("title") != title:
            client.update_view(view["id"], {"title": title})
        return view["id"]

    def _participants_view_defaults_applied(self, view_id: str) -> bool:
        stored = settings_for_event(self.event).get(
            "participants_view_defaults_view_id", default=""
        )
        return stored == view_id

    def _ensure_participants_view_columns(
        self, participants_table: TableState, view_id: str
    ) -> None:
        client = self._get_client()
        for vc in client.list_view_columns(view_id):
            col = participants_table.columns_by_id.get(vc.get("fk_column_id", ""))
            if col is None:
                continue
            col_name = col.get("column_name") or ""
            should_show = col_name == "attendee_name" or col_name.startswith("q_")
            if vc.get("show") != should_show:
                client.update_view_column(view_id, vc["id"], {"show": should_show})

    def _ensure_primary_value(self, table_state: TableState, column_name: str) -> TableState:
        column = table_state.columns_by_name.get(column_name)
        if column is None or column.get("pv"):
            return table_state
        client = self._get_client()
        client.set_primary_column(column["id"])
        return self._fetch_table_state(table_state.id)

    def _upsert_table_state_column(self, table_state: TableState, column: dict[str, Any]) -> None:
        if column.get("id"):
            table_state.columns_by_id[column["id"]] = column
            for index, existing in enumerate(table_state.columns):
                if existing.get("id") == column["id"]:
                    table_state.columns[index] = column
                    break
            else:
                table_state.columns.append(column)
        if column.get("column_name"):
            table_state.columns_by_name[column["column_name"]] = column
        if column.get("title"):
            table_state.columns_by_title[column["title"]] = column

    def _list_all_records(
        self,
        table_id: str,
        *,
        fields: list[str] | None = None,
        where: str | None = None,
    ) -> list[dict[str, Any]]:
        client = self._get_client()
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            batch = client.list_records(
                table_id,
                where=where,
                fields=fields,
                offset=offset,
                limit=RECORD_PAGE_SIZE,
            )
            rows.extend(batch)
            if len(batch) < RECORD_PAGE_SIZE:
                return rows
            offset += len(batch)

    def _delete_record_ids(self, table_id: str, record_ids: list[int]) -> None:
        if not table_id or not record_ids:
            return

        client = self._get_client()
        unique_ids = sorted({int(record_id) for record_id in record_ids})
        for start in range(0, len(unique_ids), RECORD_PAGE_SIZE):
            batch = unique_ids[start : start + RECORD_PAGE_SIZE]
            client.delete_records(table_id, [{"Id": record_id} for record_id in batch])

    def _upsert_participants(self, schema: SchemaState, order: Order) -> None:
        client = self._get_client()
        order_obj = cast(Any, order)

        positions = list(
            order_obj.all_positions.select_related("item", "variation", "seat")
            .prefetch_related("answers__question", "answers__options", "checkins")
            .order_by("positionid", "pk")
        )
        position_pks = [position.pk for position in positions]
        position_pks_set = set(position_pks)

        existing_by_pk: dict[int, list[int]] = {}
        if position_pks:
            for row in client.list_records(
                schema.participants_table_id,
                where=self._where_in(PARTICIPANT_KEY_FIELD, position_pks),
                fields=["Id", PARTICIPANT_KEY_FIELD],
                limit=max(len(position_pks) * 4, 200),
            ):
                pk_val = row.get(PARTICIPANT_KEY_FIELD)
                if pk_val is None:
                    continue
                existing_by_pk.setdefault(int(pk_val), []).append(int(row["Id"]))

        canonical: dict[int, int] = {}
        duplicate_row_ids: list[int] = []
        for pk, row_ids in existing_by_pk.items():
            if pk not in position_pks_set:
                continue
            sorted_ids = sorted(row_ids)
            canonical[pk] = sorted_ids[0]
            duplicate_row_ids.extend(sorted_ids[1:])

        if duplicate_row_ids:
            client.delete_records(
                schema.participants_table_id,
                [{"Id": row_id} for row_id in duplicate_row_ids],
            )

        creates: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []
        for position in positions:
            payload = self._participant_payload(schema, order, position)
            existing_id = canonical.get(position.pk)
            if existing_id is not None:
                payload["Id"] = existing_id
                updates.append(payload)
            else:
                creates.append(payload)

        if creates:
            client.create_records(schema.participants_table_id, creates)

        if updates:
            client.update_records(schema.participants_table_id, updates)

    def _participant_payload(
        self,
        schema: SchemaState,
        order: Order,
        position,
    ) -> dict[str, Any]:
        order_obj = cast(Any, order)
        position_obj = cast(Any, position)
        answers_json: dict[str, Any] = {}
        question_columns = dict.fromkeys(schema.question_columns.values())

        for answer in position_obj.answers.all():
            question_identifier = str(answer.question.identifier)
            question_columns[schema.question_columns[question_identifier]] = self._answer_value(
                answer
            )
            answers_json[question_identifier] = self._answer_json(answer)

        variation_name = (
            self._i18n_to_str(position_obj.variation.value) if position_obj.variation else None
        )
        name_parts = position_obj.attendee_name_parts or {}

        return {
            PARTICIPANT_KEY_FIELD: position_obj.pk,
            ORDER_CODE_FIELD: str(order_obj.code),
            "order_status": self._status_label(order_obj.status),
            "positionid": position_obj.positionid,
            "pretix_item_id": position_obj.item_id,
            "pretix_variation_id": position_obj.variation_id,
            "item_name": self._i18n_to_str(position_obj.item.name),
            "variation_name": variation_name,
            "attendee_name": position_obj.attendee_name_cached,
            "attendee_given_name": name_parts.get("given_name") or None,
            "attendee_family_name": name_parts.get("family_name") or None,
            "attendee_email": position_obj.attendee_email,
            "seat": str(position_obj.seat) if position_obj.seat else None,
            "canceled": position_obj.canceled,
            "valid_from": self._serialize_datetime(position_obj.valid_from),
            "valid_until": self._serialize_datetime(position_obj.valid_until),
            "checkin_count": position_obj.checkins.count(),
            "answers_json": answers_json,
            "raw_json": {
                "position_pk": position_obj.pk,
                "positionid": position_obj.positionid,
                "order_code": str(order_obj.code),
                "item_id": position_obj.item_id,
                "variation_id": position_obj.variation_id,
                "item_name": self._i18n_to_str(position_obj.item.name),
                "variation_name": variation_name,
                "attendee_name": position_obj.attendee_name_cached,
                "attendee_email": position_obj.attendee_email,
                "canceled": position_obj.canceled,
                "valid_from": self._serialize_datetime(position_obj.valid_from),
                "valid_until": self._serialize_datetime(position_obj.valid_until),
                "answers": answers_json,
            },
            **question_columns,
        }

    def _answer_value(self, answer: QuestionAnswer) -> Any:
        answer_obj = cast(Any, answer)
        question_obj = cast(Any, answer.question)
        question_type = question_obj.type
        if question_type == Question.TYPE_BOOLEAN:
            return answer_obj.answer == "True"
        if question_type == Question.TYPE_NUMBER:
            return self._serialize_decimal(answer_obj.answer)
        if question_type == Question.TYPE_FILE:
            return answer_obj.file_name if answer_obj.file else answer_obj.answer
        if question_type in (Question.TYPE_DATE, Question.TYPE_TIME, Question.TYPE_DATETIME):
            return answer_obj.answer or None
        if question_type in (Question.TYPE_CHOICE, Question.TYPE_CHOICE_MULTIPLE):
            labels = [self._option_label(option) for option in answer_obj.options.all()]
            labels = [label for label in labels if label]
            if not labels:
                return None
            if question_type == Question.TYPE_CHOICE:
                return labels[0]
            return ",".join(labels)
        return answer_obj.to_string(use_cached=True) or None

    def _answer_json(self, answer: QuestionAnswer) -> dict[str, Any]:
        answer_obj = cast(Any, answer)
        question_obj = cast(Any, answer.question)
        payload = {
            "question_id": question_obj.pk,
            "question_identifier": str(question_obj.identifier),
            "question_type": str(question_obj.type),
            "answer": answer_obj.answer,
            "display_answer": answer_obj.to_string(use_cached=True),
            "option_identifiers": [option.identifier for option in answer_obj.options.all()],
        }
        if answer_obj.file:
            payload["file_name"] = answer_obj.file_name
        return payload

    def _question_uidt(self, question: Question) -> str:
        question_obj = cast(Any, question)
        if question_obj.type == Question.TYPE_BOOLEAN:
            return "Checkbox"
        if question_obj.type == Question.TYPE_NUMBER:
            return "Decimal"
        if question_obj.type == Question.TYPE_TEXT:
            return "LongText"
        if question_obj.type == Question.TYPE_DATE:
            return "Date"
        if question_obj.type == Question.TYPE_TIME:
            return "Time"
        if question_obj.type == Question.TYPE_DATETIME:
            return "DateTime"
        if question_obj.type == Question.TYPE_COUNTRYCODE:
            return "SingleSelect"
        if question_obj.type == Question.TYPE_CHOICE:
            return "SingleSelect"
        if question_obj.type == Question.TYPE_CHOICE_MULTIPLE:
            return "MultiSelect"
        if question_obj.type == Question.TYPE_PHONENUMBER:
            return "PhoneNumber"
        return "SingleLineText"

    def _question_description(self, question: Question) -> str:
        question_obj = cast(Any, question)
        label = self._i18n_to_str(question_obj.question)
        identifier = str(question_obj.identifier)
        return f"pretix question {identifier}: {label}" if label else identifier

    def _question_titles(self, questions: list[Question]) -> dict[str, str]:
        raw_titles: dict[str, str] = {
            str(question.identifier): self._question_title(question) for question in questions
        }
        title_counts: dict[str, int] = {}
        for title in raw_titles.values():
            title_counts[title] = title_counts.get(title, 0) + 1

        resolved: dict[str, str] = {}
        for question in questions:
            identifier = str(question.identifier)
            base_title = raw_titles[identifier]
            title = (
                f"{base_title} ({identifier})"
                if title_counts[base_title] > 1
                else base_title
            )
            resolved[identifier] = self._bounded_question_title(
                title,
                identifier,
            )
        return resolved

    def _question_title(self, question: Question) -> str:
        question_obj = cast(Any, question)
        return self._i18n_to_str(question_obj.question).strip() or str(question_obj.identifier)

    def _bounded_question_title(self, title: str, identifier: str) -> str:
        if len(title) <= MAX_COLUMN_TITLE_LENGTH:
            return title

        duplicate_suffix = f" ({identifier})"
        if title.endswith(duplicate_suffix):
            title = title[: -len(duplicate_suffix)]

        suffix = f"... ({identifier})"
        prefix_length = max(MAX_COLUMN_TITLE_LENGTH - len(suffix), 1)
        return f"{title[:prefix_length].rstrip()}{suffix}"

    def _question_column_name(self, identifier: Any) -> str:
        return f"q_{identifier}"

    def _persist_setting(self, key: str, value: str) -> None:
        settings_for_event(self.event).set(key, value)

    def _where_equals(self, field: str, value: Any) -> str:
        if isinstance(value, int):
            encoded = str(value)
        else:
            escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
            encoded = f'"{escaped}"'
        return f'@("{field}",eq,{encoded})'

    def _where_in(self, field: str, values: list[int]) -> str:
        encoded = ",".join(str(int(value)) for value in values)
        return f'@("{field}",in,{encoded})'

    def _status_label(self, status: Any) -> str:
        return {
            Order.STATUS_PENDING: "pending",
            Order.STATUS_PAID: "paid",
            Order.STATUS_EXPIRED: "expired",
            Order.STATUS_CANCELED: "canceled",
        }.get(status, str(status))

    def _serialize_datetime(self, value) -> str | None:
        if value is None:
            return None
        if is_naive(value):
            return value.isoformat()
        return make_naive(value).isoformat()

    def _serialize_decimal(self, value: Any) -> float | None:
        if value in (None, ""):
            return None
        if isinstance(value, Decimal):
            return float(value)
        try:
            return float(Decimal(str(value)))
        except (InvalidOperation, ValueError):
            return None

    def _i18n_to_str(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, LazyI18nString):
            data = value.data
            if data is None:
                return ""
            if isinstance(data, str):
                return data
            preferred_locale = self.event.settings.locale
            if preferred_locale and data.get(preferred_locale):
                return str(data[preferred_locale])
            for candidate in data.values():
                if candidate:
                    return str(candidate)
            return ""
        return str(value)

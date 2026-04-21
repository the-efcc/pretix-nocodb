from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from django.db.models import Max
from django_countries import countries
from django.utils.timezone import is_naive, make_naive
from i18nfield.strings import LazyI18nString
from pretix.base.models import Order, OrderPayment, Question, QuestionAnswer

from .client import NocoDBAPIError, NocoDBClient
from .plugin_settings import NocoDBConfig, settings_for_event

TABLE_ORDERS = "orders"
TABLE_TICKETS = "tickets"

ORDER_KEY_FIELD = "pretix_order_code"
TICKET_KEY_FIELD = "pretix_position_id"
ORDER_LINK_FIELD = "Order"
SELECT_OPTION_COLOR = "#1f3a5f"


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


ORDERS_COLUMNS = [
    _column(ORDER_KEY_FIELD, "SingleLineText", pv=True, rqd=True),
    _column("status", "SingleLineText"),
    _column("email", "Email"),
    _column("phone", "PhoneNumber"),
    _column("locale", "SingleLineText"),
    _column("sales_channel", "SingleLineText"),
    _column("datetime", "DateTime"),
    _column("expires", "DateTime"),
    _column("payment_date", "DateTime"),
    _column("cancellation_date", "DateTime"),
    _column("total", "Decimal"),
    _column("currency", "SingleLineText"),
    _column("testmode", "Checkbox"),
    _column("valid_if_pending", "Checkbox"),
    _column("require_approval", "Checkbox"),
    _column("raw_json", "JSON"),
]

TICKETS_COLUMNS = [
    _column(TICKET_KEY_FIELD, "Number", pv=True, rqd=True),
    _column("order_status", "SingleLineText"),
    _column("positionid", "Number"),
    _column("pretix_item_id", "Number"),
    _column("pretix_variation_id", "Number"),
    _column("item_name", "SingleLineText"),
    _column("variation_name", "SingleLineText"),
    _column("attendee_name", "SingleLineText"),
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
    orders_table_id: str
    tickets_table_id: str
    order_link_column_id: str
    order_fk_column_name: str
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
        table_ids = self._ensure_tables(base_id)

        orders_table = self._fetch_table_state(table_ids[TABLE_ORDERS])
        tickets_table = self._fetch_table_state(table_ids[TABLE_TICKETS])
        order_link_column_id, order_fk_column_name, tickets_table = self._ensure_order_link_column(
            orders_table,
            tickets_table,
        )
        tickets_table = self._delete_ticket_column(tickets_table, "order_code")

        questions = list(
            self.event.questions.prefetch_related("items", "options").order_by("position", "pk")
        )
        question_titles = self._question_titles(questions)
        question_columns: dict[str, str] = {}

        for question in questions:
            column_name = self._question_column_name(question.identifier)
            question_title = question_titles[question.identifier]
            column = tickets_table.columns_by_name.get(column_name)
            if not column:
                column = self._create_question_column(
                    tickets_table.id,
                    question,
                    title=question_title,
                )
                self._upsert_table_state_column(tickets_table, column)
            elif self._question_column_needs_update(column, question_title, question):
                column = self._update_question_column(column, question, title=question_title)
                self._upsert_table_state_column(tickets_table, column)
            question_columns[question.identifier] = column["title"]

        return SchemaState(
            orders_table_id=orders_table.id,
            tickets_table_id=tickets_table.id,
            order_link_column_id=order_link_column_id,
            order_fk_column_name=order_fk_column_name,
            question_columns=question_columns,
        )

    def sync_order(self, order: Order) -> None:
        schema = self.sync_schema()
        if schema is None:
            return

        order_row_id = self._upsert_order(schema.orders_table_id, order)
        self._upsert_tickets(schema, order, order_row_id)

    def _ensure_base(self) -> str:
        client = self._get_client()
        if self.config.base_id:
            return self.config.base_id

        title = self.config.base_title or self._default_base_title()
        for base in client.list_bases(self.config.workspace_id):
            if base.get("title") == title:
                self._persist_setting("base_id", base["id"])
                self.config.base_id = base["id"]
                return base["id"]

        created = client.create_base(title, workspace_id=self.config.workspace_id)
        self._persist_setting("base_id", created["id"])
        self.config.base_id = created["id"]
        return created["id"]

    def _ensure_tables(self, base_id: str) -> dict[str, str]:
        client = self._get_client()
        existing_tables = {table.get("title"): table for table in client.list_tables(base_id)}
        settings_map = {
            TABLE_ORDERS: (self.config.orders_table_id, ORDERS_COLUMNS, "orders_table_id"),
            TABLE_TICKETS: (self.config.tickets_table_id, TICKETS_COLUMNS, "tickets_table_id"),
        }
        table_ids: dict[str, str] = {}

        for title, (configured_id, columns, setting_key) in settings_map.items():
            table_id = ""
            if configured_id:
                try:
                    table = client.get_table(configured_id)
                except NocoDBAPIError:
                    table = None
                else:
                    table_id = table["id"]

            if not table_id and title in existing_tables:
                table_id = existing_tables[title]["id"]

            if not table_id:
                table = client.create_table(base_id, title=title, columns=columns)
                table_id = table["id"]

            self._persist_setting(setting_key, table_id)
            setattr(self.config, setting_key, table_id)
            table_ids[title] = table_id

        return table_ids

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
        with suppress(NocoDBAPIError):
            client.create_column(table_id, payload)

        refreshed = self._fetch_table_state(table_id)
        column = refreshed.columns_by_name.get(payload["column_name"])
        if column:
            return column
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
        if question_obj.type != Question.TYPE_COUNTRYCODE:
            return None

        country_names = sorted(str(name) for _, name in countries)
        return [
            {"title": country_name, "color": SELECT_OPTION_COLOR}
            for country_name in country_names
        ]

    def _column_option_titles(self, column: dict[str, Any]) -> list[str]:
        return [
            str(option.get("title"))
            for option in column.get("colOptions", {}).get("options", [])
            if option.get("title")
        ]

    def _ensure_order_link_column(
        self,
        orders_table: TableState,
        tickets_table: TableState,
    ) -> tuple[str, str, TableState]:
        column = next(
            (
                candidate
                for candidate in tickets_table.columns
                if candidate.get("uidt") == "LinkToAnotherRecord"
                and candidate.get("colOptions", {}).get("type") == "bt"
                and candidate.get("colOptions", {}).get("fk_related_model_id") == orders_table.id
                and candidate.get("title") == ORDER_LINK_FIELD
            ),
            None,
        )
        if column is None:
            client = self._get_client()
            client.create_link_column(
                tickets_table.id,
                title=ORDER_LINK_FIELD,
                child_id=tickets_table.id,
                parent_id=orders_table.id,
            )
            tickets_table = self._fetch_table_state(tickets_table.id)
            column = next(
                candidate
                for candidate in tickets_table.columns
                if candidate.get("uidt") == "LinkToAnotherRecord"
                and candidate.get("colOptions", {}).get("type") == "bt"
                and candidate.get("colOptions", {}).get("fk_related_model_id") == orders_table.id
                and candidate.get("title") == ORDER_LINK_FIELD
            )

        fk_column_id = column["colOptions"]["fk_child_column_id"]
        fk_column = tickets_table.columns_by_id[fk_column_id]
        return column["id"], fk_column["column_name"], tickets_table

    def _delete_ticket_column(self, tickets_table: TableState, column_name: str) -> TableState:
        column = tickets_table.columns_by_name.get(column_name)
        if column is None:
            return tickets_table

        client = self._get_client()
        client.delete_column(column["id"])
        return self._fetch_table_state(tickets_table.id)

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

    def _upsert_order(self, table_id: str, order: Order) -> int:
        client = self._get_client()
        order_obj = cast(Any, order)
        existing = client.list_records(
            table_id,
            where=self._where_equals(ORDER_KEY_FIELD, order_obj.code),
            fields=["Id", ORDER_KEY_FIELD],
            limit=1,
        )
        payload = self._order_payload(order)
        if existing:
            payload["Id"] = existing[0]["Id"]
            client.update_records(table_id, [payload])
            return int(existing[0]["Id"])

        created = client.create_records(table_id, [payload])
        return int(created[0]["Id"])

    def _upsert_tickets(self, schema: SchemaState, order: Order, order_row_id: int) -> None:
        client = self._get_client()
        order_obj = cast(Any, order)
        existing_rows = client.list_records(
            schema.tickets_table_id,
            where=self._where_equals(schema.order_fk_column_name, order_row_id),
            fields=["Id", TICKET_KEY_FIELD],
            limit=1000,
        )
        existing_by_position_id = {
            int(row[TICKET_KEY_FIELD]): row["Id"]
            for row in existing_rows
            if row.get(TICKET_KEY_FIELD) is not None
        }
        seen_position_ids: set[int] = set()
        creates: list[dict[str, Any]] = []
        updates: list[dict[str, Any]] = []

        for position in (
            order_obj.all_positions.select_related("item", "variation", "seat")
            .prefetch_related("answers__question", "answers__options", "checkins")
            .order_by("positionid", "pk")
        ):
            payload = self._ticket_payload(schema, order, position, order_row_id)
            seen_position_ids.add(position.pk)
            existing_id = existing_by_position_id.get(position.pk)
            if existing_id is not None:
                payload["Id"] = existing_id
                updates.append(payload)
            else:
                creates.append(payload)

        stale_ids = [
            {"Id": row_id}
            for position_id, row_id in existing_by_position_id.items()
            if position_id not in seen_position_ids
        ]

        if creates:
            client.create_records(schema.tickets_table_id, creates)
        if updates:
            client.update_records(schema.tickets_table_id, updates)
        if stale_ids:
            client.delete_records(schema.tickets_table_id, stale_ids)

    def _order_payload(self, order: Order) -> dict[str, Any]:
        order_obj = cast(Any, order)
        payment_date = (
            order_obj.payments.filter(state=OrderPayment.PAYMENT_STATE_CONFIRMED)
            .aggregate(latest=Max("payment_date"))
            .get("latest")
        )
        return {
            ORDER_KEY_FIELD: str(order_obj.code),
            "status": self._status_label(order_obj.status),
            "email": order_obj.email,
            "phone": order_obj.phone,
            "locale": order_obj.locale,
            "sales_channel": order_obj.sales_channel.identifier,
            "datetime": self._serialize_datetime(order_obj.datetime),
            "expires": self._serialize_datetime(order_obj.expires),
            "payment_date": self._serialize_datetime(payment_date),
            "cancellation_date": self._serialize_datetime(order_obj.cancellation_date),
            "total": self._serialize_decimal(order_obj.total),
            "currency": str(order_obj.event.currency),
            "testmode": order_obj.testmode,
            "valid_if_pending": order_obj.valid_if_pending,
            "require_approval": order_obj.require_approval,
            "raw_json": {
                "code": str(order_obj.code),
                "status": str(order_obj.status),
                "email": order_obj.email,
                "phone": order_obj.phone,
                "locale": order_obj.locale,
                "sales_channel": order_obj.sales_channel.identifier,
                "datetime": self._serialize_datetime(order_obj.datetime),
                "expires": self._serialize_datetime(order_obj.expires),
                "payment_date": self._serialize_datetime(payment_date),
                "cancellation_date": self._serialize_datetime(order_obj.cancellation_date),
                "total": str(order_obj.total),
                "currency": str(order_obj.event.currency),
                "testmode": order_obj.testmode,
                "valid_if_pending": order_obj.valid_if_pending,
                "require_approval": order_obj.require_approval,
            },
        }

    def _ticket_payload(
        self,
        schema: SchemaState,
        order: Order,
        position,
        order_row_id: int,
    ) -> dict[str, Any]:
        order_obj = cast(Any, order)
        position_obj = cast(Any, position)
        answers_json: dict[str, Any] = {}
        question_columns = dict.fromkeys(schema.question_columns.values())

        for answer in position_obj.answers.all():
            question_identifier = answer.question.identifier
            question_columns[schema.question_columns[question_identifier]] = self._answer_value(
                answer
            )
            answers_json[question_identifier] = self._answer_json(answer)

        variation_name = (
            self._i18n_to_str(position_obj.variation.value) if position_obj.variation else None
        )

        return {
            TICKET_KEY_FIELD: position_obj.pk,
            schema.order_fk_column_name: order_row_id,
            "order_status": self._status_label(order_obj.status),
            "positionid": position_obj.positionid,
            "pretix_item_id": position_obj.item_id,
            "pretix_variation_id": position_obj.variation_id,
            "item_name": self._i18n_to_str(position_obj.item.name),
            "variation_name": variation_name,
            "attendee_name": position_obj.attendee_name_cached,
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
            question.identifier: self._question_title(question) for question in questions
        }
        title_counts: dict[str, int] = {}
        for title in raw_titles.values():
            title_counts[title] = title_counts.get(title, 0) + 1

        resolved: dict[str, str] = {}
        for question in questions:
            base_title = raw_titles[question.identifier]
            if title_counts[base_title] > 1:
                resolved[question.identifier] = f"{base_title} ({question.identifier})"
            else:
                resolved[question.identifier] = base_title
        return resolved

    def _question_title(self, question: Question) -> str:
        question_obj = cast(Any, question)
        return self._i18n_to_str(question_obj.question).strip() or str(question_obj.identifier)

    def _question_column_name(self, identifier: Any) -> str:
        return f"q_{identifier}"

    def _default_base_title(self) -> str:
        return f"pretix_{self.event.organizer.slug}_{self.event.slug}"

    def _persist_setting(self, key: str, value: str) -> None:
        settings_for_event(self.event).set(key, value)

    def _where_equals(self, field: str, value: Any) -> str:
        if isinstance(value, int):
            encoded = str(value)
        else:
            escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
            encoded = f'"{escaped}"'
        return f'@("{field}",eq,{encoded})'

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

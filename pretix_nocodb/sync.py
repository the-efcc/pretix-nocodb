from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from django.db.models import Max
from django.utils.timezone import is_naive, make_naive
from django_countries import countries
from i18nfield.strings import LazyI18nString
from pretix.base.models import Order, OrderPayment, Question, QuestionAnswer

from .client import NocoDBAPIError, NocoDBClient
from .plugin_settings import NocoDBConfig, settings_for_event

TABLE_ORDERS = "orders"
TABLE_TICKETS = "tickets"
MAX_COLUMN_TITLE_LENGTH = 255

ORDER_KEY_FIELD = "pretix_order_code"
TICKET_KEY_FIELD = "pretix_position_id"
ORDER_LINK_FIELD = "Order"
SELECT_OPTION_COLOR = "#1f3a5f"
STATUS_OPTIONS = ["pending", "paid", "expired", "canceled"]


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
    _column(TICKET_KEY_FIELD, "Number", rqd=True),
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
    orders_table_id: str
    tickets_table_id: str
    order_link_column_id: str
    order_reciprocal_link_column_id: str
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
        (
            order_link_column_id,
            order_reciprocal_link_column_id,
            orders_table,
            tickets_table,
        ) = self._ensure_order_link_column(orders_table, tickets_table)
        tickets_table = self._delete_ticket_column(tickets_table, "order_code")
        tickets_table = self._ensure_static_columns(tickets_table, TICKETS_COLUMNS)

        questions = list(
            self.event.questions.prefetch_related("items", "options").order_by("position", "pk")
        )
        question_titles = self._question_titles(questions)
        question_columns: dict[str, str] = {}

        for question in questions:
            question_identifier = str(question.identifier)
            column_name = self._question_column_name(question.identifier)
            question_title = question_titles[question_identifier]
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
            question_columns[question_identifier] = column["title"]

        item_names, variation_names = self._collect_item_options()
        tickets_table = self._ensure_select_column(
            tickets_table, "item_name", "Item name", item_names,
        )
        tickets_table = self._ensure_select_column(
            tickets_table, "variation_name", "Variation name", variation_names,
        )
        tickets_table = self._ensure_select_column(
            tickets_table, "order_status", "Order status", STATUS_OPTIONS,
        )

        orders_table = self._ensure_select_column(
            orders_table, "status", "Status", STATUS_OPTIONS,
        )
        orders_table = self._ensure_select_column(
            orders_table, "currency", "Currency", [str(self.event.currency)],
        )

        tickets_table = self._ensure_primary_value(tickets_table, "attendee_name")

        return SchemaState(
            orders_table_id=orders_table.id,
            tickets_table_id=tickets_table.id,
            order_link_column_id=order_link_column_id,
            order_reciprocal_link_column_id=order_reciprocal_link_column_id,
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

    def _ensure_order_link_column(
        self,
        orders_table: TableState,
        tickets_table: TableState,
    ) -> tuple[str, str, TableState, TableState]:
        def find_v2_link(table: TableState) -> dict[str, Any] | None:
            return next(
                (
                    candidate
                    for candidate in table.columns
                    if candidate.get("uidt") == "Links"
                    and candidate.get("colOptions", {}).get("type") == "mo"
                    and candidate.get("colOptions", {}).get("fk_related_model_id")
                    == orders_table.id
                    and candidate.get("title") == ORDER_LINK_FIELD
                ),
                None,
            )

        legacy = next(
            (
                candidate
                for candidate in tickets_table.columns
                if candidate.get("uidt") == "LinkToAnotherRecord"
                and candidate.get("colOptions", {}).get("fk_related_model_id") == orders_table.id
                and candidate.get("title") == ORDER_LINK_FIELD
            ),
            None,
        )
        if legacy is not None:
            client = self._get_client()
            client.delete_column(legacy["id"])
            tickets_table = self._fetch_table_state(tickets_table.id)
            orders_table = self._fetch_table_state(orders_table.id)

        link_column = find_v2_link(tickets_table)
        if link_column is None:
            client = self._get_client()
            client.create_link_column(
                tickets_table.id,
                title=ORDER_LINK_FIELD,
                child_id=orders_table.id,
                parent_id=tickets_table.id,
            )
            tickets_table = self._fetch_table_state(tickets_table.id)
            orders_table = self._fetch_table_state(orders_table.id)
            link_column = find_v2_link(tickets_table)
            if link_column is None:
                raise RuntimeError(f"Order link column {ORDER_LINK_FIELD} was not created")

        link_col_options = link_column["colOptions"]
        reciprocal = next(
            (
                candidate
                for candidate in orders_table.columns
                if candidate.get("uidt") == "Links"
                and candidate.get("colOptions", {}).get("type") == "om"
                and candidate.get("colOptions", {}).get("fk_related_model_id")
                == tickets_table.id
                and candidate.get("colOptions", {}).get("fk_mm_model_id")
                == link_col_options.get("fk_mm_model_id")
            ),
            None,
        )
        if reciprocal is None:
            raise RuntimeError(
                f"Reciprocal link column for {ORDER_LINK_FIELD} not found on orders table"
            )

        return link_column["id"], reciprocal["id"], orders_table, tickets_table

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

    def _ensure_primary_value(self, table_state: TableState, column_name: str) -> TableState:
        column = table_state.columns_by_name.get(column_name)
        if column is None or column.get("pv"):
            return table_state
        client = self._get_client()
        client.set_primary_column(column["id"])
        return self._fetch_table_state(table_state.id)

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

        positions = list(
            order_obj.all_positions.select_related("item", "variation", "seat")
            .prefetch_related("answers__question", "answers__options", "checkins")
            .order_by("positionid", "pk")
        )
        position_pks = [position.pk for position in positions]
        position_pks_set = set(position_pks)

        linked_rows = client.list_linked_records(
            schema.orders_table_id,
            schema.order_reciprocal_link_column_id,
            order_row_id,
            fields=["Id", TICKET_KEY_FIELD],
            limit=1000,
        )
        linked_ids: set[int] = {int(row["Id"]) for row in linked_rows}

        row_position_pk: dict[int, int | None] = {}
        for row in linked_rows:
            pk_val = row.get(TICKET_KEY_FIELD)
            row_position_pk[int(row["Id"])] = int(pk_val) if pk_val is not None else None

        # Also pull any tickets keyed by the current positions; that catches
        # orphan rows whose order link was lost (e.g. legacy column migration)
        # plus duplicate rows that a previous broken sync created.
        if position_pks:
            for row in client.list_records(
                schema.tickets_table_id,
                where=self._where_in(TICKET_KEY_FIELD, position_pks),
                fields=["Id", TICKET_KEY_FIELD],
                limit=max(len(position_pks) * 4, 200),
            ):
                pk_val = row.get(TICKET_KEY_FIELD)
                if pk_val is None:
                    continue
                row_position_pk.setdefault(int(row["Id"]), int(pk_val))

        by_position: dict[int, list[int]] = {}
        stale_ids: list[int] = []
        for row_id, pk_val in row_position_pk.items():
            if pk_val is None:
                continue
            if pk_val in position_pks_set:
                by_position.setdefault(pk_val, []).append(row_id)
            else:
                stale_ids.append(row_id)

        canonical: dict[int, int] = {}
        duplicate_row_ids: list[int] = []
        for pk, row_ids in by_position.items():
            sorted_ids = sorted(row_ids, key=lambda rid: (rid not in linked_ids, rid))
            canonical[pk] = sorted_ids[0]
            duplicate_row_ids.extend(sorted_ids[1:])

        if duplicate_row_ids:
            client.delete_records(
                schema.tickets_table_id,
                [{"Id": row_id} for row_id in duplicate_row_ids],
            )

        creates: list[tuple[int, dict[str, Any]]] = []
        updates: list[dict[str, Any]] = []
        for position in positions:
            payload = self._ticket_payload(schema, order, position)
            existing_id = canonical.get(position.pk)
            if existing_id is not None:
                payload["Id"] = existing_id
                updates.append(payload)
            else:
                creates.append((position.pk, payload))

        if creates:
            created = client.create_records(
                schema.tickets_table_id, [payload for _, payload in creates]
            )
            for (_, _), created_row in zip(creates, created, strict=True):
                client.link_records(
                    schema.tickets_table_id,
                    schema.order_link_column_id,
                    int(created_row["Id"]),
                    order_row_id,
                )

        if updates:
            client.update_records(schema.tickets_table_id, updates)

        for row_id in canonical.values():
            if row_id not in linked_ids:
                client.link_records(
                    schema.tickets_table_id,
                    schema.order_link_column_id,
                    row_id,
                    order_row_id,
                )

        if stale_ids:
            client.delete_records(
                schema.tickets_table_id, [{"Id": row_id} for row_id in stale_ids]
            )

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
            TICKET_KEY_FIELD: position_obj.pk,
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

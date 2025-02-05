# -*- coding: utf-8 -*-
from datetime import datetime, timedelta, date
import logging
from time import sleep
from typing import List

from settings import Settings

from progress.bar import Bar as Bar

from .exceptions import TooLargeBoarders
from .methods import OrdersMethods
from .request import RequestApi

MAX_COUNT_CUSTOMERS_PER_REQUEST = 500

logger = logging.getLogger("root")


class Order:
    def __init__(self, order: dict):
        self.order = order
        self.id: int = order.get('id')
        self.status_name = order.get("orderStatus", {}).get("name", None)
        self.status_id = order.get("orderStatus", {}).get("id", None)

        self.customer = order.get("customer", {})
        self.customer_id = order.get("customer", {}).get("id", None)
        self.customer_vk = self.customer.get("vk", {}) if self.customer else None
        self.customer_vk_id = self.customer_vk.get("id", {}) if self.customer_vk else None
        self.customer_vk_messages_group_id = self.customer_vk.get("messagesGroupId", {}) if self.customer_vk else None

        self.tracking_number = None
        for custom_field in order.get('customFields', []):
            if custom_field.get('fieldId', None) == 5882:  # айди кастомного поля - айди для сдека
                self.tracking_number = custom_field.get("value", None)
                break

class OrdersAPI:
    def __init__(self, request_api: RequestApi):
        self.request_api = request_api

    def get(
            self,
            date_from: datetime = None,
            date_to: datetime = None,
            order_statuses: list = None,
            ids: List[int] = None,
            internal_numbers: List[int] = None,
            customer_id: int = None,
            count: int = 500,
            offset: int = 0
    ) -> 'OrdersResponse':
        if order_statuses is None:
            order_statuses = []
        if count > MAX_COUNT_CUSTOMERS_PER_REQUEST:
            raise TooLargeBoarders(
                f'Количество запрашиваемых клиентов за раз должно быть меньше {MAX_COUNT_CUSTOMERS_PER_REQUEST}'
            )
        out_statuses = []
        for status in order_statuses:
            if isinstance(status, int):
                out_statuses.append({'id': status})
            elif isinstance(status, str):
                out_statuses.append({'name': status})
            else:
                raise TypeError(f'Ожидалось int или str, получил {type(status)}')

        data = {
            'dateFrom': date_from.strftime('%Y-%m-%d') if date_from else None,
            'dateTill': (date_to + timedelta(days=1)).strftime(
                '%Y-%m-%d') if date_to else None,
            'orderStatuses': out_statuses,
            'customerId': customer_id,
            'ids': ids,
            'internalNumbers': internal_numbers,
            'pageSize': count,
            'startRowNumber': offset,
        }
        response = self.request_api.send(
            OrdersMethods.get,
            data=data
        )

        return OrdersResponse(response)

    def get_all(
            self,
            date_from: datetime = None,
            date_to: datetime = None,
            order_statuses: list = None,
            ids: List[int] = None,
            internal_numbers: List[int] = None,
            customer_id: int = None,
    ) -> List[Order]:

        items = []
        count = MAX_COUNT_CUSTOMERS_PER_REQUEST
        offset = 0

        r = self.get(
            date_from, date_to, order_statuses,
            ids, internal_numbers, customer_id,
            count=1, offset=0
        )
        total_count = r.not_returned_count + r.count

        if total_count == 0:
            return []

        with Bar(f'Orders | {self.request_api.login}',
                 max=total_count, fill='█', empty_fill='░') as bar:
            while len(items) < total_count:
                r = self.get(
                    date_from, date_to, order_statuses,
                    ids, internal_numbers, customer_id,
                    count, offset
                )
                items.extend(r.orders)
                offset += count
                bar.next(r.count)
                sleep(2)
        return items

    def set_many_statuses(
        self,
        data: list[tuple[str, str]]
    ):
        grouped_data = {}

        for order_id, status in data:
            if status not in grouped_data:
                grouped_data[status] = []
            grouped_data[status].append(order_id)

        for crm_status, ids in grouped_data.items():
            crm_status = int(crm_status)

            if not ids:
                continue

            logger.info(f"Обновление {len(ids)} заказов до статуса '{crm_status}'.")

            for id in ids:
                logger.info(f"Обновление заказа {id} до статуса {crm_status} ({Settings.INVERTED_STATUSES[str(crm_status)]}), https://bluesales.ru/app/Customers/OrderView.aspx?id={id}")

            orders_data = {
                "ids": ids,
                "orderStatus": {
                    "id": crm_status
                },
            }

            today = date.today()

            formatted_date = today.strftime("%Y-%m-%d")

            if crm_status in Settings.STATUS_TO_DATE_FIELD:
                orders_data.append({
                    "customFieldValue": {
                        "fieldId": Settings.STATUS_TO_DATE_FIELD[crm_status],
                        "value": formatted_date
                    }
                })

            try:
                response = self.request_api.send(
                    OrdersMethods.update_many,
                    data=orders_data
                )

                if isinstance(response, str):
                    print(f"Результат обновления {len(ids)} заказов до статуса '{crm_status}': '{response}'")
                elif response.success:
                    print(f"Успешно обновлено {len(ids)} заказов до статуса '{crm_status}'.")
                else:
                    print(f"Ошибка при обновлении заказов до статуса '{crm_status}': {response.error}")

            except Exception as e:
                print(f"Исключение при обновлении заказов до статуса '{crm_status}': {e}")

class OrdersResponse:
    def __init__(self, response: dict):
        self.count: int = response['count']
        self.not_returned_count: int = response['notReturnedCount']
        self.orders: list[Order] = [Order(obj) for obj in response['orders']]
        self.response: dict = response

    def __repr__(self):
        return str(self.response)

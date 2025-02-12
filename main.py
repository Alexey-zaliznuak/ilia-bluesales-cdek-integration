# !/root/bluesales-cdek-transfering-integration/venv/bin/python
import os

from typing import List

from datetime import datetime, timedelta
from time import sleep
from requests.exceptions import HTTPError

from external.cdek import Client
from external.bluesales import BlueSales
from external.bluesales.exceptions import BlueSalesError

import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
from settings import Settings
from external.bluesales.ordersapi import Order


logger = logging.getLogger("root")
logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler("/root/bluesales-cdek-transfering-integration/log.log", maxBytes=64*1024, backupCount=3, encoding='utf-8')
formatter = logging.Formatter('%(message)s')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

full_file_handler = RotatingFileHandler("/root/bluesales-cdek-transfering-integration/full_log.log", maxBytes=256*1024, backupCount=3, encoding='utf-8')
full_file_handler.setFormatter(formatter)
full_file_handler.setLevel(logging.DEBUG)

logger.addHandler(file_handler)
logger.addHandler(full_file_handler)

stream_handler = StreamHandler()
stream_formatter = logging.Formatter("%(message)s")
stream_handler.setFormatter(stream_formatter)
stream_handler.setLevel(logging.INFO)
logger.addHandler(stream_handler)


def notify_that_orders_in_pvz(orders: List[Order]):
    if not orders:
        return

    logger.info("\n=== Рассылка уведомления о доставке в пунты выдачи / постаматы ===")

    for order in orders:
        order_contact_data = (
            f"Айди клиента в вк: {order.customer_vk_id}, "
            f"Айди группы переписки клиента в вк: {order.customer_vk_messages_group_id}, "
            f"https://bluesales.ru/app/Customers/OrderView.aspx?id={order.id}"
        )

        if not (order.customer_vk_id and order.customer_vk_messages_group_id):
            logger.info(f"У клиента не указаны данные в вк для уведомления. {order_contact_data}")
            continue

        vk = Settings.VK_CLIENTS_BY_GROUP_ID[order.customer_vk_messages_group_id]
        result = vk.messages.send(
            user_id=order.customer_vk_id,
            # message=Settings.text_for_postomat if is_postomat else Settings.text_for_pvz,
            message=Settings.text_for_pvz,
            random_id=int.from_bytes(os.getrandom(4), byteorder="big")
        )
        logger.debug("Результат отправки: " + str(result))
        logger.info(f"Отправка уведомления что заказ в пвз/постомате. {order_contact_data}")

def notify_that_orders_picked(orders: List[Order]):
    if not orders:
        return

    logger.info("\n=== Рассылка уведомления о получении заказа ===")

    for order in orders:
        order_contact_data = (
            f"Айди клиента в вк: {order.customer_vk_id}, "
            f"Айди группы переписки клиента в вк: {order.customer_vk_messages_group_id}, "
            f"https://bluesales.ru/app/Customers/OrderView.aspx?id={order.id}"
        )

        if not (order.customer_vk_id and order.customer_vk_messages_group_id):
            logger.info(f"У клиента не указаны данные в вк для уведомления. {order_contact_data}")
            continue

        vk = Settings.VK_CLIENTS_BY_GROUP_ID[order.customer_vk_messages_group_id]
        result = vk.messages.send(
            user_id=order.customer_vk_id,
            message=Settings.text_for_picked,
            random_id=int.from_bytes(os.getrandom(4), byteorder="big")
        )
        logger.debug("Результат отправки: " + str(result))
        logger.info(f"Отправка уведомления что заказ получен. {order_contact_data}")

def notify_that_orders_returned(orders: List[Order]):
    if not orders:
        return

    logger.info("\n=== Рассылка уведомления о возврате заказа ===")

    for order in orders:
        order_contact_data = (
            f"Айди клиента в вк: {order.customer_vk_id}, "
            f"Айди группы переписки клиента в вк: {order.customer_vk_messages_group_id}, "
            f"https://bluesales.ru/app/Customers/OrderView.aspx?id={order.id}"
        )

        if not (order.customer_vk_id and order.customer_vk_messages_group_id):
            logger.info(f"У клиента не указаны данные в вк для уведомления. {order_contact_data}")
            continue

        vk = Settings.VK_CLIENTS_BY_GROUP_ID[order.customer_vk_messages_group_id]
        result = vk.messages.send(
            user_id=order.customer_vk_id,
            message=Settings.text_for_returned,
            random_id=int.from_bytes(os.getrandom(4), byteorder="big")
        )
        logger.debug("Результат отправки: " + str(result))
        logger.info(f"Отправка уведомления что заказ возвращается. {order_contact_data}")


def get_crm_status_by_cdek(current_crm_status: str, cdek_status_name: str):
    return Settings.CDEK_TO_CRM_STATUS_ID.get(cdek_status_name, current_crm_status)

def main(*args, **kwargs):
    BLUESALES = BlueSales(Settings.BLUESALES_LOGIN, Settings.BLUESALES_PASSWORD)
    CDEK = Client(Settings.CDEK_CLIENT_ID, Settings.CDEK_CLIENT_SECRET)

    bluesales_orders = []

    for _ in range(3):
        try:
            bluesales_orders = BLUESALES.orders.get_all(date_from=datetime.today() - timedelta(days=60))
            break
        except BlueSalesError as e:
            print(e, "sleep 30 seconds...")
            sleep(30)

    print("Всего:", len(bluesales_orders), "сделок")

    # отсеиваем у кого нет статуса или номера трекера
    bluesales_orders = list(filter(
        lambda o:
            o.tracking_number
            and o.status_name
            and o.status_name not in [
                "Возврат",
                "Доставлен",
                "Вручен",
            ],
        bluesales_orders
        )
    )

    print("Активных", len(bluesales_orders), "сделок")

    update_orders = []

    orders_notify_that_order_in_pvz = []  # заказы, заказчикам которых нужно сделать уведу что из заказ в ПВЗ
    orders_notify_that_order_picked = []  # заказы, заказчикам которых нужно сделать уведу что они забрпли заказ
    orders_notify_that_order_returned = []  # заказы, заказчикам которых нужно сделать уведу что у них возврат

    for order in bluesales_orders:
        try:
            if order.status_name in ["Разбор", "Правки заказа"]:
                continue
            cdek_status = CDEK.get_order_info(order.tracking_number)["entity"]["statuses"][0]["code"]

            if (
                cdek_status != 'CREATED' and
                Settings.STATUSES[order.status_name] != get_crm_status_by_cdek(order.status_name, cdek_status)  # статус поменялся
            ):
                update_orders.append([order.id, get_crm_status_by_cdek(order.status_name, cdek_status)])

                if get_crm_status_by_cdek(order.status_name, cdek_status) == Settings.STATUSES["Ожидает в ПВЗ"]:
                    orders_notify_that_order_in_pvz.append(order)

                if get_crm_status_by_cdek(order.status_name, cdek_status) == Settings.STATUSES["Вручен"]:
                    orders_notify_that_order_picked.append(order)

                if get_crm_status_by_cdek(order.status_name, cdek_status) == Settings.STATUSES["Возврат"]:
                    orders_notify_that_order_returned.append(order)

        except HTTPError as e:
            logger.error(e)

    BLUESALES.orders.set_many_statuses(update_orders)

    notify_that_orders_in_pvz(orders_notify_that_order_in_pvz)
    # notify_that_orders_picked(orders_notify_that_order_picked)
    # notify_that_orders_returned(orders_notify_that_order_returned)

if __name__ == "__main__":
    logger.info(
        "=" * 10 + "  " + datetime.now().strftime("%d-%m-%Y %H:%M") + "  " + "=" * 10
    )
    try:
        main()
    except TimeoutError as e:
        logger.error(e)
    finally:
        logger.info("\n"*2)

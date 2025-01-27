from time import sleep
from requests.exceptions import HTTPError

from external.cdek import Client
from external.bluesales import BlueSales
from settings import Settings


STATUSES = {
    "Новый": "134024",
    "На макет": "120520",
    "Макет готов": "134023",
    "Вторая предоплата": "164126",
    "Правки МАКЕТА": "125845",
    "Макет Гравировки": "159523",
    "Правка гравировки": "159524",
    "На изготовление": "133378",
    "Изготовление заказа": "133379",
    "На гравировку": "155538",
    "Гравировка заказа": "155845",
    "Переизготовление": "149372",
    "Правка ЗАКАЗА": "145088",
    "Заказ готов": "134119",
    "На отправку": "133382",
    "Отправить позже": "133380",
    "Отправить Почта России": "154196",
    "Ошибка при отправке": "153827",
    "Отправлен": "125846",
    "Досылка почты": "154197",
    "Доставлен до ПВЗ": "120524",
    "Вручен": "133163",
    "ЗП выплачено": "133376",
    "Возврат (в пути)": "150581",
    "Возврат": "133375",
    "Возврат в офисе": "154934",
    "Отказ от заказа": "146307",
    "Отказ от макета": "125843",
    "Утилизация": "148185",
    "отложенная предоплата": "158237"
}

CDEK_TO_CRM_STATUS_ID = {
    "ACCEPTED": STATUSES["ЗАКАЗ ОФОРМЛЕН"],
    "CREATED": STATUSES["Заказ готов"],
    "RECEIVED_AT_SHIPMENT_WAREHOUSE": STATUSES["Ожидает отправку СДЭК"],
    "READY_TO_SHIP_AT_SENDING_OFFICE": STATUSES["Отправлен+"],
    "READY_FOR_SHIPMENT_IN_TRANSIT_CITY": STATUSES["Отправлен+"],
    "READY_FOR_SHIPMENT_IN_SENDER_CITY": STATUSES["Отправлен+"],
    "RETURNED_TO_SENDER_CITY_WAREHOUSE": STATUSES["Отправлен+"],
    "TAKEN_BY_TRANSPORTER_FROM_SENDER_CITY": STATUSES["Отправлен+"],
    "SENT_TO_TRANSIT_CITY": STATUSES["Отправлен+"],
    "ACCEPTED_IN_TRANSIT_CITY": STATUSES["Отправлен+"],
    "ACCEPTED_AT_TRANSIT_WAREHOUSE": STATUSES["Отправлен+"],
    "RETURNED_TO_TRANSIT_WAREHOUSE": STATUSES["Отправлен+"],
    "READY_TO_SHIP_IN_TRANSIT_OFFICE": STATUSES["Отправлен+"],
    "TAKEN_BY_TRANSPORTER_FROM_TRANSIT_CITY": STATUSES["Отправлен+"],
    "SENT_TO_SENDER_CITY": STATUSES["Отправлен+"],
    "SENT_TO_RECIPIENT_CITY": STATUSES["Отправлен+"],
    "ACCEPTED_IN_SENDER_CITY": STATUSES["Отправлен+"],
    "ACCEPTED_IN_RECIPIENT_CITY": STATUSES["Отправлен+"],
    "ACCEPTED_AT_RECIPIENT_CITY_WAREHOUSE": STATUSES["Отправлен+"],
    "ACCEPTED_AT_PICK_UP_POINT": STATUSES["Ожидает в ПВЗ"],
    "TAKEN_BY_COURIER": STATUSES["Отправлен+"],
    "RETURNED_TO_RECIPIENT_CITY_WAREHOUSE": STATUSES["Отправлен+"],
    "DELIVERED": STATUSES["Доставлен"],
    "NOT_DELIVERED": STATUSES["Возврат"],
    "INVALID": STATUSES["Возврат"],
    "IN_CUSTOMS_INTERNATIONAL": STATUSES["Отправлен+"],
    "SHIPPED_TO_DESTINATION": STATUSES["Отправлен+"],
    "PASSED_TO_TRANSIT_CARRIER": STATUSES["Отправлен+"],
    "IN_CUSTOMS_LOCAL": STATUSES["Отправлен+"],
    "CUSTOMS_COMPLETE": STATUSES["Ожидает в ПВЗ"],
    "POSTOMAT_POSTED": STATUSES["Возврат"],
    "POSTOMAT_SEIZED": STATUSES["Возврат"],
    "POSTOMAT_RECEIVED": STATUSES["Доставлен"],
}


def get_crm_status_by_cdek(current_crm_status: str, cdek_status_name: str):
    return CDEK_TO_CRM_STATUS_ID.get(cdek_status_name, current_crm_status)

def main(*args, **kwargs):
    BLUESALES = BlueSales(Settings.BLUESALES_LOGIN, Settings.BLUESALES_PASSWORD)
    CDEK = Client(Settings.CDEK_CLIENT_ID, Settings.CDEK_CLIENT_SECRET)

    for _ in range(3):
        try:
            bluesales_orders = BLUESALES.orders.get_all()
            break
        except Exception as e:
            print(e)
            sleep(30)

    print("Всего:", len(bluesales_orders), "сделок")

    # отсеиваем у кого нет статуса или номера трекера в сдек
    bluesales_orders = list(filter(
        lambda o:
            o.tracking_number
            and o.status_name
            and o.status_name not in [
                "Вручен",
                "ЗП выплачено",
                "Отказ от заказа",
                "Утилизация",
                "Возврат",
                "Переизготовление",
            ],
        bluesales_orders
        )
    )

    print("Активных", len(bluesales_orders), "сделок")

    update_orders = []
    all_statuses = []


    for order in bluesales_orders:
        try:
            if order.status_name == "Разбор":
                continue

            cdek_status = CDEK.get_order_info(order.tracking_number)["entity"]["statuses"][0]["code"]
            # print(order.status_name, cdek_status, get_crm_status_by_cdek(order.status_name, cdek_status))

            all_statuses.append({
                'order_id': order.id,
                'status_name': order.status_name,
                'cdek_status': cdek_status
            })

            if (
                cdek_status != 'CREATED' and
                STATUSES[order.status_name] != get_crm_status_by_cdek(order.status_name, cdek_status)
            ):
                # print('Добавлен ', order.id, get_crm_status_by_cdek(order.status_name, cdek_status), 'был', STATUSES[order.status_name], '-', cdek_status)
                update_orders.append([order.id, get_crm_status_by_cdek(order.status_name, cdek_status)])
        except HTTPError as e:
            print(e)

    # with open('order_statuses.json', 'w', encoding='utf-8') as f:
    #     json.dump(all_statuses, f, ensure_ascii=False, indent=4)

    BLUESALES.orders.set_many_statuses(update_orders)

if __name__ == "__main__":
    main()

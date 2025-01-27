import requests


class Client:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret

        self.access_token = self.get_access_token(client_id, client_secret)

    def get_access_token(self, client_id, client_secret):
        url = f'https://api.cdek.ru/v2/oauth/token?grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}'
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        return response.json()['access_token']

    def get_order_info(self, cdek_number):
        url = f'https://api.cdek.ru/v2/orders?cdek_number={cdek_number}'
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def update_order_status(self, order_id, new_status_id: int):
        url = "https://api.bluesales.ru/v1/orders/updateMany"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "id": order_id,
            "orderStatus": {
                "id": new_status_id
            }
        }

        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            return response.json()

        return {"error": response.text}

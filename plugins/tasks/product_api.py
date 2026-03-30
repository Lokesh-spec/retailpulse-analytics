import requests
import logging

logger = logging.getLogger(__name__)


def fetch_product_data_api(url: str, headers: dict,
                           timeout: int) -> list[dict]:
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        logger.info(f"Status: {response.status_code}")
        return response.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f'HTTP Error: {e}')
        raise
    except requests.exceptions.Timeout:
        logger.error('Request Time Out')
        raise
    except requests.exceptions.ConnectionError:
        logger.error('Connection Failed')
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise

# app/utils/pagination_utils.py
"""Utilities for paginating data lists."""

from typing import List, Any


def paginate_list_get_page(data: List[Any], page: int, per_page: int) -> List[Any]:
    """Returns a single page of data from a list."""
    if page < 1:
        page = 1
    start = (page - 1) * per_page
    end = start + per_page
    return data[start:end]


def paginate_list_all_pages(data: List[Any], per_page: int) -> List[List[Any]]:
    """Splits data into multiple pages (lists of items). Matches utils.py paginate_list."""
    if per_page <= 0:
        return [data] if data else [[]] # Avoid division by zero, return all data in one page or empty
    return [data[i : i + per_page] for i in range(0, len(data), per_page)]

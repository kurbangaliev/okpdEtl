import time
from tabnanny import check
from datetime import datetime

from tenacity import wait_exponential


def is_string(string):
    return True if isinstance(string, str) else False

def is_number(number):
    return True if isinstance(number, int) or isinstance(number, float) else False

def check_number(str_value):
    if is_number(str_value):
        return True
    if is_string(str_value):
        return False
    if isinstance(int, float):
        return True
    if str_value.isdigit():
        return True
    return False

def convert_to_number(any_value):
    if any_value is None:
        return 0
    if is_number(any_value):
        return any_value
    if is_string(any_value):
        value = 0
        try:
            value = float(any_value)
        except ValueError:
            value = 0
        return value

print(convert_to_number("281119.60"))


now = datetime.now()
time.sleep(10)
now2 = datetime.now()
print(now)
print(now2)
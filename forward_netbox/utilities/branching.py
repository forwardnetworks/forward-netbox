from uuid import uuid4

from utilities.request import NetBoxFakeRequest


def build_branch_request(user):
    return NetBoxFakeRequest(
        {
            "id": uuid4(),
            "user": user,
            "META": {},
            "COOKIES": {},
            "POST": {},
            "GET": {},
            "FILES": {},
            "method": "POST",
            "path": "",
        }
    )

from starlette.requests import Request

from scripts.admin_ui import (
    render_admin_acl,
    render_admin_bootstrap,
    render_admin_error,
    render_admin_login,
)


def _request(path: str = "/admin/login") -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [],
            "query_string": b"",
            "server": ("testserver", 80),
            "scheme": "http",
            "client": ("127.0.0.1", 12345),
        }
    )


def test_admin_templates_render_with_request_first_api():
    request = _request()

    responses = [
        render_admin_login(request),
        render_admin_bootstrap(request),
        render_admin_acl(request, users=[], collections=[], grants={}),
        render_admin_error(request, title="Error", message="Something failed"),
    ]

    assert [response.status_code for response in responses] == [200, 200, 200, 400]

from webhook.helpers import determine_alert_routes


def test_helpers_module_importable():
    assert callable(determine_alert_routes)

from transit_reliability.api.main import app


def test_api_routes_are_registered() -> None:
    paths = {route.path for route in app.routes}

    assert "/health" in paths
    assert "/trains/current" in paths
    assert "/routes/reliability" in paths
    assert "/feed-health" in paths
    assert "/history/line-activity" in paths
    assert "/history/feed-health" in paths

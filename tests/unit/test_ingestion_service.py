from src.DataIngestion.api import ingestion_service


class FakeCursor:
    def __init__(self, row):
        self.row = row
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, values):
        self.executed.append((query, values))

    def fetchone(self):
        return self.row


class FakeConnection:
    def __init__(self, row):
        self.cursor_obj = FakeCursor(row)
        self.committed = False
        self.closed = False

    # psycopg2: `with conn` commits on success, rolls back on error, but does NOT close
    def __enter__(self):
        return self

    def __exit__(self, exc_type, *rest):
        self.committed = exc_type is None
        return False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


def test_upsert_team_commits_and_closes_the_connection(monkeypatch):
    row = {"id": 1, "team_name": "Duke"}
    connection = FakeConnection(row)
    monkeypatch.setattr(ingestion_service, "get_connection", lambda: connection)

    response = ingestion_service.app.test_client().post("/teams", json={
        "team_name": "Duke", "conference_id": 4, "regular_season_record": "34-2",
        "regular_season_wins": 34, "regular_season_losses": 2,
    })

    assert response.status_code == 200
    assert response.get_json() == row
    assert connection.committed is True
    assert connection.closed is True
    query, values = connection.cursor_obj.executed[0]
    assert "ON CONFLICT (team_name)" in query
    assert values == ("Duke", 4, "34-2", 34, 2)


def test_health():
    assert ingestion_service.app.test_client().get("/health").get_json() == {"status": "ok"}

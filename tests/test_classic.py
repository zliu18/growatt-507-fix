"""Exercise the Classic repair with real HA and mocked Growatt transport."""

import asyncio
from datetime import UTC, datetime, timedelta
from importlib.metadata import version
from json import JSONDecodeError, loads
from pathlib import Path
from threading import Event
from types import MappingProxyType
from unittest.mock import AsyncMock, Mock
from zoneinfo import ZoneInfo

import pytest
from requests import ConnectionError, Response
from requests.adapters import HTTPAdapter

from homeassistant import loader
from homeassistant.components.energy.data import SOLAR_SOURCE_SCHEMA
from homeassistant.components.sensor.recorder import list_statistic_ids
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr, entity_registry as er
from homeassistant.helpers.recorder import DATA_INSTANCE
from homeassistant.helpers.sun import get_astral_event_date
from homeassistant.helpers.update_coordinator import UpdateFailed
from homeassistant.util import dt as dt_util

from custom_components import growatt_server
from custom_components.growatt_server import classic
from custom_components.growatt_server.config_flow import GrowattServerConfigFlow
from custom_components.growatt_server.coordinator import GrowattCoordinator
from custom_components.growatt_server.models import GrowattRuntimeData
from custom_components.growatt_server.sensor import async_setup_entry as setup_sensors

pytestmark = pytest.mark.asyncio

URL = "https://openapi-au.growatt.com/"
LOGIN = {"success": True, "user": {"id": "test-user-id"}}
NOW = datetime(2026, 11, 20, tzinfo=UTC)
LOGIN_ERROR_PAGE = (Path(__file__).parent / "fixtures/classic_login_error.html").read_bytes()


@pytest.fixture(autouse=True)
def deny_network(monkeypatch):
    """Fail the suite if a requests client tries to reach the network."""

    def deny(*args, **kwargs):
        pytest.fail("Unexpected network request")

    monkeypatch.setattr(HTTPAdapter, "send", deny)


@pytest.fixture
def mock_growatt(monkeypatch):
    """Each client has a distinct mock session and shares the scripted cloud."""
    cloud = Mock()
    cloud.login.return_value = LOGIN
    cloud.inverter_detail.return_value = {"status": 1, "deviceSn": "inverter-1"}
    cloud.plant_info.return_value = {
        "plantMoneyText": "1/AUD",
        "deviceList": [],
        "totalEnergy": 1234.5,
    }
    cloud.device_list.return_value = [
        {"deviceSn": "inverter-1", "deviceType": "inverter"}
    ]

    def create(**kwargs):
        api = Mock()
        api.session.hooks = {"response": []}
        api.login = cloud.login
        api.inverter_detail = cloud.inverter_detail
        api.plant_info = cloud.plant_info
        api.device_list = cloud.device_list
        return api

    monkeypatch.setattr(classic, "GrowattApi", create)
    return cloud


def entry(plant="plant-1", password="mock-secret"):
    return ConfigEntry(
        data={
            "auth_type": "password",
            "plant_id": plant,
            "username": "MockUser",
            "password": password,
            "url": URL,
            "name": "Mock Growatt",
        },
        discovery_keys=MappingProxyType({}),
        domain="growatt_server",
        minor_version=1,
        options={},
        source="user",
        state=ConfigEntryState.SETUP_IN_PROGRESS,
        subentries_data=None,
        title="Mock Growatt",
        unique_id=plant,
        version=1,
    )


def coordinator(hass, config_entry=None):
    return GrowattCoordinator(
        hass, config_entry or entry(), "inverter-1", "inverter", "plant-1"
    )


def clock(monkeypatch):
    class Clock(datetime):
        current = NOW

        @classmethod
        def now(cls, tz=None):
            return cls.current.astimezone(tz)

    monkeypatch.setattr(classic, "datetime", Clock)
    return Clock


async def test_setup_and_concurrent_healthy_polls(tmp_path, mock_growatt):
    hass = HomeAssistant(str(tmp_path))
    hass.config_entries = Mock()
    hass.config_entries.async_forward_entry_setups = AsyncMock()
    config_entry = entry()
    original_data = dict(config_entry.data)

    await growatt_server.async_setup_entry(hass, config_entry)
    total = config_entry.runtime_data.total_coordinator
    device = config_entry.runtime_data.devices["inverter-1"]
    second_plant = coordinator(hass, entry("plant-2"))
    assert total.api is device.api is second_plant.api
    for _ in range(4):
        await asyncio.gather(
            total._async_update_data(),
            device._async_update_data(),
            second_plant._async_update_data(),
        )
    assert mock_growatt.login.call_count == 1
    assert mock_growatt.inverter_detail.call_count == 9
    assert dict(config_entry.data) == original_data


async def test_concurrent_expiry_reauthenticates_once(tmp_path, mock_growatt):
    hass = HomeAssistant(str(tmp_path))
    first, second = coordinator(hass), coordinator(hass, entry("plant-2"))
    await first._async_update_data()
    mock_growatt.inverter_detail.side_effect = [
        classic.ClassicSessionExpired,
        {"status": 2},
        {"status": 3},
    ]
    assert await asyncio.gather(
        first._async_update_data(), second._async_update_data()
    ) == [{"status": 2}, {"status": 3}]
    assert mock_growatt.login.call_count == 2


async def test_507_persists_and_one_probe_runs_after_restart(
    tmp_path, monkeypatch, mock_growatt
):
    utc = clock(monkeypatch)
    hass = HomeAssistant(str(tmp_path))
    first, second = coordinator(hass), coordinator(hass, entry("plant-2"))
    await first._async_update_data()
    mock_growatt.inverter_detail.side_effect = [classic.ClassicSessionExpired]
    mock_growatt.login.return_value = {"success": False, "msg": 507}
    failures = await asyncio.gather(
        first._async_update_data(), second._async_update_data(), return_exceptions=True
    )
    assert all(isinstance(error, UpdateFailed) for error in failures)
    assert mock_growatt.login.call_count == 2

    store = tmp_path / ".storage" / "growatt_server.classic_cooldowns"
    persisted = store.read_text()
    assert all(value not in persisted for value in ("MockUser", "mock-secret", URL))
    assert list(loads(persisted)["data"].values()) == [
        (NOW + timedelta(days=1)).isoformat()
    ]

    restored = HomeAssistant(str(tmp_path))
    third = coordinator(restored)
    utc.current = NOW + timedelta(days=1, microseconds=-1)
    with pytest.raises(UpdateFailed):
        await third._async_update_data()
    with pytest.raises(ConfigEntryNotReady):
        await growatt_server.async_setup_entry(restored, entry())
    assert mock_growatt.login.call_count == 2

    utc.current = NOW + timedelta(days=1)
    mock_growatt.login.return_value = LOGIN
    mock_growatt.inverter_detail.side_effect = None
    await asyncio.gather(
        third._async_update_data(), coordinator(restored)._async_update_data()
    )
    assert mock_growatt.login.call_count == 3


async def test_507_blocks_config_flow_and_failed_probe_renews_deadline(
    tmp_path, monkeypatch, mock_growatt
):
    utc = clock(monkeypatch)
    hass = HomeAssistant(str(tmp_path))
    first = coordinator(hass)
    mock_growatt.login.return_value = {"success": False, "msg": "507"}
    with pytest.raises(ConfigEntryNotReady):
        await growatt_server.async_setup_entry(hass, entry())

    flow = GrowattServerConfigFlow()
    flow.hass = hass
    credentials = {
        "region": "australia_new_zealand",
        "username": "MockUser",
        "password": "different",
    }
    flow._async_show_password_form = Mock(return_value={"type": "form"})
    await flow.async_step_password_auth(credentials)
    flow._async_show_password_form.assert_called_once_with({"base": "cannot_connect"})
    assert mock_growatt.login.call_count == 1

    utc.current += timedelta(days=1)
    failures = await asyncio.gather(
        first._async_update_data(),
        coordinator(hass, entry(password="other"))._async_update_data(),
        return_exceptions=True,
    )
    assert all(isinstance(error, UpdateFailed) for error in failures)
    assert mock_growatt.login.call_count == 2
    with pytest.raises(classic.ClassicLoginCooldown) as error:
        await first.classic_session.async_login()
    assert error.value.until == NOW + timedelta(days=2)


async def test_502_is_credentials_error_in_setup_recovery_and_flow(
    tmp_path, mock_growatt
):
    hass = HomeAssistant(str(tmp_path))
    device = coordinator(hass)
    await device._async_update_data()
    mock_growatt.inverter_detail.side_effect = classic.ClassicSessionExpired
    mock_growatt.login.return_value = {"success": False, "msg": "502"}
    with pytest.raises(ConfigEntryAuthFailed):
        await device._async_update_data()
    with pytest.raises(ConfigEntryAuthFailed):
        await growatt_server.async_setup_entry(hass, entry())
    assert mock_growatt.login.call_count == 2

    flow = GrowattServerConfigFlow()
    flow.hass = hass
    flow._async_show_password_form = Mock(return_value={"type": "form"})
    await flow.async_step_password_auth(
        {
            "region": "australia_new_zealand",
            "username": "MockUser",
            "password": "mock-secret",
        }
    )
    flow._async_show_password_form.assert_called_once_with({"base": "invalid_auth"})
    assert mock_growatt.login.call_count == 2
    assert not (tmp_path / ".storage" / "growatt_server.classic_cooldowns").exists()


@pytest.mark.parametrize(
    "failure",
    [
        JSONDecodeError("non-JSON", "<html>upstream</html>", 0),
        ConnectionError("network down"),
    ],
)
async def test_other_transport_failures_do_not_relogin(tmp_path, mock_growatt, failure):
    hass = HomeAssistant(str(tmp_path))
    device = coordinator(hass)
    await device._async_update_data()
    mock_growatt.inverter_detail.side_effect = failure
    with pytest.raises(UpdateFailed):
        await device._async_update_data()
    mock_growatt.inverter_detail.side_effect = None
    assert await device._async_update_data() == {"status": 1, "deviceSn": "inverter-1"}
    assert mock_growatt.login.call_count == 1


@pytest.mark.parametrize(
    ("status", "content", "headers", "expected"),
    [
        (401, b"", {}, classic.ClassicSessionExpired),
        (302, b"", {"Location": "/login.do"}, classic.ClassicSessionExpired),
        (302, b"", {"Location": "https://other.invalid/login.do"}, None),
        (
            200,
            b'<form action="/newTwoLoginAPI.do">',
            {"Content-Type": "text/html"},
            classic.ClassicSessionExpired,
        ),
        (200, b"<html>proxy error</html>", {"Content-Type": "text/html"}, None),
        (
            200,
            b'{"back": {"success": false, "msg": "507"}}',
            {},
            classic._ClassicRateLimited,
        ),
        (200, b'{"power": 507}', {}, None),
        (403, b"", {}, None),
        (507, b"", {}, None),
    ],
)
async def test_response_classification(status, content, headers, expected):
    response = Response()
    response.url = URL + "PlantListAPI.do"
    response.status_code = status
    response.headers.update(headers)
    response._content = content
    if expected:
        with pytest.raises(expected):
            classic._check_auth_response(response)
    else:
        classic._check_auth_response(response)


@pytest.mark.parametrize(
    ("path", "status", "content", "expired"),
    [
        ("error.do", 200, LOGIN_ERROR_PAGE, True),
        ("error.do", 403, LOGIN_ERROR_PAGE, False),
        ("maintenance.do", 200, LOGIN_ERROR_PAGE, False),
        ("error.do", 200, b"<html><title>Login Page</title>server error</html>", False),
        ("error.do", 200, LOGIN_ERROR_PAGE.replace(b"dumpLogin", b"serverError"), False),
        ("error.do", 200, LOGIN_ERROR_PAGE.replace(b'href="/login"', b'href="https://other.invalid/login"'), False),
    ],
)
async def test_explicit_login_error_page(path, status, content, expired):
    response = Response()
    response.url = URL + path
    response.status_code = status
    response.headers["Content-Type"] = "text/html;charset=UTF-8"
    response._content = content
    if expired:
        with pytest.raises(classic.ClassicSessionExpired):
            classic._check_auth_response(response)
    else:
        classic._check_auth_response(response)


@pytest.mark.parametrize("rate_limited", [False, True])
async def test_login_error_redirect_recovers_once_or_persists_507(
    tmp_path, monkeypatch, rate_limited
):
    """Replay the observed /error.do redirect using the real pinned library."""
    hass = HomeAssistant(str(tmp_path))
    first, second = coordinator(hass), coordinator(hass, entry("plant-2"))
    login = b'{"back":{"success":true,"user":{"id":"mock","rightlevel":1}}}'
    recovery = b'{"back":{"success":false,"msg":"507"}}' if rate_limited else login
    script = [
        (200, login, {}),
        (200, b'{"status":1}', {}),
        (302, b"", {"Location": "/error.do"}),
        (200, LOGIN_ERROR_PAGE, {"Content-Type": "text/html;charset=UTF-8"}),
        (200, recovery, {}),
    ]
    if not rate_limited:
        script.extend([(200, b'{"status":2}', {}), (200, b'{"status":3}', {})])
    responses = iter(script)
    paths = []

    def mock_send(adapter, request, **kwargs):
        assert kwargs["timeout"] == (10, 30)
        paths.append(request.path_url.split("?")[0])
        status, content, headers = next(responses)
        response = Response()
        response.url = request.url
        response.request = request
        response.status_code = status
        response._content = content
        response.headers["Content-Type"] = "application/json"
        response.headers.update(headers)
        return response

    monkeypatch.setattr(HTTPAdapter, "send", mock_send)
    assert await first._async_update_data() == {"status": 1}
    result = await asyncio.gather(
        first._async_update_data(), second._async_update_data(), return_exceptions=True
    )
    assert paths[:5] == [
        "/newTwoLoginAPI.do", "/newInverterAPI.do", "/newInverterAPI.do",
        "/error.do", "/newTwoLoginAPI.do",
    ]
    assert paths.count("/newTwoLoginAPI.do") == 2
    if rate_limited:
        assert all(isinstance(error, UpdateFailed) for error in result)
        assert len(paths) == 5
        assert (tmp_path / ".storage/growatt_server.classic_cooldowns").exists()
    else:
        assert result == [{"status": 2}, {"status": 3}]
        assert len(paths) == 7


async def test_cancelled_poll_finishes_persisting_507(tmp_path, mock_growatt):
    hass = HomeAssistant(str(tmp_path))
    device = coordinator(hass)
    started, release = Event(), Event()

    def blocked_login(*args):
        started.set()
        assert release.wait(timeout=5)
        return {"success": False, "msg": "507"}

    mock_growatt.login.side_effect = blocked_login
    task = asyncio.create_task(device._async_update_data())
    try:
        assert await asyncio.to_thread(started.wait, 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()

    with pytest.raises(UpdateFailed):
        await coordinator(hass)._async_update_data()
    assert mock_growatt.login.call_count == 1
    assert (tmp_path / ".storage" / "growatt_server.classic_cooldowns").exists()


async def test_sensor_registry_reuses_energy_entity_and_statistic_id(
    tmp_path, mock_growatt
):
    hass = HomeAssistant(str(tmp_path))
    config_entry = entry()
    total = GrowattCoordinator(hass, config_entry, "plant-1", "total", "plant-1")
    device = coordinator(hass, config_entry)
    await device._async_update_data()
    config_entry.runtime_data = GrowattRuntimeData(total, {"inverter-1": device})
    added = []
    await setup_sensors(hass, config_entry, added.extend)

    hass.config_entries = Mock()
    hass.config_entries.async_wait_initialized = AsyncMock()
    hass.config_entries.async_get_entry.return_value = config_entry
    dr.async_setup(hass)
    await dr.async_load(hass, load_empty=True)
    registry = er.async_get(hass)
    await registry.async_load()
    sensor = next(
        entity for entity in added if entity.unique_id == "plant-1-total_energy_output"
    )
    original = registry.async_get_or_create(
        "sensor",
        "growatt_server",
        "plant-1-total_energy_output",
        suggested_object_id="existing_solar_production",
        config_entry=config_entry,
    )
    migrated = registry.async_get_or_create(
        "sensor", "growatt_server", sensor.unique_id, config_entry=config_entry
    )
    assert (
        migrated.entity_id == original.entity_id == "sensor.existing_solar_production"
    )
    attributes = {
        "unit_of_measurement": sensor.native_unit_of_measurement,
        "device_class": sensor.device_class,
        "state_class": sensor.state_class,
    }
    energy_source = SOLAR_SOURCE_SCHEMA(
        {"type": "solar", "stat_energy_from": original.entity_id}
    )
    hass.states.async_set(original.entity_id, "1234.5", attributes)
    hass.data[DATA_INSTANCE] = Mock(entity_filter=lambda _: True)
    statistic_ids = [energy_source["stat_energy_from"]]
    baseline = await hass.async_add_executor_job(
        list_statistic_ids, hass, statistic_ids
    )
    hass.states.async_set(migrated.entity_id, "1234.6", attributes)
    migrated_stats = await hass.async_add_executor_job(
        list_statistic_ids, hass, statistic_ids
    )
    assert migrated_stats == baseline
    metadata = migrated_stats[original.entity_id]
    assert metadata["statistic_id"] == energy_source["stat_energy_from"]
    assert metadata["source"] == "recorder" and metadata["has_sum"]
    assert sensor.native_unit_of_measurement == "kWh"
    assert sensor.state_class == "total"
    assert sensor.device_class == "energy"
    assert any(entity.unique_id.startswith("inverter-1-") for entity in added)


async def test_installable_override_and_unchanged_sensor_sources(tmp_path):
    hass = HomeAssistant(str(tmp_path))
    loader.async_setup(hass)
    implementation = await loader.async_get_integration(hass, "growatt_server")
    assert implementation.pkg_path == "custom_components.growatt_server"
    assert version("homeassistant") == "2026.11.2"
    assert version("growattServer") == "2.2.0"
    official = Path(loader.__file__).parent / "components/growatt_server"
    patched = Path(growatt_server.__file__).parent
    for name in ("sensor", "number.py", "switch.py", "services.py", "translations"):
        candidate = official / name
        paths = (
            (path for path in candidate.rglob("*") if path.suffix in {".py", ".json"})
            if candidate.is_dir()
            else [candidate]
        )
        for path in paths:
            assert (
                path.read_bytes() == (patched / path.relative_to(official)).read_bytes()
            )


async def test_pinned_library_runs_response_hook(tmp_path, monkeypatch):
    """Run the real Requests response hooks and Growatt 2.2.0 decoder offline."""
    hass = HomeAssistant(str(tmp_path))
    device = coordinator(hass)
    login = b'{"back":{"success":true,"user":{"id":"mock","rightlevel":1}}}'
    responses = iter(
        [
            (200, login),
            (200, b'{"status":1}'),
            (401, b""),
            (200, login),
            (200, b'{"status":2}'),
            (200, b'{"back":{"success":false,"msg":"507"}}'),
        ]
    )
    paths = []

    def mock_send(adapter, request, **kwargs):
        assert kwargs["timeout"] == (10, 30)
        paths.append(request.path_url.split("?")[0])
        status, payload = next(responses)
        response = Response()
        response.status_code = status
        response.url = request.url
        response.request = request
        response._content = payload
        response.headers["Content-Type"] = "application/json"
        return response

    monkeypatch.setattr(HTTPAdapter, "send", mock_send)
    assert await device._async_update_data() == {"status": 1}
    assert await device._async_update_data() == {"status": 2}
    with pytest.raises(UpdateFailed):
        await device._async_update_data()
    with pytest.raises(UpdateFailed):
        await coordinator(hass)._async_update_data()
    assert paths == [
        "/newTwoLoginAPI.do",
        "/newInverterAPI.do",
        "/newInverterAPI.do",
        "/newTwoLoginAPI.do",
        "/newInverterAPI.do",
        "/newInverterAPI.do",
    ]


async def test_failed_expiry_replay_does_not_create_login_burst(tmp_path, mock_growatt):
    hass = HomeAssistant(str(tmp_path))
    first, second = coordinator(hass), coordinator(hass, entry("plant-2"))
    await first._async_update_data()
    mock_growatt.inverter_detail.side_effect = classic.ClassicSessionExpired
    failures = await asyncio.gather(
        first._async_update_data(), second._async_update_data(), return_exceptions=True
    )
    assert all(isinstance(error, UpdateFailed) for error in failures)
    assert mock_growatt.login.call_count == 2
    assert mock_growatt.inverter_detail.call_count == 3


async def test_cooldown_scoped_to_account_and_server(tmp_path, mock_growatt):
    hass = HomeAssistant(str(tmp_path))
    session = classic.async_get_classic_session(hass, URL, "MockUser", "secret")
    mock_growatt.login.return_value = {"success": False, "msg": "507"}
    with pytest.raises(classic.ClassicLoginCooldown):
        await session.async_login()
    mock_growatt.login.return_value = LOGIN

    same = classic.async_get_classic_session(hass, URL, "mockuser", "different")
    with pytest.raises(classic.ClassicLoginCooldown):
        await same.async_login()
    assert mock_growatt.login.call_count == 1

    other_account = classic.async_get_classic_session(hass, URL, "other", "secret")
    other_server = classic.async_get_classic_session(
        hass, "https://openapi.growatt.com/", "MockUser", "secret"
    )
    await asyncio.gather(other_account.async_login(), other_server.async_login())
    assert mock_growatt.login.call_count == 3


async def test_classic_background_polling_observes_solar_window(
    tmp_path, mock_growatt, monkeypatch
):
    """Pause both polling and discovery while preserving readings and errors."""
    hass = HomeAssistant(str(tmp_path))
    hass.config_entries = Mock()
    hass.config_entries.async_forward_entry_setups = AsyncMock()
    hass.config.latitude = -34
    hass.config.longitude = 151
    monkeypatch.setattr(dt_util, "DEFAULT_TIME_ZONE", ZoneInfo("Australia/Sydney"))
    track = Mock(return_value=Mock())
    monkeypatch.setattr(growatt_server, "async_track_time_interval", track)
    config_entry = entry()
    await growatt_server.async_setup_entry(hass, config_entry)
    total = config_entry.runtime_data.total_coordinator
    inverter = config_entry.runtime_data.devices["inverter-1"]
    schedule = Mock()
    monkeypatch.setattr(inverter, "_schedule_refresh", schedule)
    scan = track.call_args.args[1]

    day = datetime(2026, 11, 21).date()
    sunrise = get_astral_event_date(hass, "sunrise", day)
    sunset = get_astral_event_date(hass, "sunset", day)
    start = sunrise - timedelta(minutes=30)
    end = sunset + timedelta(minutes=30)
    now = start - timedelta(seconds=1)
    monkeypatch.setattr(dt_util, "utcnow", lambda: now)
    assert dt_util.as_local(now).date() != now.date()

    saved = inverter.data
    inverter.last_update_success = False
    await inverter._handle_refresh_interval()
    await scan(now)
    schedule.assert_called_once()
    assert inverter.data is saved
    assert not inverter.last_update_success
    assert not total._fetch_device_list
    assert mock_growatt.inverter_detail.call_count == 1
    assert mock_growatt.device_list.call_count == 1

    now = start
    await inverter._handle_refresh_interval()
    assert inverter.last_update_success
    assert mock_growatt.inverter_detail.call_count == 2

    now = end - timedelta(seconds=1)
    await inverter._handle_refresh_interval()
    assert mock_growatt.inverter_detail.call_count == 3

    now = end
    await inverter._handle_refresh_interval()
    await scan(now)
    assert mock_growatt.inverter_detail.call_count == 3
    assert mock_growatt.device_list.call_count == 1
    assert mock_growatt.login.call_count == 1


@pytest.mark.parametrize(
    ("hour", "minute", "paused"),
    [(6, 29, True), (6, 30, False), (20, 29, False), (20, 30, True)],
)
async def test_classic_polling_uses_local_fallback(
    tmp_path, mock_growatt, monkeypatch, hour, minute, paused
):
    """Fall back to fixed local hours when only one solar event is available."""
    from custom_components.growatt_server import coordinator as coordinator_module

    zone = ZoneInfo("Australia/Sydney")
    monkeypatch.setattr(dt_util, "DEFAULT_TIME_ZONE", zone)
    local_now = datetime(2026, 11, 21, hour, minute, tzinfo=zone)
    monkeypatch.setattr(dt_util, "utcnow", lambda: local_now.astimezone(UTC))
    monkeypatch.setattr(
        coordinator_module,
        "get_astral_event_date",
        Mock(side_effect=[None, local_now.astimezone(UTC)]),
    )
    hass = HomeAssistant(str(tmp_path))
    assert coordinator(hass).is_polling_paused() is paused

# Frontend integration

Base URL: `http://localhost:8081` in development, same origin as the SPA in production.

The OpenAPI contract is [openapi.yaml](../openapi.yaml), tagged `api-v0.1.0`. Generate the TypeScript client from it rather than hand-writing request types.

## Authentication

`POST /api/v1/auth/login`

```json
{"email": "dispatcher@demo.ftb", "password": "DemoPass123!"}
```

The response sets two cookies and returns the CSRF token:

```json
{
  "user": {"id": "...", "tenant_id": "...", "email": "...", "name": "...", "role": "dispatcher"},
  "csrf_token": "..."
}
```

- `ftb_session` is `HttpOnly` and identifies the session.
- `ftb_csrf` is readable by the SPA.
- Every `POST`, `PATCH` and `DELETE` must send `X-CSRF-Token` with the value from login, from `GET /me`, or from the `ftb_csrf` cookie.
- The session lasts `SESSION_TTL` (12h by default). There is no refresh endpoint; log in again.
- `POST /api/v1/auth/logout` revokes the session.

Roles:

| Role | Read | Mutate trips, vehicles, geofences, import | Acknowledge alerts | Audit |
|---|---|---|---|---|
| admin | yes | yes | yes | yes |
| dispatcher | yes | yes | yes | no |
| security | yes | no | yes | yes |
| viewer | yes | no | no | no |

Every response is scoped to the logged-in user's tenant. The client never sends a tenant id.

## Live data

1. `GET /api/v1/live/snapshot` returns every vehicle with its last position, speed, heading, `recorded_at` and `online`.
2. Open `GET /api/v1/live/ws` with the session cookie. The server pushes JSON messages:

```json
{"type":"position","tenant_id":"...","vehicle_id":"demo-imei-0007","lat":6.5,"lon":3.4,"recorded_at":"..."}
{"type":"alert","tenant_id":"...","vehicle_id":"...","alert":{"type":"geofence_exit","severity":"high","occurred_at":"..."}}
```

Rules:

- Messages are only for the user's tenant.
- Apply a position by `vehicle_id`.
- On disconnect, retry with backoff and call the snapshot endpoint again. There is no sequence number yet, so a reconnect can miss deltas.
- The WebSocket origin must match `WS_ALLOWED_ORIGINS`; for Vite, proxy `/api` and the socket through the dev server so both are same-origin.

## Screens and endpoints

| Screen | Calls |
|---|---|
| Login | `POST /auth/login` |
| App shell | `GET /me`, `POST /auth/logout` |
| Map | `GET /live/snapshot`, `GET /live/ws`, `GET /vehicles?q=&status=` |
| Vehicle drawer | `GET /vehicles/{id}`, `GET /vehicles/{id}/events` |
| Vehicle edit | `PATCH /vehicles/{id}` |
| Playback | `GET /vehicles/{id}/history?from=&to=&resolution=` |
| Alerts | `GET /alerts?status=&type=`, `POST /alerts/{id}/ack` |
| Alert rules | `GET /alert-rules`, `PATCH /alert-rules/{id}` |
| Trips | `GET /trips?status=`, `POST /trips`, `PATCH /trips/{id}` |
| Drivers | `GET /drivers`, `POST /drivers`, `PATCH /drivers/{id}` |
| Geofences | `GET /geofences`, `POST /geofences`, `PATCH /geofences/{id}`, `DELETE /geofences/{id}` |
| Import | `POST /import/vehicles` (multipart field `file`) |
| Reports | `GET /reports/summary?from=&to=` |
| System | `GET /system/health` |
| Audit | `GET /audit` (admin, security) |

## Conventions

- Lists use `limit` (default 100, max 500) and `offset`. The response is `{"data":[...]}` with no total count yet. Use the array length to decide whether to page.
- Geometry is GeoJSON. Timestamps are RFC 3339 UTC.
- Errors:

```json
{"error":{"code":"invalid_credentials","message":"invalid email or password"}}
```

- Enums:
  - roles: `admin`, `dispatcher`, `security`, `viewer`
  - trip status: `planned`, `assigned`, `active`, `completed`, `cancelled`
  - alert type: `geofence_enter`, `geofence_exit`, `route_deviation`, `idle`, `speeding`, `device_offline`, `unplanned_trip`
  - alert severity: `low`, `medium`, `high`, `critical`
  - alert status: `open`, `acked`, `closed`
  - geofence kind: `depot`, `customer`, `restricted`, `other`

## Import

CSV headers, all optional except `imei`:

```
imei, sim_number, device_model, protocol, plate, vehicle_type, make, model,
year, colour, chassis_number, depot, department, driver_name, driver_phone,
driver_licence
```

The response contains `job_id`, `total`, `imported`, `failed` and up to 100 row errors. Re-uploading the same file is safe.

## Local smoke test

```bash
API=http://localhost:8081
JAR=/tmp/ftb.jar
curl -s -c "$JAR" -X POST "$API/api/v1/auth/login" \
  -H 'Content-Type: application/json' \
  -d '{"email":"dispatcher@demo.ftb","password":"DemoPass123!"}'
curl -s -b "$JAR" "$API/api/v1/vehicles?limit=5"
```

## Known gaps

- No user administration screen or endpoint.
- No alert close action, only acknowledge.
- No pagination totals or report CSV export.
- No WebSocket sequence numbers.
- `GET /live/snapshot` returns the whole tenant; there is no bbox filter yet.
- Cookies require a same-origin SPA or a development proxy. There is no CORS configuration.

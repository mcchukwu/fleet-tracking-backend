# UI brief

## Product

A fleet control console for dispatchers, security and operations. The map is the product: where every truck is, what it is doing, and what needs attention.

Visual brand is configured in the frontend project. This document is about screens and behaviour, not identity.

## Users

| Role | Uses it for |
|---|---|
| Dispatcher | Assign trips, watch progress, acknowledge operational alerts |
| Security | Review incidents, replay a vehicle, check who did what |
| Admin | Vehicles, drivers, geofences, users, import |
| Viewer | Read-only oversight |

## Screens

### 1. Login

Email and password. Error state for wrong credentials and rate limiting. No self-service reset in v0.1.

### 2. Map (default)

- Full-bleed map, Google Maps first.
- Vehicle markers show plate or a short id; colour by status: moving, idle, offline, alert.
- Left panel: search by plate or IMEI, filter by status or depot, list of vehicles in view.
- KPI strip: vehicles online, active trips, open alerts, last data age.
- Clicking a marker opens the drawer.
- Geofences drawn as polygons, coloured by kind: depot, customer, restricted.

### 3. Vehicle drawer

Fields: plate, type, make/model/year, colour, depot, department, driver name and phone, current trip origin/destination/status, speed, heading, last seen, online.

Actions: centre on map, play back, edit vehicle, assign driver.

Below the fields, a short event list: recent geofence entries/exits and alerts, newest first.

### 4. Playback

Time range selector, default last 2 hours. Map shows the route line and a moving marker. Speed control. The API downsamples; show the resolution it used.

### 5. Alerts

- List: time, vehicle plate, type, severity, status, zone when relevant.
- Filters: open, acknowledged, type, severity.
- Action: acknowledge, with the current user recorded.
- Detail panel: map position, trip, evidence.

### 6. Trips

- Table: vehicle, driver, origin, destination, status, started, ended, distance.
- Create: choose vehicle, optional driver, origin, destination.
- Status actions: assign, start, complete, cancel. Completing stores distance.

### 7. Geofences

- List and map preview.
- Create and edit: name, kind (depot, customer, restricted, other), polygon drawn on the map.
- Delete is blocked when events exist; show the reason.

### 8. Drivers

Table: name, phone, licence, status, assigned vehicles. Create and edit.

### 9. Import

Drag a CSV, show the expected headers, upload, then show counts and the first row errors. Keep the previous jobs visible if cheap; the API keeps them in the database.

### 10. Reports

Range picker, default 7 days. Cards: vehicles, trips, completed trips, active trips, alerts, open alerts, distance. Alert breakdown by type. Export is not in v0.1.

### 11. System

Dependencies (Postgres, Redis), vehicles, online count, active trips, open alerts, ingest counters: received, dropped, stale, validation errors, queue depth, cold-path failures. This screen exists to answer "is the system healthy" during an incident.

### 12. Audit (admin, security)

Actor, action, entity, time. Filter by action if cheap. Read-only.

## Core flows

**Morning dispatch:** log in, map loads, filter by depot, create a trip for a vehicle, watch it go active, acknowledge an unplanned stop.

**Security incident:** alert arrives, open detail, replay the last 30 minutes, see the route deviation, acknowledge, note the driver and trip.

**Onboarding:** import a 5,000-row CSV, review errors, search a plate from the file, assign a driver.

## States

Every list and panel needs: loading, empty ("no vehicles match"), error with retry, and stale data. The map must show when the live socket is disconnected and when the last update is older than expected.

## Visual direction

Neutral, high contrast, one accent colour. Dense but calm. No gradients, no decorative cards, no dashboard clutter. The map owns the page; panels float above it. Numbers are tabular. Status uses colour plus a label, never colour alone.

## Demo story

1. Twenty trucks on the map, three geofences.
2. A truck exits the refinery; alert.
3. A second truck deviates; high alert with evidence.
4. Security replays the segment and acknowledges.
5. Import screen: 5,000 rows already processed, then a small live upload.
6. System screen: everything green, device last-seen visible.

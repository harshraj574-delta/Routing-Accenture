# eTMS Feature-Matrix Mapping (Accenture "Transport New App Success Matrix")

**Status:** Draft v1 — canonical taxonomy pending Accenture's actual xlsx
**Date:** 2026-06-17
**Purpose:** Map Delta's current + planned capabilities against the standard corporate
employee-transport (eTMS) feature scoring matrix, to maximize the
**Available / Partial / Not-Available** score that wins the procurement.

> **Action needed:** The actual *"Transport New App Success matrix doc - Ver 1.0 -
> 14102025.xlsx"* was an email attachment and is **not in the repo**. Drop it into
> `data/` and I will re-map row-by-row against Accenture's exact feature wording and
> their response columns (J/K/L/M, "Available / Available-need-customization /
> Not Available / Give Demo"). Until then this uses the standard eTMS taxonomy that
> these RFPs (MoveInSync, Routematic, Safetrax, WeGo) share ~80% of.

## Legend
- ✅ **Available** — exists today, demoable
- 🟡 **Partial** — core exists, needs customization/wiring
- 🔴 **Not Available** — to be built
- **Effort**: S (days) / M (weeks) / L (month+)

The strategic framing from the engagement: the routing engine (layer A) and real-time
platform (layer C) feed *into* this matrix, but the matrix scores the **whole application
suite** — portals, apps, safety, vendor, billing, MIS, integrations. Most procurement
losses happen on the unglamorous modules (vendor mgmt, billing, MIS, compliance docs),
not on routing. Score those honestly.

---

## 1. Employee / Rider Mobile App

| Feature | Status | Notes | Effort |
|---|---|---|---|
| Scheduled trip view (pickup time, vehicle, driver) | ✅ | Existing employee app | — |
| Live cab tracking + ETA | 🟡 | App tracks during live trip; formalize via real-time plane (layer C) | M |
| Roster / shift request, week schedule | 🟡 | Depends on roster module | M |
| Ad-hoc / spot booking | 🟡 | Confirm in current app | M |
| Cancellation / no-show marking | 🟡 | — | S |
| OTP-based trip start (rider↔driver verification) | 🟡 | Common RFP must-have; confirm | S |
| SOS / panic button | 🔴 | Safety hard-requirement for women-safety scoring | M |
| Driver rating & feedback | 🟡 | — | S |
| Share-my-ride / trip-share to guardian | 🔴 | Women-safety scoring item | M |
| Notifications (push/SMS) | 🟡 | — | S |

## 2. Driver Mobile App

| Feature | Status | Notes | Effort |
|---|---|---|---|
| Trip roster / manifest, turn-by-turn nav | ✅/🟡 | Existing driver app; confirm nav | — |
| Live GPS broadcast during trip | ✅ | This is the layer-C producer | — |
| Start/end trip, stop-reached, OTP at pickup | 🟡 | — | S |
| Passenger no-show marking | 🟡 | — | S |
| Adaptive ping (battery/bandwidth) | 🔴 | Needed at 2000+ scale (see arch doc) | S |
| SOS acknowledgement / emergency flow | 🔴 | — | M |
| Driver compliance doc display (DL, badge) | 🔴 | — | S |
| Multi-trip / rotation handling in-app | 🔴 | Ties to engine #11/#12 | M |

## 3. Admin / Operations Web Portal

| Feature | Status | Notes | Effort |
|---|---|---|---|
| Live fleet map (all active vehicles) | 🔴 | **Layer C Phase 1** — design doc written | L |
| Trip monitoring, plan-vs-actual, deviation alerts | 🔴 | Layer C | L |
| Roster/shift management, demand upload | 🔴 | Core eTMS module | L |
| Route plan review / manual override / re-route | 🟡 | Engine produces plans; needs ops UI | L |
| Escort/marshal assignment view | 🟡 | Guard logic in engine; needs UI | M |
| Exception/escalation console (delay, off-route, SOS) | 🔴 | Layer C ops plane | M |
| Geofence management (campus, zones) | 🔴 | — | M |
| RBAC / multi-role / multi-site admin | 🔴 | Multi-facility (BDC+DDC) requires it | M |

## 4. Routing & Scheduling Engine  *(your strongest layer — see routing constraints email)*

| Feature | Status | Notes | Effort |
|---|---|---|---|
| Auto route generation (VRP) | ✅ | OR-Tools v3 | — |
| Heterogeneous fleet, capacity & occupancy caps | ✅ | 4/6-7/12-seater | — |
| Pickup + drop trip types | ✅ | — | — |
| Women-safety: female-last-pickup / first-drop + guard | ✅ | `tryGuardSwap`, 1.5km | — |
| NMT / narrow-lane → small-vehicle routing | ✅ | `routeSpecialEmployees` | — |
| Medical/PWD grouping (max 2, same category) | ✅ | — | — |
| Traffic × distance route-deviation thresholds | ✅ | `lookupCdcRule` / CDC bands | — |
| Zone-based / zone-clubbing routing | ✅ | profile flags | — |
| Escort time-windows per city (night shift) | 🟡 | Guard exists; per-city windows not wired | S |
| Distance-banded max travel time (120/150 min) | 🟡 | `maxDuration` not yet distance-banded | S |
| Per-city avg speed (16 vs 24 km/h) | 🟡 | Inferred from shift buffers today | S |
| **Time-windowed fleet availability (12-seater windows)** | 🔴 | Static counts only | M |
| **Multi-trip scheduling (driver 8h, 5-6 rotations)** | 🔴 | Single-solve today; biggest engine gap | L |
| **Drop→pickup chaining (vehicle reuse to office)** | 🔴 | Part of multi-trip scheduling | L |
| Real-time/learned travel times in cost matrix | 🟡 | OSRM static; layer-C feed = Phase 3 | M |
| Dynamic re-optimization on disruption | 🔴 | Layer C Phase 4; job store ready | L |

## 5. Real-Time Tracking & Monitoring  *(layer C — arch doc written)*

| Feature | Status | Notes | Effort |
|---|---|---|---|
| Live vehicle positions, breadcrumb history | 🔴 | Phase 1 | L |
| Geofence enter/exit auto trip-state | 🔴 | — | M |
| Off-route / idle / speeding alerts | 🔴 | GPS-derived (no OBD) | M |
| ETA recompute from live position | 🔴 | — | M |
| Trip replay | 🔴 | from cold store | M |

## 6. Safety & Compliance (women-safety — heavily weighted in Indian corporate RFPs)

| Feature | Status | Notes | Effort |
|---|---|---|---|
| Female employee escort logic | ✅ | engine | — |
| No-female-first-pickup / last-drop rule | ✅ | engine | — |
| SOS/panic (rider + driver) → control room | 🔴 | must-have | M |
| Speed-limit / rash-driving alerts | 🔴 | GPS-derived | M |
| Geofence boundary alerts | 🔴 | — | M |
| Mask/CCTV/health (post-COVID legacy items) | 🔴 | often still in matrices | S |
| Audit trail of safety events | 🟡 | event log in layer C | M |

## 7. Vendor / Supplier Management  *(commonly underestimated — score honestly)*

| Feature | Status | Notes | Effort |
|---|---|---|---|
| Vendor onboarding, vehicle pools, SLAs | 🔴 | mixed owned+vendor fleet | L |
| Vehicle & driver document registry + expiry alerts | 🔴 | RC, insurance, PUC, permit, DL, police verification | M |
| Vendor allocation / quota by vehicle type | 🔴 | ties to fleet counts in email | M |
| Vendor performance scorecards | 🔴 | — | M |

## 8. Billing, Invoicing & Trip Costing

| Feature | Status | Notes | Effort |
|---|---|---|---|
| Trip cost computation (km/slab/package) | 🔴 | core procurement module | L |
| Vendor payout reconciliation | 🔴 | — | L |
| Cost-center / department allocation, employee recovery | 🔴 | — | M |
| Invoice generation / export | 🔴 | — | M |

## 9. MIS / Reporting / Analytics

| Feature | Status | Notes | Effort |
|---|---|---|---|
| Trip / occupancy / utilization reports | 🔴 | — | M |
| Cost & efficiency dashboards | 🔴 | — | M |
| Safety & compliance reports | 🔴 | — | M |
| Custom report builder / scheduled exports | 🔴 | — | L |
| Carbon / sustainability reporting | 🔴 | increasingly in RFPs | M |

## 10. Roster / Shift / Demand Management

| Feature | Status | Notes | Effort |
|---|---|---|---|
| Shift master, login-logout windows | 🔴 | drives planning | M |
| Demand upload / API from HRMS | 🔴 | — | M |
| Auto-roster from shift patterns | 🔴 | — | M |
| Weekly / recurring booking | 🔴 | — | M |

## 11. Integrations

| Feature | Status | Notes | Effort |
|---|---|---|---|
| SSO / Active Directory / SAML | 🔴 | enterprise must-have | M |
| HRMS / employee master sync | 🟡 | have employee data pipeline | M |
| Badge/swipe / access-control reconciliation | 🔴 | — | M |
| Maps (OSRM today; Google fallback) | ✅ | — | — |
| Notification gateways (SMS/email/push/WhatsApp/IVR) | 🟡 | — | M |
| Payroll / ERP export | 🔴 | — | M |

## 12. Platform / Non-functional

| Feature | Status | Notes | Effort |
|---|---|---|---|
| Multi-site / multi-city (BDC + DDC) | 🟡 | engine per-city; app multi-tenancy TBD | M |
| RBAC, audit logs | 🔴 | — | M |
| Scale to 2000+ vehicles | 🟡 | arch designed (layer C), not built | L |
| Data residency / security / DPDP compliance | 🟡 | confirm | M |
| SLA / uptime / DR | 🟡 | App Runner today | M |

---

## Scorecard summary (canonical taxonomy)

| Module | ✅ | 🟡 | 🔴 | Verdict |
|---|---|---|---|---|
| Routing engine | strong | few | 3 big | **Differentiator — lead with this** |
| Driver/Employee apps | base | many | some | Solid base, needs safety + real-time polish |
| Real-time tracking | — | — | most | Design ready, build pending (layer C) |
| Safety/women-safety | engine-side ✅ | — | SOS/alerts | Routing safety strong; app-side SOS gap |
| Vendor / Billing / MIS / Roster | — | — | most | **Where deals are lost — plan honestly** |
| Integrations / platform | maps ✅ | several | SSO etc. | Standard enterprise build-out |

### Recommended positioning for the matrix
1. **Lead with the routing engine** — your genuine differentiator. The constraint table
   in the 17-June email is ~70% already implemented; that's a strong demo.
2. **Be honest on vendor/billing/MIS/roster** — marking these "Available" when they're not
   invites a failed demo. "Available, need customization" is defensible where a real base
   exists; "Roadmap" beats a broken demo.
3. **Tell the real-time story with the arch doc** — even if unbuilt, a credible 2000+-vehicle
   architecture (layer C) scores on "scalability/roadmap" line items.
4. **Map women-safety carefully** — engine logic is strong, but app-side SOS/share-ride/
   speed-alerts are gaps that these RFPs weight heavily. Close the quick ones (S-effort) first.

## Next step
Provide `data/Transport New App Success matrix … .xlsx` → I will produce a row-aligned
mapping with your exact response columns filled, flag every "customization" claim that
needs a demo-ready proof point, and prioritize the gap-closures by (matrix weight × effort).

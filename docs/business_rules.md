# Business rules and metric definitions

Status: **defined before implementation** (Milestone 1). The generator, the expected-results
ledger, dbt models and reconciliation checks all implement these rules. If code and this
document disagree, that is a bug.

All data is synthetic. The fictional company sells physical goods online in one currency.

## Units and time

- Currency: USD only. Money is stored as **integer cents** (`bigint`). No floating point.
- Timestamps are `timestamptz` and are interpreted in **UTC**. A "day" is a UTC calendar date.
- Event time (when something happened) is distinguished from arrival time (when the source
  learned of it). Shipment events carry both (`event_at`, `received_at`).

## Orders and lines

- An order has 1..5 lines. Line amount = `quantity × unit_price_cents − discount_cents`,
  with `0 ≤ discount_cents ≤ quantity × unit_price_cents` (enforced in the source schema).
- `unit_price_cents` is the product's list price **at order time**; later price changes do not
  alter existing lines.
- Order total = sum of its line amounts.
- Order status lifecycle: `placed → paid → shipped → delivered`; terminal alternatives
  `cancelled` (only before any successful capture) and `refunded` (fully refunded after capture).
  Status in the source reflects state as of the simulation end.

## Payments and refunds

- An order has one or more payment attempts (`attempt_number`). Statuses:
  `pending`, `succeeded`, `failed`, `refunded`.
- At most one attempt per order captures money. A captured attempt has `captured_at` and
  `amount_cents` = order total. `refunded` means the captured payment was **fully** refunded.
- Refunds are separate rows (`shop.refunds`) linked to the captured payment. A payment may have
  several partial refunds; the sum of its refunds never exceeds its captured amount.
- A partially refunded payment keeps status `succeeded`.
- Capture can happen days after the order is placed (late-arriving payments).

## Revenue metrics (mart_daily_revenue, grain: one row per UTC date)

| Metric | Definition | Date attribution |
|---|---|---|
| `orders_placed` | count of orders placed | `orders.placed_at` |
| `orders_cancelled` | count of orders placed that day that are cancelled | `orders.placed_at` |
| `booked_revenue_cents` | sum of line amounts for orders placed that day, **excluding cancelled orders** | `orders.placed_at` |
| `captured_cents` | sum of `amount_cents` of captured payments (status `succeeded` or `refunded`) | `payments.captured_at` |
| `refunded_cents` | sum of `refunds.amount_cents` | `refunds.refunded_at` |
| `net_collected_cents` | `captured_cents − refunded_cents` | per row |

Why two bases: booked revenue answers "what did customers commit to on that day"; net
collected is cash basis and is what payment reconciliation compares. Keeping them on separate
date attributions, and computing each from a single fact grain, avoids double counting when
orders, items, payments and refunds are joined. **Never** sum line amounts after joining to
payments or refunds.

Booked revenue of fully refunded orders is still booked on the order date; the refund reduces
`net_collected_cents` on the refund date.

## Inventory

Each movement carries two deltas:

| movement_type | on_hand_delta | allocated_delta | When |
|---|---|---|---|
| `restock` | +q | 0 | replenishment |
| `allocation` | 0 | +q | payment captured (stock reserved for the order) |
| `allocation_release` | 0 | −q | order fully refunded before shipping |
| `shipment` | −q | −q | shipment created (reserved stock leaves the building) |

Position per product and warehouse: `on_hand = Σ on_hand_delta`, `allocated = Σ allocated_delta`,
`available = on_hand − allocated`. The generator never lets `available` go negative.
Refunds after shipment do not return stock (returns handling is out of scope).

## Shipments

- One shipment per captured, non-refunded-before-shipping order, from one warehouse.
- Events in order: `label_created, picked_up, in_transit, out_for_delivery, delivered`.
- Some events reach the source late (`received_at` hours after `event_at`), so arrival order
  can differ from event order.

## Known simplifications

- Single currency; no taxes or shipping fees.
- No returns or restocking from refunds.
- Batch extraction cannot observe hard deletes (CDC in the full profile can).

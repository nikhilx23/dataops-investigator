-- Operational source schema for the synthetic commerce company.
-- Money: integer minor units (USD cents). Timestamps: timestamptz, stored and compared in UTC.
-- updated_at drives incremental batch extraction; the trigger below maintains it on UPDATE
-- unless the writer sets it explicitly (the generator writes simulated times).

CREATE TABLE shop.customers (
    customer_id     bigint PRIMARY KEY,
    email           text        NOT NULL UNIQUE,
    full_name       text        NOT NULL,
    country_code    char(2)     NOT NULL,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);

CREATE TABLE shop.warehouses (
    warehouse_id    integer PRIMARY KEY,
    code            text        NOT NULL UNIQUE,
    name            text        NOT NULL,
    region          text        NOT NULL,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);

CREATE TABLE shop.products (
    product_id        bigint PRIMARY KEY,
    sku               text        NOT NULL UNIQUE,
    name              text        NOT NULL,
    category          text        NOT NULL,
    list_price_cents  bigint      NOT NULL CHECK (list_price_cents >= 0),
    is_active         boolean     NOT NULL DEFAULT true,
    created_at        timestamptz NOT NULL,
    updated_at        timestamptz NOT NULL
);

CREATE TABLE shop.orders (
    order_id        bigint PRIMARY KEY,
    customer_id     bigint      NOT NULL REFERENCES shop.customers,
    status          text        NOT NULL CHECK (status IN
                        ('placed','paid','shipped','delivered','cancelled','refunded')),
    currency        char(3)     NOT NULL DEFAULT 'USD' CHECK (currency = 'USD'),
    placed_at       timestamptz NOT NULL,
    cancelled_at    timestamptz,
    updated_at      timestamptz NOT NULL
);

CREATE TABLE shop.order_items (
    order_item_id     bigint PRIMARY KEY,
    order_id          bigint      NOT NULL REFERENCES shop.orders,
    line_number       integer     NOT NULL,
    product_id        bigint      NOT NULL REFERENCES shop.products,
    quantity          integer     NOT NULL CHECK (quantity > 0),
    unit_price_cents  bigint      NOT NULL CHECK (unit_price_cents >= 0),
    discount_cents    bigint      NOT NULL DEFAULT 0 CHECK (discount_cents >= 0),
    created_at        timestamptz NOT NULL,
    updated_at        timestamptz NOT NULL,
    UNIQUE (order_id, line_number),
    CHECK (discount_cents <= quantity * unit_price_cents)
);

CREATE TABLE shop.payments (
    payment_id      bigint PRIMARY KEY,
    order_id        bigint      NOT NULL REFERENCES shop.orders,
    attempt_number  integer     NOT NULL CHECK (attempt_number >= 1),
    status          text        NOT NULL CHECK (status IN ('pending','succeeded','failed','refunded')),
    method          text        NOT NULL,
    amount_cents    bigint      NOT NULL CHECK (amount_cents > 0),
    created_at      timestamptz NOT NULL,
    captured_at     timestamptz,
    failure_reason  text,
    updated_at      timestamptz NOT NULL,
    UNIQUE (order_id, attempt_number),
    CHECK ((status IN ('succeeded','refunded')) = (captured_at IS NOT NULL))
);

CREATE TABLE shop.refunds (
    refund_id       bigint PRIMARY KEY,
    payment_id      bigint      NOT NULL REFERENCES shop.payments,
    amount_cents    bigint      NOT NULL CHECK (amount_cents > 0),
    reason          text        NOT NULL,
    refunded_at     timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);

CREATE TABLE shop.inventory_movements (
    movement_id         bigint PRIMARY KEY,
    product_id          bigint      NOT NULL REFERENCES shop.products,
    warehouse_id        integer     NOT NULL REFERENCES shop.warehouses,
    movement_type       text        NOT NULL CHECK (movement_type IN
                            ('restock','allocation','allocation_release','shipment')),
    on_hand_delta       integer     NOT NULL,
    allocated_delta     integer     NOT NULL,
    reference_order_id  bigint      REFERENCES shop.orders,
    occurred_at         timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL
);

CREATE TABLE shop.shipments (
    shipment_id     bigint PRIMARY KEY,
    order_id        bigint      NOT NULL UNIQUE REFERENCES shop.orders,
    warehouse_id    integer     NOT NULL REFERENCES shop.warehouses,
    carrier         text        NOT NULL,
    tracking_number text        NOT NULL UNIQUE,
    status          text        NOT NULL CHECK (status IN
                        ('label_created','picked_up','in_transit','out_for_delivery','delivered')),
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL
);

CREATE TABLE shop.shipment_events (
    shipment_event_id  bigint PRIMARY KEY,
    shipment_id        bigint      NOT NULL REFERENCES shop.shipments,
    event_type         text        NOT NULL CHECK (event_type IN
                           ('label_created','picked_up','in_transit','out_for_delivery','delivered')),
    event_at           timestamptz NOT NULL,   -- when it happened (event time)
    received_at        timestamptz NOT NULL,   -- when the source learned of it (may be late)
    updated_at         timestamptz NOT NULL,
    CHECK (received_at >= event_at)
);

-- Incremental-extraction indexes: (updated_at, pk) is the stable pagination key.
CREATE INDEX ON shop.customers (updated_at, customer_id);
CREATE INDEX ON shop.warehouses (updated_at, warehouse_id);
CREATE INDEX ON shop.products (updated_at, product_id);
CREATE INDEX ON shop.orders (updated_at, order_id);
CREATE INDEX ON shop.order_items (updated_at, order_item_id);
CREATE INDEX ON shop.payments (updated_at, payment_id);
CREATE INDEX ON shop.refunds (updated_at, refund_id);
CREATE INDEX ON shop.inventory_movements (updated_at, movement_id);
CREATE INDEX ON shop.shipments (updated_at, shipment_id);
CREATE INDEX ON shop.shipment_events (updated_at, shipment_event_id);

CREATE FUNCTION shop.touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at THEN
        NEW.updated_at := clock_timestamp();
    END IF;
    RETURN NEW;
END $$;

DO $$
DECLARE t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['customers','warehouses','products','orders','order_items','payments',
                             'refunds','inventory_movements','shipments','shipment_events'] LOOP
        EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE ON shop.%I '
                       'FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at()', t || '_touch', t);
    END LOOP;
END $$;

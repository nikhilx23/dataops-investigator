-- Logical replication publication for Debezium (full profile).
-- Owned by src_app (table owner), so the CDC role never needs ownership or superuser.
-- Every shop table except none: the product change log is published too, so the CDC path
-- receives SCD2 versions in commit order exactly like the batch path.
-- Requires wal_level=logical to emit changes; on a server with wal_level=replica the
-- publication exists but carries nothing (the lightweight profile does not use it).
CREATE PUBLICATION dataops_cdc FOR TABLE
    shop.warehouses, shop.customers, shop.products, shop.product_versions, shop.orders,
    shop.order_items, shop.payments, shop.refunds, shop.inventory_movements, shop.shipments,
    shop.shipment_events
    WITH (publish = 'insert, update, delete, truncate');

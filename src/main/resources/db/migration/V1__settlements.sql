-- payments-worker es dueño de esta tabla dentro de la base `payments` (las convenciones internas de API), que es
-- propiedad de payments-api. payment_id es unique: es la clave de idempotencia del consumer
-- (mismo evento dos veces no duplica). IF NOT EXISTS porque compartimos la base con otro dueño:
-- no queremos que nuestra migración falle si alguien ya creó algo con este nombre a mano.
CREATE TABLE IF NOT EXISTS settlements (
    id uuid PRIMARY KEY,
    payment_id uuid NOT NULL,
    settled_at timestamptz NOT NULL,
    batch_ref text
);

CREATE UNIQUE INDEX IF NOT EXISTS settlements_payment_id_key ON settlements (payment_id);

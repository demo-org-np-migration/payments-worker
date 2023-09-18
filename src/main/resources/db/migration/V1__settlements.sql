-- payments-worker es dueño de esta tabla dentro de la base `payments` (las convenciones internas de API).
-- payment_id es unique: es la clave de idempotencia del consumer (mismo evento dos veces no duplica).
CREATE TABLE settlements (
    id uuid PRIMARY KEY,
    payment_id uuid NOT NULL UNIQUE,
    settled_at timestamptz NOT NULL,
    batch_ref text
);

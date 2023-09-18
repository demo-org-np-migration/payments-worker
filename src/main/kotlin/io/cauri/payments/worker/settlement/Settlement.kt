package io.cauri.payments.worker.settlement

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant
import java.util.UUID

/** Fila de `settlements` en la base `payments` (las convenciones internas de API). `payment_id` es unique: es la
 *  clave de idempotencia con la que el worker decide si ya procesó un `payments.completed`. */
@Entity
@Table(name = "settlements")
class Settlement(
    @Id
    val id: UUID,

    @Column(name = "payment_id", nullable = false, unique = true)
    val paymentId: UUID,

    @Column(name = "settled_at", nullable = false)
    val settledAt: Instant,

    @Column(name = "batch_ref")
    val batchRef: String?,
)

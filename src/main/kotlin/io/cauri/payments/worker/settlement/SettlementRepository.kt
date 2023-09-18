package io.cauri.payments.worker.settlement

import org.springframework.data.jpa.repository.JpaRepository
import java.util.UUID

interface SettlementRepository : JpaRepository<Settlement, UUID> {
    fun existsByPaymentId(paymentId: UUID): Boolean
}

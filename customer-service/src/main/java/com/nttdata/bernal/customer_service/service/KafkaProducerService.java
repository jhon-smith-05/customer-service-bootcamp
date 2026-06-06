package com.nttdata.bernal.customer_service.service;

import com.nttdata.bernal.customer_service.model.event.AuditEvent;
import com.nttdata.bernal.customer_service.model.event.FraudAlertEvent;
import com.nttdata.bernal.customer_service.model.event.NotificationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

    private final StreamBridge streamBridge;

    public static final String BINDING_NOTIFICATIONS = "banking-notifications-out-0";
    public static final String BINDING_AUDIT         = "audit-events-out-0";
    public static final String BINDING_FRAUD         = "fraud-alerts-out-0";

    public void sendNotification(NotificationEvent event) {
        send(BINDING_NOTIFICATIONS, event);
    }

    public void sendAuditEvent(AuditEvent event) {
        send(BINDING_AUDIT, event);
    }

    public void sendFraudAlert(FraudAlertEvent event) {
        send(BINDING_FRAUD, event);
    }

    private void send(String binding, Object payload) {
        boolean sent = streamBridge.send(binding, payload);
        if (sent) {
            log.info("Event sent to binding {}: {}", binding, payload);
        } else {
            log.error("Failed to send event to binding {}", binding);
        }
    }
}

package com.nttdata.bernal.customer_service.service;

import com.nttdata.bernal.customer_service.exception.BusinessRuleException;
import com.nttdata.bernal.customer_service.exception.ResourceNotFoundException;
import com.nttdata.bernal.customer_service.model.Customer;
import com.nttdata.bernal.customer_service.model.CustomerType;
import com.nttdata.bernal.customer_service.model.event.AuditEvent;
import com.nttdata.bernal.customer_service.model.event.NotificationEvent;
import com.nttdata.bernal.customer_service.repository.CustomerRepository;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.circuitbreaker.ReactiveCircuitBreakerFactory;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Service;
import reactor.adapter.rxjava.RxJava3Adapter;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class CustomerService {

    @Value("${business.rules.unique-document:true}")
    private boolean uniqueDocument;

    @Value("${business.rules.max-customers-per-day:100}")
    private int maxCustomersPerDay;

    private final CustomerRepository customerRepository;
    private final KafkaProducerService kafkaProducer;
    private final ReactiveCircuitBreakerFactory circuitBreakerFactory;

    public Single<Customer> create(Customer customer) {
        log.debug("Creating customer with document number {}", customer.getDocumentNumber());

        Mono<Customer> result = customerRepository.existsByDocumentNumber(customer.getDocumentNumber())
                .flatMap(exists -> {
                    if (exists && uniqueDocument) {
                        return Mono.error(new BusinessRuleException("Customer document number already exists"));
                    }
                    return customerRepository.save(customer);
                })
                .doOnSuccess(saved -> {

                    kafkaProducer.sendNotification(NotificationEvent.builder()
                            .customerId(saved.getId())
                            .type("INFO")
                            .channel("EMAIL")
                            .message("Bienvenido a BankPlatform, " + saved.getName())
                            .timestamp(LocalDateTime.now())
                            .build());

                    kafkaProducer.sendAuditEvent(AuditEvent.builder()
                            .eventId(UUID.randomUUID().toString())
                            .serviceOrigin("customer-service")
                            .action("CREATE")
                            .entityId(saved.getId())
                            .timestamp(LocalDateTime.now())
                            .metadata(Map.of(
                                    "name", saved.getName(),
                                    "documentNumber", saved.getDocumentNumber(),
                                    "type", saved.getType().toString()
                            ))
                            .build());
                })
                .doOnError(e -> log.error("Error creating customer: {}", e.getMessage()));

        return RxJava3Adapter.monoToSingle(result);
    }

    public Flowable<Customer> findAll() {
        return RxJava3Adapter.fluxToFlowable(customerRepository.findAll());
    }

    public Single<Customer> findById(String id) {
        return RxJava3Adapter.monoToSingle(
                circuitBreakerFactory.create("customer-service")
                        .run(
                                findCustomer(id),
                                throwable -> {
                                    log.error("Circuit breaker activated for findById: {}",
                                            throwable.getMessage());
                                    return Mono.error(new ResourceNotFoundException(
                                            "Customer service temporarily unavailable"));
                                }
                        )
        );
    }

    public Single<Customer> update(String id, Customer customer) {
        Mono<Customer> result = findCustomer(id)
                .flatMap(current -> {
                    current.setDocumentNumber(customer.getDocumentNumber());
                    current.setName(customer.getName());
                    current.setEmail(customer.getEmail());
                    current.setPhone(customer.getPhone());
                    current.setType(customer.getType());
                    return customerRepository.save(current);
                })
                .doOnSuccess(updated -> {

                    kafkaProducer.sendAuditEvent(AuditEvent.builder()
                            .eventId(UUID.randomUUID().toString())
                            .serviceOrigin("customer-service")
                            .action("UPDATE")
                            .entityId(updated.getId())
                            .timestamp(LocalDateTime.now())
                            .metadata(Map.of(
                                    "name", updated.getName(),
                                    "documentNumber", updated.getDocumentNumber(),
                                    "type", updated.getType().toString()
                            ))
                            .build());
                })
                .doOnError(e -> log.error("Error updating customer {}: {}", id, e.getMessage()));

        return RxJava3Adapter.monoToSingle(result);
    }

    public Completable delete(String id) {
        Mono<Void> result = findCustomer(id)
                .flatMap(customer -> customerRepository.delete(customer)
                        .doOnSuccess(v -> {

                            kafkaProducer.sendAuditEvent(AuditEvent.builder()
                                    .eventId(UUID.randomUUID().toString())
                                    .serviceOrigin("customer-service")
                                    .action("DELETE")
                                    .entityId(id)
                                    .timestamp(LocalDateTime.now())
                                    .metadata(Map.of(
                                            "deletedId", id,
                                            "name", customer.getName(),
                                            "documentNumber", customer.getDocumentNumber()
                                    ))
                                    .build());
                        })
                )
                .doOnError(e -> log.error("Error deleting customer {}: {}", id, e.getMessage()));

        return RxJava3Adapter.monoToCompletable(result);
    }

    private Mono<Customer> findCustomer(String id) {
        return customerRepository.findById(id)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("Customer not found")));
    }

    // Buscar clientes por tipo usando Streams
    public Flowable<Customer> findByType(CustomerType type) {
        return RxJava3Adapter.fluxToFlowable(
                customerRepository.findAll()
                        .filter(c -> c.getType() == type)
                        .sort(Comparator.comparing(Customer::getName))
        );
    }

    // Agrupar clientes por tipo
    public Single<Map<CustomerType, List<Customer>>> groupByType() {
        return RxJava3Adapter.fluxToFlowable(customerRepository.findAll())
                .toList()
                .map(customers -> customers.stream()
                        .collect(Collectors.groupingBy(Customer::getType))
                );
    }

    // Buscar con múltiples filtros
    public Flowable<Customer> search(String name, CustomerType type) {
        return RxJava3Adapter.fluxToFlowable(
                customerRepository.findAll()
                        .filter(c -> (name == null || c.getName()
                                .toLowerCase().contains(name.toLowerCase()))
                                && (type == null || c.getType() == type))
                        .sort(Comparator.comparing(Customer::getName))
        );
    }
}

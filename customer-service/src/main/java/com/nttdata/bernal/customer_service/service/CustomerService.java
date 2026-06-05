package com.nttdata.bernal.customer_service.service;

import com.nttdata.bernal.customer_service.exception.BusinessRuleException;
import com.nttdata.bernal.customer_service.exception.ResourceNotFoundException;
import com.nttdata.bernal.customer_service.model.Customer;
import com.nttdata.bernal.customer_service.repository.CustomerRepository;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Service;
import reactor.adapter.rxjava.RxJava3Adapter;
import reactor.core.publisher.Mono;

@Slf4j
@Service
@RefreshScope
@RequiredArgsConstructor
public class CustomerService {

    @Value("${business.rules.unique-document:true}")
    private boolean uniqueDocument;

    @Value("${business.rules.max-customers-per-day:100}")
    private int maxCustomersPerDay;

    private final CustomerRepository customerRepository;

    public Single<Customer> create(Customer customer) {
        log.debug("Creating customer with document number {}", customer.getDocumentNumber());
        Mono<Customer> result = customerRepository.existsByDocumentNumber(customer.getDocumentNumber())
                .flatMap(exists -> {
                    if (exists) {
                        return Mono.error(new BusinessRuleException("Customer document number already exists"));
                    }
                    return customerRepository.save(customer);
                });
        return RxJava3Adapter.monoToSingle(result);
    }

    public Flowable<Customer> findAll() {
        return RxJava3Adapter.fluxToFlowable(customerRepository.findAll());
    }

    public Single<Customer> findById(String id) {
        return RxJava3Adapter.monoToSingle(findCustomer(id));
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
                });
        return RxJava3Adapter.monoToSingle(result);
    }

    public Completable delete(String id) {
        return RxJava3Adapter.monoToCompletable(
                findCustomer(id).flatMap(customerRepository::delete)
        );
    }

    private Mono<Customer> findCustomer(String id) {
        return customerRepository.findById(id)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("Customer not found")));
    }
}

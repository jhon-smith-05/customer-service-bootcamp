package com.nttdata.bernal.customer_service.controller;

import com.nttdata.bernal.customer_service.model.Customer;
import com.nttdata.bernal.customer_service.service.CustomerService;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/customers")
@RequiredArgsConstructor
public class CustomerController {

    private final CustomerService customerService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Single<Customer> create(@RequestBody Customer customer) {
        return customerService.create(customer);
    }

    @GetMapping
    public Flowable<Customer> findAll() {
        return customerService.findAll();
    }

    @GetMapping("/{id}")
    public Single<Customer> findById(@PathVariable("id") String id) {
        return customerService.findById(id);
    }

    @PutMapping("/{id}")
    public Single<Customer> update(@PathVariable("id") String id, @RequestBody Customer customer) {
        return customerService.update(id, customer);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Completable delete(@PathVariable("id") String id) {
        return customerService.delete(id);
    }
}

package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.UserRecord;
import org.springframework.data.repository.CrudRepository;

public interface UserRepository extends CrudRepository<UserRecord, Long> {
    // Inherits findById(Long id) from CrudRepository which returns Optional<UserRecord>
}

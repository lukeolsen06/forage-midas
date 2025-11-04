package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.UserRecord;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface UserRepository extends JpaRepository<UserRecord, Long> {
    // Inherits findById(Long id) from CrudRepository which returns Optional<UserRecord>
    Optional<UserRecord> findByName(String name);
    
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT u FROM UserRecord u WHERE u.id = :id")
    Optional<UserRecord> findByIdWithLock(@Param("id") Long id);
}

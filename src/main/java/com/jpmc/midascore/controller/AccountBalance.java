package com.jpmc.midascore.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.component.DatabaseConduit;
import org.springframework.beans.factory.annotation.Autowired;
import com.jpmc.midascore.entity.UserRecord;
import java.util.Optional;

@RestController
@RequestMapping("/balance")
public class AccountBalance {

    @Autowired
    DatabaseConduit databaseConduit;

    @GetMapping("")
    public Balance getBalance(@RequestParam("userId") Long userId) {
        Optional<UserRecord> user = databaseConduit.findUserById(userId);
        if (user.isPresent()) {
            float balance = user.get().getBalance();
            return new Balance(balance);
        }
        else {
            return new Balance(0.0f);
        }
    }

}

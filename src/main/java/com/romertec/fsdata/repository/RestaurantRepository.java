package com.romertec.fsdata.repository;

import com.romertec.fsdata.entity.Restaurant;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RestaurantRepository extends JpaRepository<Restaurant, Integer> {
}

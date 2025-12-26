package com.romertec.fsdata.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "menu")
public class Menu {

    @Id
    private Integer id;

    @Column(name = "category")
    private String category;

    @Column(name = "description")
    private String description;

    @Column(name = "name")
    private String name;

    @Column(name = "price")
    private String price;

    @Column(name = "restaurant_id")
    private Integer restaurantId;

    // Relación opcional (solo lectura) si la necesitas en el futuro.
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "restaurant_id", insertable = false, updatable = false)
    private Restaurant restaurant;

    // Getters/Setters

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getPrice() { return price; }
    public void setPrice(String price) { this.price = price; }

    public Integer getRestaurantId() { return restaurantId; }
    public void setRestaurantId(Integer restaurantId) { this.restaurantId = restaurantId; }

    public Restaurant getRestaurant() { return restaurant; }
    public void setRestaurant(Restaurant restaurant) { this.restaurant = restaurant; }
}

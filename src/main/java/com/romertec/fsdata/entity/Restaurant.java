package com.romertec.fsdata.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "restaurants")
public class Restaurant {

    @Id
    private Integer id;

    @Column(name = "category")
    private String category;

    @Column(name = "city")
    private String city;

    @Column(name = "lat")
    private String lat;

    @Column(name = "lng")
    private String lng;

    @Column(name = "name")
    private String name;

    @Column(name = "position")
    private String position;

    @Column(name = "price_range")
    private String priceRange;

    // OJO: la columna en tu tabla se llama `raitings`
    @Column(name = "raitings")
    private String raitings;

    @Column(name = "score")
    private String score;

    @Column(name = "state")
    private String state;

    @Column(name = "street")
    private String street;

    @Column(name = "unit")
    private String unit;

    @Column(name = "zip")
    private String zip;

    // Getters/Setters

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getCity() { return city; }
    public void setCity(String city) { this.city = city; }

    public String getLat() { return lat; }
    public void setLat(String lat) { this.lat = lat; }

    public String getLng() { return lng; }
    public void setLng(String lng) { this.lng = lng; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getPosition() { return position; }
    public void setPosition(String position) { this.position = position; }

    public String getPriceRange() { return priceRange; }
    public void setPriceRange(String priceRange) { this.priceRange = priceRange; }

    public String getRaitings() { return raitings; }
    public void setRaitings(String raitings) { this.raitings = raitings; }

    public String getScore() { return score; }
    public void setScore(String score) { this.score = score; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public String getStreet() { return street; }
    public void setStreet(String street) { this.street = street; }

    public String getUnit() { return unit; }
    public void setUnit(String unit) { this.unit = unit; }

    public String getZip() { return zip; }
    public void setZip(String zip) { this.zip = zip; }
}

package com.romertec.fsdata.support;

public class RestaurantCsvRow {

    private Integer id;
    private String position;
    private String name;
    private String score;
    private String ratings;
    private String category;
    private String priceRange;
    private String fullAddress;
    private String zipCode;
    private String lat;
    private String lng;

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public String getPosition() { return position; }
    public void setPosition(String position) { this.position = position; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getScore() { return score; }
    public void setScore(String score) { this.score = score; }

    public String getRatings() { return ratings; }
    public void setRatings(String ratings) { this.ratings = ratings; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getPriceRange() { return priceRange; }
    public void setPriceRange(String priceRange) { this.priceRange = priceRange; }

    public String getFullAddress() { return fullAddress; }
    public void setFullAddress(String fullAddress) { this.fullAddress = fullAddress; }

    public String getZipCode() { return zipCode; }
    public void setZipCode(String zipCode) { this.zipCode = zipCode; }

    public String getLat() { return lat; }
    public void setLat(String lat) { this.lat = lat; }

    public String getLng() { return lng; }
    public void setLng(String lng) { this.lng = lng; }
}

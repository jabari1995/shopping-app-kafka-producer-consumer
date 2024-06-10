package com.shopping.kafka.model;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents a product.
 */
@Getter
@Setter

public class Product {
    private  int id;
    private  String name;
    private  double price;
    private  String imgUrl;

    /**
     * Default constructor for the Product class.
     * Creates a new instance of the Product class.
     */
    public Product() {}

    /**
     * Represents a product.
     *
     * @param id the unique identifier of the product
     * @param name the name of the product
     * @param price the price of the product
     * @param imgUrl the URL of the product image
     */
    public Product(int id, String name, double price, String imgUrl) {
        this.id = id;
        this.name = name;
        this.price = price;
        this.imgUrl = imgUrl;
    }

    /**
     * Compares this Product object to the specified object. The result is true if and only if the argument is not null and is a Product object that has the same id, price, name,
     * and imgUrl as this object.
     *
     * @param o the object to compare to this Product object
     * @return true if the given object is a Product object with the same id, price, name, and imgUrl; false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Product product = (Product) o;
        return id == product.id &&
                Double.compare(product.price, price) == 0 &&
                Objects.equals(name, product.name) &&
                Objects.equals(imgUrl, product.imgUrl);
    }

    /**
     * Computes the hash code for this Product object.
     *
     * @return the computed hash code for this Product object
     */
    @Override
    public int hashCode() {
        return Objects.hash(id, name, price, imgUrl);
    }
}
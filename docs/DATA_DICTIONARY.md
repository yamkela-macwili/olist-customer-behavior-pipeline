# Data Dictionary: Olist Brazilian E-Commerce Dataset

### Source: [Kaggle: Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
9 CSV files, joined on `order_id` / `customer_id` / `product_id` / `seller_id`.
---

## `olist_customers_dataset.csv`
| Column | Notes |
|---|---|
| customer_id | Order-level key, unique per order, not per person |
| customer_unique_id | Actual unique customer identifier (use this for RFM, not customer_id) |
| customer_zip_code_prefix | |
| customer_city | |
| customer_state | |

## `olist_orders_dataset.csv`
| Column | Notes |
|---|---|
| order_id | Primary key |
| customer_id | FK → customers |
| order_status | delivered, shipped, canceled, etc., filter for analysis scope |
| order_purchase_timestamp | |
| order_approved_at | |
| order_delivered_carrier_date | |
| order_delivered_customer_date | Can be null, cancelled/undelivered orders |
| order_estimated_delivery_date | Used for delivery delay calculation |

## `olist_order_items_dataset.csv`
| Column | Notes |
|---|---|
| order_id | FK → orders |
| order_item_id | Line item number within the order |
| product_id | FK → products |
| seller_id | FK → sellers |
| shipping_limit_date | |
| price | |
| freight_value | |

## `olist_order_payments_dataset.csv`
| Column | Notes |
|---|---|
| order_id | FK → orders |
| payment_sequential | An order can have multiple payment entries |
| payment_type | credit_card, boleto, voucher, etc. |
| payment_installments | |
| payment_value | |

## `olist_order_reviews_dataset.csv`
| Column | Notes |
|---|---|
| review_id | |
| order_id | FK → orders |
| review_score | 1–5 |
| review_comment_title | Often null |
| review_comment_message | Often null, Portuguese text |
| review_creation_date | |
| review_answer_timestamp | |

## `olist_products_dataset.csv`
| Column | Notes |
|---|---|
| product_id | Primary key |
| product_category_name | Portuguese, join with translation table |
| product_name_lenght | (sic, typo in source dataset, keep as-is) |
| product_description_lenght | (sic) |
| product_photos_qty | |
| product_weight_g | |
| product_length_cm / height_cm / width_cm | |

## `olist_sellers_dataset.csv`
| Column | Notes |
|---|---|
| seller_id | Primary key |
| seller_zip_code_prefix | |
| seller_city | |
| seller_state | |

## `olist_geolocation_dataset.csv`
| Column | Notes |
|---|---|
| geolocation_zip_code_prefix | Many-to-many with actual addresses, not a clean join key |
| geolocation_lat / geolocation_lng | |
| geolocation_city | |
| geolocation_state | |

## `product_category_name_translation.csv`
| Column | Notes |
|---|---|
| product_category_name | Portuguese, joins to products |
| product_category_name_english | English translation |

---

## Known data quality issues to handle explicitly (not silently)

- `order_delivered_customer_date` is null for cancelled/undelivered orders, decide how these are treated in delivery-experience features (exclude vs. flag).
- `customer_id` vs `customer_unique_id`, using the wrong one will silently break RFM (every order looks like a new customer).
- Review comments are largely in Portuguese, relevant if any sentiment analysis is added later.
- `geolocation` is not 1:1 with zip prefix, don't naively join it without deduplication.

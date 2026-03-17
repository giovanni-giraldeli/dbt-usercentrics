# dbt-usercentrics

This dbt project transforms OLTP backend data from Usercentrics into a star schema data model for OLAP analytics. The project is structured into three layers: staging, intermediary, and data mart.

## Project Structure

-   **models/staging**: This layer contains the initial data ingestion from the sources. The data is cleaned and lightly transformed to be used in the downstream models.
-   **models/intermediary**: This layer contains intermediary transformations that are not yet ready for the final data mart. This layer is used to create the dimension and fact tables.
-   **models/data_mart**: This layer contains the final star schema data model, which is composed of dimension and fact tables.

## Data Models

### Conceptual Data Model
The target is to have a Star Schema data model with a SCD Type 1 customer table to reference the current state of the customers, but as well provide historical information to support historical analysis for the company.
![Star Schema ERD](assets/Star%20Schema%20ERD.png)

The final data model is a star schema composed of the following tables:

### Dimensions

-   **dim_date**: Contains a list of dates and their relevant date parts.
-   **dim_countries**: Contains a list of countries with standardized naming convention.
-   **dim_customers**: Contains the current information about customers.
-   **dim_product_specs**: Contains information about product specifications.
-   **dim_customers_history**: Contains the history of customer profile changes.
-   **dim_customers_domain_groups_bridge**: Bridge table between customers and domain groups.
-   **dim_customers_domain_groups_history_bridge**: Historical bridge table between customers and domain groups.

### Facts

-   **fact_domains**: Contains facts about domains.

### Cubes

-   **cube_customers_snapshots_last_24m**: Contains relevant information about customers and their usage for the last 24 months.

### Semantic Layer

-   **snapshot_metrics_last_24m**: Contains relevant metrics pre-calculated for each month in the last 24 months.

## Running the project

To run this project, you will need to have dbt installed and configured. You can find more information about how to do that in the [dbt documentation](https://docs.getdbt.com/docs/introduction).

Once you have dbt configured, you can run the project using the following command:

```bash
dbt run
```

This will run all the models in the project and create the final data mart tables in your data warehouse.

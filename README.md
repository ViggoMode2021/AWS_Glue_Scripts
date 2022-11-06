# AWS_Glue_Scripts

Glue Job 1 - Take data from an extrapolated CSV file in an 23 bucket that is stored in a Glue database via a crawler and process an ETL job. First, 
Glue will change the schema by renaming 2 columns and dropping a few columns as well. Second, Glue will drop NULL fields. Third, Glue will run a custom query to order 
results by price ascending and limit to 10. Lastly, the data will be sent to the RDS table. 

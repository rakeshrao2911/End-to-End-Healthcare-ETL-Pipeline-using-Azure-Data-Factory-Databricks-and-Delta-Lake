# End-to-End-Healthcare-ETL-Pipeline-using-Azure-Data-Factory-Databricks-and-Delta-Lake

In this project, a scalable automated ETL pipeline is built to load healthcare data (EMR, Claims, CPT, NPI, ICD) data from various sources into Azure Data Lake using Azure Data Factory.
Azure Data Lake (ADLS Gen2) is used for storage and data is processed with Delta Lake using Azure Databricks. The data is organized into three layers (Bronze, Silver and Gold) following the Medallion architecture.

**What problem I solved using this project ?**

The problem was that health care data from two different hosipitals existed in multiple different systems (SQL database , ADLS Gen2 data lake and public web API's). There was no centralized system to easily access data for both analytics and machine learning purposes. I built an automated , scalable ETL pipleline that brought all of this data into Azure Data Lake. Organized this data following the Medallion architecture into three different layers - bronze , gold and silver. All the raw data from different sources is kept in bronze layer. 
Cleaned data is moved from bronze layer to silver layer applying data quality checks and implementing Slowly Changing Dimension Type 2. This cleaned data from silver layer can be used for machine learning purposes. Data from silver is modelled using star and snowflake schema and moved into gold layer. This cleaned and modelled data can be used for analytics purpose.

**Project Architecture**

<img width="807" height="501" alt="image" src="https://github.com/user-attachments/assets/4ee319aa-4845-4124-93f2-ea48ae77fa76" />



An end-to-end scalable pipeline is implemented to load healthcare data and process it using Azure Data facory, Azure Data Lake Gen2, Azure Databricks and Delta following Medallion architecture.

**Tools & Technologoes Used:**

**Azure Data Factory:** ADF is used to orchestrate data movement (full and incremental load) from sources to bronze layer by building and implenting configuration driven pipelines.

**Azure SQL Database:** Azure SQL database is source for EMR data from both hosipitals.

**Azure Data Lake Storage Gen2 (ADLS Gen2)**: It stores raw data, cleaned data and modelled data in three different layers. Claims and CPT data is uploaded to landing folder in ALDS Gen2. Configuration file which is used to configure pipeline in ADF is stored in config folder in ADLS Gen2.

**Azure Databricks + Delta Lake:** Databricks with delta lake is used to move data by applying data quality checks and implementind SCD type2 between different layers in ADLS Gen2.

**Azure Key vault:** It is used to store secrets. These secrets stored in key vault are used within Azure Databricks and Azure Data Factory in different scenarios.

**Web API:** It is used to fetch NPI and ICD data from two different websites into the lake.

Python and complex SQL queries are used through out the project to load the data, process the data , validate the data and model the data in databricks. Python is primarily used for API calls, handling dynamic logic and for writing ETL scripts. SQL is used for handling heavy lifting of data for joins, filtering, SCD Type 2 logic, and building star-schema models.

**Architecture Layers.**

Followed Medallion archtecture to store data in data lake:
<img width="460" height="211" alt="Screenshot 2025-07-19 at 10 10 09 PM" src="https://github.com/user-attachments/assets/6b3c993e-9b7f-42fb-8623-5accfef3f006" />

**Understanding Data Sources:**

<img width="643" height="306" alt="Pasted Graphic" src="https://github.com/user-attachments/assets/8c9bf1a2-ea11-4619-a609-2141542e0cbf" />

**EMR data **
**Data source:** (Azure SQL database)

We have EMR data of two hispitals in sql database
EMR data in both hosipitals has tables which include:
**Departments**(contains department information within hosipital)
**Encounters**( contains patients encounter details with hosipital). 
**Patients**(Contains patients information)
**Providers **(It contains details of service providers (doctors))
Schema: (ProviderID,FirstName,LastName,Specialization,DeptID,NPI)
**Transactions** (It contains details of transactions made by patient)
Scema(TransactionID,EncounterID,PatientID,ProviderID,DeptID,VisitDate,ServiceDate,PaidDate,VisitType,Amount,AmountType,PaidAmount,ClaimID,PayorID,ProcedureCode,ICDCode,LineOfBusiness,MedicaidID,MedicareID,InsertDate,ModifiedDate

**Claims Data:**
Data source:( ADLS Gen2)
Some third party insurance company uploads claims data for hosipital a and hosipital b in a folder called landing. This dataset contains detailed information about healthcare insurance claims, including patient visits, billing amounts, payments, and payer details for each submitted claim. There will be two datasets one for each hosipital . These two datasets will be uploaded to adls gen2 in a container called landing.
Schema(ClaimID,TransactionID,PatientID,EncounterID,ProviderID,DeptID,ServiceDate,ClaimDate,PayorID,ClaimAmount,PaidAmount,ClaimStatus,PayorType,Deductible,Coinsurance,Copay,InsertDate,ModifiedDate)

**CPT (Current Procedural Terminology code) data:**
**Data source** (ADLS Gen2)
CPT data provides standardized codes used to describe medical procedures and services . Each entry includes the procedure category, CPT code, its description, and the current status of the code. This data is uploaded to landing container of data lake along with claims data.
Schema (Procedure Code Category,CPT Codes,Procedure Code Descriptions,Code Status)

**ICD (International Classification of Diseases)**
**Data source** ( website (API) )
It contains standardized codes used to classify diagnoses, symptoms, and medical conditions. Maintained by the World Health Organization (WHO)

**NPI (National Provider Identifier)**
**Data source** (Website (API) )
It includes a unique 10-digit identification number assigned to healthcare providers and organizations in the U.S. This is issued by CMS (Centers for Medicare & Medicaid Services)

**Loading EMR data from SQL database to bronze layer of ADLS Gen2 using Azure Data Factory:**

EMR data from two different hosipitals is sitting in SQL database. An ETL pipeline is built in Azure data factory to load EMR data from both hosipitals into ADLS Gen2. To build and implement this pipeline a configuration file which contains details like databasename, datasource,  tablename, loadtype, watermark, is_active and target is used. Azure key vault is used to store secrets and this secrets are used while creating linked services in Azure Data Factory.

<img width="680" height="1041" alt="Pasted Graphic 2" src="https://github.com/user-attachments/assets/384b8311-eb02-4f78-bacd-cde2ab1e3da7" />

Above built ETL pipeline starts by iterating over a configuration file to identify the data sources, then checks if data already exists in the sink. If data is found, it is first archived to avoid overwriting. The pipeline then determines the load type—either full or incremental. For a full load, all data is loaded into ADLS and the log file is updated. For incremental loads, it fetches the last load timestamp from the log file, pulls only new or changed records, and then updates the log file accordingly.

<img width="1295" height="986" alt="Pasted Graphic 4" src="https://github.com/user-attachments/assets/9fedd863-cdfd-4457-8273-8a72e1e6bd5a" />

**Mount point in Databricks**:
To load and upload data to ADLS Gen2 from Azure Databricks, a mount point is created using Azure Data Factory. Access key for ADLS Gen2 is accessed in databricks using azure key vault backed secret scope.
NPI and ICD data is fetched from two different websites using web APIs and ingested into the Bronze layer of the data lake.
Claims data and CPT data are initially placed in the landing folder of ADLS Gen2 . Using Azure Data bricks, this raw data is ingested into the Bronze layer for further processing.
Now that all the raw data is available in the Bronze layer of ADLS Gen2, it is further processed and cleaned before being moved to the Silver layer. In the Silver layer, data is transformed, validated, and enriched to ensure consistency and quality.
**Transforming Bronze Data to Silver Layer Using Delta Lake in Azure Databricks:**

CPT data is moved from the bronze layer to the silver layer using Azure Databricks (leveraging the Delta Lakehouse) through SQL queries.
Slowly Changing Dimension (SCD) Type 2 logic and data quality checks are also implemented while moving CPT and Claims data from the bronze to the silver layer.

To ensure data quality and historical tracking, a separate data quality check table is created that mirrors all columns from the original bronze (raw) table. An additional column called is_quarantined flags records with missing or invalid values in critical fields (e.g., IDs or dates). Records with is_quarantined = true are excluded from further processing, ensuring only clean and valid records move forward. This table also includes three more metadata columns — is_current, inserted_date, and modified_date, to support Slowly Changing Dimension Type 2 (SCD2) tracking.

When data moves from the data quality check table to the silver layer, only non-quarantined records are processed. During this transfer, SCD Type 2 logic applies: if a record already exists in the silver table and any of its non-key fields have changed, the existing record is marked as outdated (is_current = false, and modified_date is updated). A new record is then inserted with the updated data and is_current = true, preserving both historical and current states. This ensures the silver layer always maintains the latest version of the data along with a complete audit trail of changes.

EMR data from two different hospitals is present in the bronze layer. The EMR data from both hospitals follows different schemas. While loading EMR data from the bronze to the silver layer, data from both hospitals is merged into a single model using common data modeling. To track data sources, an additional column called datasource is added during the merge.

We now have cleaned and structured data in the silver layer, making it reliable and ready for downstream use. Data scientists can leverage this curated data to build predictive models, perform advanced analytics, and extract actionable insights for healthcare operations and patient care improvement

**Data Aggregation and Modeling in the Gold Layer:**
The clean data from the silver layer is further aggregated and modeled using star and snowflake schemas, then moved to the gold layer. The gold layer contains well-structured dimension and fact tables derived from the curated data. This refined data is ready for analytics and dashboard development using tools like Power BI or Tableau.

**Further Improvements that can be made to above pipeline:**

--> Currently, the pipeline runs sequentially. It can be optimized to run tasks in parallel, improving performance and reducing overall load time.
--> Add a condition to the configuration logic so that the pipeline only executes when the is_active column is set to 1 in the configuration file. This will allow better control over which tables are processed.

**Conclusion:**

This project builds a reliable and efficient data pipeline using Azure to bring together healthcare data from different sources. It starts by collecting raw data from hospitals, insurance claims, and public APIs, storing everything safely in the bronze layer. Then, it cleans and combines the data in the silver layer, making sure to track any changes over time. Finally, it organizes the data in the gold layer using familiar models that make it easy for analysts and data scientists to use.

By following this approach, the pipeline creates a single, trusted source of healthcare data that teams rely on for reporting, analysis, and building predictive models. Using Azure Data Factory and Databricks automates the process, handles both full and incremental updates, and keeps everything up to date without a lot of manual effort. Overall, this project demonstrates how cloud technologies simplify managing complex healthcare data and help organizations get valuable insights faster.

**References:**

Emilio, N. (n.d.). What is medallion architecture in a Data Lakehouse context?. What is Medallion architecture in a Data Lakehouse context? https://blog.bismart.com/en/medallion-architecture

Mittal, S. (n.d.). End to End Azure Data Engineering Project by Sumit Mittal . https://www.youtube.com/watch?v=d3Vw3VtKDnc

Team, S., Deanna deBara, A., & Nacole Riccaboni, D. (n.d.). ICD and CPT codes: Their use & difference. SimplePractice. https://www.simplepractice.com/blog/icd-codes-and-cpt-codes/

What is claims data and its advantages and disadvantages?. Datavant. (n.d.). https://www.datavant.com/real-world-data-rwd/claims-data
















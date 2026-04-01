Use PortfolioProject_MarketingAnalytics


Select * from products

-- Query to categorize products based on their price
--dim_products

Select 
	ProductID,
	ProductName,
	Price,
	Case
		When Price < 50 Then 'Low'
		When Price Between 50 AND 200 Then 'Medium'
		Else 'High'
	End Price_Category
	From products
	



-- SQL Statement to join dim_customers with  dim_geography to enrich customer data with geografic information 

Select * from customers where CustomerID='1'

Select * from geography

--- dim_customer


Select  
	c.CustomerID,
	c.CustomerName,
	c.Email,
	c.Gender,
	c.age,
	g.City,
	g.Country
From customers c 
Join
geography g
On
c.GeographyID=g.GeographyID



--- fact_customer_review

-- Query to clean whitespace issue in the ReviewText Column

Select * from customer_reviews

Select 
	ReviewID,
	CustomerID,
	ProductID,
	ReviewDate,
	Rating,
	Replace(ReviewText,'  ',' ') As Review_Text
	from customer_reviews




-- Query to clean and normalize the engagement_data table 

-- fact_engagement in power bi

Select * from engagement_data

Select Distinct ContentType from engagement_data
Sp_Columns engagement_data

Select 
	EngagementID,
	contentID,
	CampaignID,
	ProductID,
	UPPER
		(REPLACE(
			REPLACE(ContentType, 'Socialmedia','Social Media'),
				'newsletter' , 'New Sletter')) as ContentType ,
	LEFT(ViewsClicksCombined, CHARINDEX('-' ,ViewsClicksCombined) - 1 ) as Views,
	RIGHT(ViewsClicksCombined,LEN(ViewsClicksCombined) - CHARINDEX('-',ViewsClicksCombined)) AS Clicks,
	Likes,
	FORMAT(CONVERT(DATE, EngagementDate), 'yyyy-mm-dd') as EngagementDate
From engagement_data
 





-- Make a Common Table Expression (CTE) to identify and tag duplicate records from customer journey

Select * from customer_journey


WITH DuplicateRecords AS (
    SELECT 
        JourneyID,  -- Select the unique identifier for each journey (and any other columns you want to include in the final result set)
        CustomerID,  -- Select the unique identifier for each customer
        ProductID,  -- Select the unique identifier for each product
        VisitDate,  -- Select the date of the visit, which helps in determining the timeline of customer interactions
        Stage,  -- Select the stage of the customer journey (e.g., Awareness, Consideration, etc.)
        Action,  -- Select the action taken by the customer (e.g., View, Click, Purchase)
        Duration,  -- Select the duration of the action or interaction
        -- Use ROW_NUMBER() to assign a unique row number to each record within the partition defined below
        ROW_NUMBER() OVER (
            -- PARTITION BY groups the rows based on the specified columns that should be unique
            PARTITION BY CustomerID, ProductID, VisitDate, Stage, Action  
            -- ORDER BY defines how to order the rows within each partition (usually by a unique identifier like JourneyID)
            ORDER BY JourneyID  
        ) AS row_num  -- This creates a new column 'row_num' that numbers each row within its partition
    FROM 
        dbo.customer_journey  -- Specifies the source table from which to select the data
)

-- Select all records from the CTE where row_num > 1, which indicates duplicate entries
    
SELECT *
FROM DuplicateRecords
 WHERE row_num > 1  -- Filters out the first occurrence (row_num = 1) and only shows the duplicates (row_num > 1)
ORDER BY JourneyID





-- fact_customer_journey in power bi

-- Outer query selects the final cleaned and standardized data and fill nulls
    
SELECT 
    JourneyID,  -- Selects the unique identifier for each journey to ensure data traceability
    CustomerID,  -- Selects the unique identifier for each customer to link journeys to specific customers
    ProductID,  -- Selects the unique identifier for each product to analyze customer interactions with different products
    VisitDate,  -- Selects the date of the visit to understand the timeline of customer interactions
    Stage,  -- Uses the uppercased stage value from the subquery for consistency in analysis
    Action,  -- Selects the action taken by the customer (e.g., View, Click, Purchase)
    COALESCE(Duration, avg_duration) AS Duration  -- Replaces missing durations with the average duration for the corresponding date
FROM 
    (
        -- Subquery to process and clean the data
        SELECT 
            JourneyID,  -- Selects the unique identifier for each journey to ensure data traceability
            CustomerID,  -- Selects the unique identifier for each customer to link journeys to specific customers
            ProductID,  -- Selects the unique identifier for each product to analyze customer interactions with different products
            VisitDate,  -- Selects the date of the visit to understand the timeline of customer interactions
            UPPER(Stage) AS Stage,  -- Converts Stage values to uppercase for consistency in data analysis
            Action,  -- Selects the action taken by the customer (e.g., View, Click, Purchase)
            Duration,  -- Uses Duration directly, assuming it's already a numeric type
            AVG(Duration) OVER (PARTITION BY VisitDate) AS avg_duration,  -- Calculates the average duration for each date, using only numeric values
            ROW_NUMBER() OVER (
                PARTITION BY CustomerID, ProductID, VisitDate, UPPER(Stage), Action  -- Groups by these columns to identify duplicate records
                ORDER BY JourneyID  -- Orders by JourneyID to keep the first occurrence of each duplicate
            ) AS row_num  -- Assigns a row number to each row within the partition to identify duplicates
        FROM 
            dbo.customer_journey  -- Specifies the source table from which to select the data
    ) AS subquery  -- Names the subquery for reference in the outer query
WHERE 
    row_num = 1  -- Keeps only the first occurrence of each duplicate group identified in the subquery 
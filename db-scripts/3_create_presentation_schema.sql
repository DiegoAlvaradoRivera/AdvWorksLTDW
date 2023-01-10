
CREATE SCHEMA presentation;
GO

/*
Name: 	     ProductHistory
Description: table that contains the history of changes of the product entities.
*/
CREATE TABLE presentation.ProductHistory(

  -- PK
  SurrogateKey              INT IDENTITY(1, 1) NOT NULL,

  -- attributes 
  ProductID                 INT             NOT NULL,
  Name                      NVARCHAR(50)    NOT NULL,
  ProductNumber             NVARCHAR(25)    NOT NULL,
  Color                     NVARCHAR(15)    NULL,
  StandardCost              MONEY           NOT NULL,
  ListPrice                 MONEY           NOT NULL,
  Size                      NVARCHAR(5)     NULL,
  Weight                    DECIMAL(8, 2)   NULL,
  SellStartDate             DATETIME        NOT NULL,
  SellEndDate               DATETIME        NULL,
  DiscontinuedDate          DATETIME        NULL,
  ProductModel              NVARCHAR(50)    NOT NULL,
  ProductModelDescription   NVARCHAR(400)   NOT NULL,
  ProductSubcategory        NVARCHAR(50)    NOT NULL,
  ProductCategory           NVARCHAR(50)    NOT NULL,

  -- SCD 2 Metadata Columns
  RowEffectiveDate          DATETIME        NOT NULL,
  RowExpirationDate         DATETIME        NOT NULL,
  RowCurrentFlag            BIT             NOT NULL,
  
  CONSTRAINT PK_DimProduct PRIMARY KEY CLUSTERED (SurrogateKey)
);
GO


/*
Name: 	     CustomerHistory
Description: table that contains the history of changes of the customer entities.
*/
CREATE TABLE presentation.CustomerHistory(
  
  -- PK
  SurrogateKey              INT IDENTITY(1, 1) NOT NULL,
  
  -- attributes
  CustomerID                INT             NOT NULL,
  NameStyle                 BIT             NOT NULL,
  Title                     NVARCHAR(8)     NULL,
  FirstName                 NVARCHAR(50)    NOT NULL,
  MiddleName                NVARCHAR(50)    NULL,
  LastName                  NVARCHAR(50)    NOT NULL,
  Suffix                    NVARCHAR(10)    NULL,
  CompanyName               NVARCHAR(128)   NULL,
  SalesPerson               NVARCHAR(256)   NULL,
  EmailAddress              NVARCHAR(50)    NULL,
  Phone                     NVARCHAR(25)    NULL,
  MainOfficeAddressLine1    NVARCHAR(60)    NOT NULL,
  MainOfficeAddressLine2    NVARCHAR(60)    NULL,
  MainOfficeCity            NVARCHAR(30)    NOT NULL,
  MainOfficeStateProvince   NVARCHAR(50)    NOT NULL,
  MainOfficeCountryRegion   NVARCHAR(50)    NOT NULL,
  MainOfficePostalCode      NVARCHAR(15)    NOT NULL,
  
  -- SCD 2 Metadata Columns
  RowEffectiveDate          DATETIME        NOT NULL,
  RowExpirationDate         DATETIME        NOT NULL,
  RowCurrentFlag            BIT             NOT NULL,
  
  CONSTRAINT PK_DimCustomer PRIMARY KEY CLUSTERED (SurrogateKey)
);
GO

/*
Name: 	     FactSalesOrders
Description: table that contains the shipped sales orders.
*/
CREATE TABLE presentation.FactSalesOrders(

  -- PK
  SalesOrderDetailID        INT               NOT NULL,

  -- FKs
  SalesOrderID              INT               NOT NULL,
  CustomerSK                INT               NOT NULL,
  ProductSK                 INT               NOT NULL,

	-- SOH Fields
	OrderDate                 DATETIME          NOT NULL,
	DueDate                   DATETIME          NOT NULL,
	ShipDate                  DATETIME          NULL,
	CustomerID                INT               NOT NULL,
	Status                    TINYINT           NOT NULL,
	OnlineOrderFlag           BIT               NOT NULL,
	SalesOrderNumber          NVARCHAR(23)      NOT NULL,
	PurchaseOrderNumber       NVARCHAR(25)      NULL,
	AccountNumber             NVARCHAR(15)      NULL,
	ShipMethod                NVARCHAR(50)      NOT NULL,
	CreditCardApprovalCode    varchar(15)       NULL,
	AllocatedTaxAmt           MONEY             NOT NULL,
	AllocatedFreight          MONEY             NOT NULL,
	Comment                   NVARCHAR(max)     NULL,

	-- shipping info
	ShippingAddressLine1      NVARCHAR(60)      NOT NULL,
	ShippingAddressLine2      NVARCHAR(60)      NULL,
	ShippingCity              NVARCHAR(30)      NOT NULL,
	ShippingStateProvince     NVARCHAR(50)      NOT NULL,
	ShippingCountryRegion     NVARCHAR(50)      NOT NULL,
	ShippingPostalCode        NVARCHAR(15)      NOT NULL,

	--SOD Fields
	ProductID                 INT               NOT NULL,
	OrderQty                  SMALLINT          NOT NULL,
	UnitPrice                 MONEY             NOT NULL,
	UnitPriceDiscount         MONEY             NOT NULL, 
	LineTotal     AS ISNULL(UnitPrice*(1.0-UnitPriceDiscount)*OrderQty, 0.0),
	LineTotalDue  AS ISNULL(UnitPrice*(1.0-UnitPriceDiscount)*OrderQty + AllocatedTaxAmt + AllocatedFreight, 0.0),

	CONSTRAINT PK_FactSalesOrders PRIMARY KEY CLUSTERED (SalesOrderDetailID),
  CONSTRAINT FK_CustomerSK      FOREIGN KEY (customerSK)  REFERENCES presentation.CustomerHistory(SurrogateKey),
  CONSTRAINT FK_ProductSK       FOREIGN KEY (productSK)   REFERENCES presentation.ProductHistory(SurrogateKey)
);
GO

/*
Name: 	     DimCustomer
Description: view on top of the CustomerHistory table that implements the customer dimension. 
    It applies the appropiate SCD semantic for each column.
*/
CREATE VIEW presentation.DimCustomer
AS
  SELECT 

       SurrogateKey

      ,CustomerID

      -- SCD 1 columns: keep the most recent value
      ,LAST_VALUE(NameStyle) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS NameStyle

      ,LAST_VALUE(Title) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS Title
    
      ,LAST_VALUE(FirstName) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS FirstName

      ,LAST_VALUE(MiddleName) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS MiddleName

      ,LAST_VALUE(LastName) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS LastName

      ,LAST_VALUE(Suffix) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS Suffix

      ,LAST_VALUE(CompanyName) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS CompanyName


      ,LAST_VALUE(EmailAddress) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS EmailAddress

      ,LAST_VALUE(Phone) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS Phone

      -- SCD 2 columns: keep the history
      ,SalesPerson
      ,MainOfficeAddressLine1
      ,MainOfficeAddressLine2
      ,MainOfficeCity
      ,MainOfficeStateProvince
      ,MainOfficeCountryRegion
      ,MainOfficePostalCode

      -- SCD 2 metadata columns
      ,RowEffectiveDate
      ,RowExpirationDate
      ,RowCurrentFlag
      ,CONVERT(
          BIT, 
          IIF((MAX(RowExpirationDate) OVER(PARTITION BY CustomerID)) = convert(DATETIME, '9999-12-31'), 0, 1)
      ) AS RowDeletedFlag
    
  FROM presentation.CustomerHistory;
GO

/*
Name: 	     DimProduct
Description: view on top of the ProductHistory table that implements the product dimension. 
    It applies the appropiate SCD semantic for each column.
*/
CREATE VIEW presentation.DimProduct
AS
  SELECT 

      SurrogateKey

      ,ProductID

      -- SCD 0 columns: keep the original value
      ,FIRST_VALUE(SellStartDate) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS SellStartDate

      -- SCD 2 columns_ show history of changes
      ,Name
      ,ProductNumber
      ,Color
      ,StandardCost
      ,ListPrice
      ,Size
      ,Weight

      -- SCD 1 columns: keep most current value
      ,LAST_VALUE(SellEndDate) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS SellEndDate

      ,LAST_VALUE(DiscontinuedDate) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS DiscontinuedDate

      ,LAST_VALUE(ProductModel) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ProductModel

      ,LAST_VALUE(ProductModelDescription) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ProductModelDescription

      ,LAST_VALUE(ProductSubcategory) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ProductSubcategory

      ,LAST_VALUE(ProductCategory) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS ProductCategory

      -- SCD 2 metadata columns
      ,RowEffectiveDate
      ,RowExpirationDate
      ,RowCurrentFlag
      ,CONVERT(
          BIT, 
          IIF((MAX(RowExpirationDate) OVER(PARTITION BY ProductID)) = convert(DATETIME, '9999-12-31'), 0, 1)
       ) AS RowDeletedFlag
       
  FROM presentation.ProductHistory;
GO

prINT 'PRESENTATION SCHEMA CREATED SUCCESSFULLY';
GO
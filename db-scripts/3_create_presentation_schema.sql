
/*

DESCRIPTION
This schema contains the tables nad views that conform the DW presentation area.


TABLES
- ProductsHistory: table that contains the history of changes of the product entities.
- CustomersHistory: table that contains the history of changes of the customer entities.
- FactSalesOrdes: table that contains the shipped sales orders.


VIEWS
- ProductsHistoryCurrentRows: view that returns the active row for the product entities.
- CustomersHistoryCurrentRows: view that returs thr active row for the customer entities.
- DimCustomer: view on top of the CustomersHistory table that implements the customer dimension. 
    It applies the appropiate SCD semantic for each column.
- DimProduct: view on top of the ProductsHistory table that implements the product dimension. 
    It applies the appropiate SCD semantic for each column.

*/


CREATE SCHEMA presentation;
GO

/*
Name: 	     presentation.ProductsHistory
Description: table that contains the history of changes of the product entities.
*/
CREATE TABLE presentation.ProductsHistory(
  [SurrogateKey]              [int] identity(1, 1) NOT NULL,
  [ProductID]                 [int] NOT NULL,
  [Name]                      [nvarchar](50) NOT NULL,
  [ProductNumber]             [nvarchar](25) NOT NULL,
  [Color]                     [nvarchar](15) NULL,
  [StandardCost]              [money] NOT NULL,
  [ListPrice]                 [money] NOT NULL,
  [Size]                      [nvarchar](5) NULL,
  [Weight]                    [decimal](8, 2) NULL,
  [SellStartDate]             [datetime] NOT NULL,
  [SellEndDate]               [datetime] NULL,
  [DiscontinuedDate]          [datetime] NULL,
  [ProductModel]              [nvarchar](50) NOT NULL,
  [ProductModelDescription]   [nvarchar](400) NOT NULL,
  [ProductSubcategory]        [nvarchar](50) NOT NULL,
  [ProductCategory]           [nvarchar](50) NOT NULL,
  -- SCD 2 Metadata Columns
  [RowEffectiveDate]          [datetime] NOT NULL,
  [RowExpirationDate]         [datetime] NOT NULL,
  [RowCurrentFlag]            [bit] NOT NULL,
  CONSTRAINT [PK_DimProduct] PRIMARY KEY CLUSTERED ([SurrogateKey])
);
GO

/*
Name: 	     presentation.ProductsHistoryCurrentRows
Description: view that returns the active row for the product entities.
*/
CREATE VIEW presentation.ProductsHistoryCurrentRows 
AS 
  SELECT * 
  FROM presentation.ProductsHistory 
  WHERE RowCurrentFlag = 1;
GO

/*
Name: 	     presentation.CustomersHistory
Description: table that contains the history of changes of the customer entities.
*/
CREATE TABLE presentation.CustomersHistory(
  -- primary key
  [SurrogateKey]              [int] identity(1, 1) NOT NULL,
  -- attributes
  [CustomerID]                [int] NOT NULL,
  [NameStyle]                 [bit] NOT NULL,
  [Title]                     [nvarchar](8) NULL,
  [FirstName]                 [nvarchar](50) NOT NULL,
  [MiddleName]                [nvarchar](50) NULL,
  [LastName]                  [nvarchar](50) NOT NULL,
  [Suffix]                    [nvarchar](10) NULL,
  [CompanyName]               [nvarchar](128) NULL,
  [SalesPerson]               [nvarchar](256) NULL,
  [EmailAddress]              [nvarchar](50) NULL,
  [Phone]                     [nvarchar](25) NULL,
  [MainOfficeAddressLine1]    [nvarchar](60) NOT NULL,
  [MainOfficeAddressLine2]    [nvarchar](60) NULL,
  [MainOfficeCity]            [nvarchar](30) NOT NULL,
  [MainOfficeStateProvince]   [nvarchar](50) NOT NULL,
  [MainOfficeCountryRegion]   [nvarchar](50) NOT NULL,
  [MainOfficePostalCode]      [nvarchar](15) NOT NULL,
  -- SCD 2 Metadata Columns
  [RowEffectiveDate]          [datetime] NOT NULL,
  [RowExpirationDate]         [datetime] NOT NULL,
  [RowCurrentFlag]            [bit] NOT NULL,
  CONSTRAINT [PK_DimCustomer] PRIMARY KEY CLUSTERED ([SurrogateKey])
);
GO

/*
Name: 	     presentation.CustomersHistoryCurrentRows
Description: view that returns the active row for the customer entities.
*/
CREATE VIEW presentation.CustomersHistoryCurrentRows 
AS 
  SELECT * 
  FROM presentation.CustomersHistory 
  WHERE RowCurrentFlag = 1;
GO

/*
Name: 	     presentation.FactSalesOrders
Description: table that contains the shipped sales orders.
*/
CREATE TABLE presentation.FactSalesOrders(
	-- SOH Fields
  [SalesOrderID]              [int] NOT NULL,
	[OrderDate]                 [datetime] NOT NULL,
	[DueDate]                   [datetime] NOT NULL,
	[ShipDate]                  [datetime] NULL,
	[CustomerID]                [int] NOT NULL,
	[Status]                    [tinyint] NOT NULL,
	[OnlineOrderFlag]           [bit] NOT NULL,
	[SalesOrderNumber]          [nvarchar](23) NOT NULL,
	[PurchaseOrderNumber]       [nvarchar](25) NULL,
	[AccountNumber]             [nvarchar](15) NULL,
	[ShipMethod]                [nvarchar](50) NOT NULL,
	[CreditCardApprovalCode]    [varchar](15) NULL,
	[AllocatedTaxAmt]           [money] NOT NULL,
	[AllocatedFreight]          [money] NOT NULL,
	[Comment]                   [nvarchar](max) NULL,
	-- shipping info
	[ShippingAddressLine1]      [nvarchar](60) NOT NULL,
	[ShippingAddressLine2]      [nvarchar](60) NULL,
	[ShippingCity]              [nvarchar](30) NOT NULL,
	[ShippingStateProvince]     [nvarchar](50) NOT NULL,
	[ShippingCountryRegion]     [nvarchar](50) NOT NULL,
	[ShippingPostalCode]        [nvarchar](15) NOT NULL,
	--SOD Fields
	[SalesOrderDetailID]        [int] NOT NULL,
	[ProductID]                 [int] NOT NULL,
	[OrderQty]                  [smallint] NOT NULL,
	[UnitPrice]                 [money] NOT NULL,
	[UnitPriceDiscount]         [money] NOT NULL,
	[LineTotal]     AS isnull([UnitPrice]*(1.0-[UnitPriceDiscount])*[OrderQty], 0.0),
	[LineTotalDue]  AS isnull(([UnitPrice]*(1.0-[UnitPriceDiscount])*[OrderQty])+[AllocatedTaxAmt]+[AllocatedFreight], 0.0),
	CONSTRAINT [PK_FactSalesOrders] PRIMARY KEY CLUSTERED ([SalesOrderID],[SalesOrderDetailID])
);
GO

/*
Name: 	     presentation.DimCustomer
Description: view on top of the CustomersHistory table that implements the customer dimension. 
    It applies the appropiate SCD semantic for each column.
*/
CREATE VIEW presentation.DimCustomer
AS
  SELECT 
      [SurrogateKey]
      ,[CustomerID]

      -- SCD 1 columns: keep the most recent value
      ,LAST_VALUE(NameStyle) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [NameStyle]

      ,LAST_VALUE(Title) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [Title]
    
      ,LAST_VALUE(FirstName) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [FirstName]

      ,LAST_VALUE(MiddleName) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [MiddleName]

      ,LAST_VALUE(LastName) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [LastName]

      ,LAST_VALUE(Suffix) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [Suffix]

      ,LAST_VALUE(CompanyName) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [CompanyName]


      ,LAST_VALUE(EmailAddress) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [EmailAddress]

      ,LAST_VALUE(Phone) OVER(
        PARTITION BY CustomerID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [Phone]

      -- SCD 2 columns: keep the history
      ,[SalesPerson]
      ,[MainOfficeAddressLine1]
      ,[MainOfficeAddressLine2]
      ,[MainOfficeCity]
      ,[MainOfficeStateProvince]
      ,[MainOfficeCountryRegion]
      ,[MainOfficePostalCode]

      -- SCD 2 metadata columns
      ,[RowEffectiveDate]
      ,[RowExpirationDate]
      ,[RowCurrentFlag]
      ,CONVERT(
          BIT, 
          IIF((MAX(RowExpirationDate) OVER(PARTITION BY CustomerID)) = convert(DATETIME, '9999-12-31'), 0, 1)
      ) AS [RowDeletedFlag]
  FROM [presentation].[CustomersHistory];
GO

/*
Name: 	     presentation.DimProduct
Description: view on top of the ProductsHistory table that implements the product dimension. 
    It applies the appropiate SCD semantic for each column.
*/
CREATE VIEW presentation.DimProduct
AS
  SELECT 
      [SurrogateKey]
      ,[ProductID]

      -- SCD 0 columns: keep the original value
      ,FIRST_VALUE(SellStartDate) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS [SellStartDate]

      -- SCD 2 columns_ show history of changes
      ,[Name]
      ,[ProductNumber]
      ,[Color]
      ,[StandardCost]
      ,[ListPrice]
      ,[Size]
      ,[Weight]

      -- SCD 1 columns: keep most current value
      ,LAST_VALUE(SellEndDate) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [SellEndDate]

      ,LAST_VALUE(DiscontinuedDate) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [DiscontinuedDate]

      ,LAST_VALUE(ProductModel) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [ProductModel]

      ,LAST_VALUE(ProductModelDescription) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [ProductModelDescription]

      ,LAST_VALUE(ProductSubcategory) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [ProductSubcategory]

      ,LAST_VALUE(ProductCategory) OVER(
        PARTITION BY ProductID 
        ORDER BY RowEffectiveDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS [ProductCategory]

      -- SCD 2 metadata columns
      ,[RowEffectiveDate]
      ,[RowExpirationDate]
      ,[RowCurrentFlag]
      ,CONVERT(
          BIT, 
          IIF((MAX(RowExpirationDate) OVER(PARTITION BY ProductID)) = convert(DATETIME, '9999-12-31'), 0, 1)
       ) AS [RowDeletedFlag]
  FROM [presentation].[ProductsHistory];
GO

print 'PRESENTATION SCHEMA CREATED SUCCESSFULLY';
GO
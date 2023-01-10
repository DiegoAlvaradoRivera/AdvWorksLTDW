
create schema presentation;
go 

/*
Name: 	     ProductHistory
Description: table that contains the history of changes of the product entities.
*/
create table presentation.ProductHistory(

  -- PK
  SurrogateKey              int identity(1, 1) not null,

  -- attributes 
  ProductID                 int             not null,
  Name                      nvarchar(50)    not null,
  ProductNumber             nvarchar(25)    not null,
  Color                     nvarchar(15)    null,
  StandardCost              money           not null,
  ListPrice                 money           not null,
  Size                      nvarchar(5)     null,
  Weight                    DECIMAL(8, 2)   null,
  SellStartDate             datetime        not null,
  SellEndDate               datetime        null,
  DiscontinuedDate          datetime        null,
  ProductModel              nvarchar(50)    not null,
  ProductModelDescription   nvarchar(400)   not null,
  ProductSubcategory        nvarchar(50)    not null,
  ProductCategory           nvarchar(50)    not null,

  -- SCD 2 Metadata Columns
  RowEffectiveDate          datetime        not null,
  RowExpirationDate         datetime        not null,
  RowCurrentFlag            bit             not null,
  
  constraint PK_DimProduct primary key clustered (SurrogateKey)
);
go


/*
Name: 	     CustomerHistory
Description: table that contains the history of changes of the customer entities.
*/
create table presentation.CustomerHistory(
  
  -- PK
  SurrogateKey              int identity(1, 1) not null,
  
  -- attributes
  CustomerID                int             not null,
  NameStyle                 bit             not null,
  Title                     nvarchar(8)     null,
  FirstName                 nvarchar(50)    not null,
  MiddleName                nvarchar(50)    null,
  LastName                  nvarchar(50)    not null,
  Suffix                    nvarchar(10)    null,
  CompanyName               nvarchar(128)   null,
  SalesPerson               nvarchar(256)   null,
  EmailAddress              nvarchar(50)    null,
  Phone                     nvarchar(25)    null,
  MainOfficeAddressLine1    nvarchar(60)    not null,
  MainOfficeAddressLine2    nvarchar(60)    null,
  MainOfficeCity            nvarchar(30)    not null,
  MainOfficeStateProvince   nvarchar(50)    not null,
  MainOfficeCountryRegion   nvarchar(50)    not null,
  MainOfficePostalCode      nvarchar(15)    not null,
  
  -- SCD 2 Metadata Columns
  RowEffectiveDate          datetime        not null,
  RowExpirationDate         datetime        not null,
  RowCurrentFlag            bit             not null,
  
  constraint PK_DimCustomer primary key clustered (SurrogateKey)
);
go

/*
Name: 	     FactSalesOrders
Description: table that contains the shipped sales orders.
*/
create table presentation.FactSalesOrders(

  -- PK
  SalesOrderDetailID        int               not null,

  -- FKs
  SalesOrderID              int               not null,
  CustomerSK                int               not null,
  ProductSK                 int               not null,

	-- SOH Fields
	OrderDate                 datetime          not null,
	DueDate                   datetime          not null,
	ShipDate                  datetime          null,
	CustomerID                int               not null,
	Status                    TINYINT           not null,
	OnlineOrderFlag           bit               not null,
	SalesOrderNumber          nvarchar(23)      not null,
	PurchaseOrderNumber       nvarchar(25)      null,
	AccountNumber             nvarchar(15)      null,
	ShipMethod                nvarchar(50)      not null,
	CreditCardApprovalCode    varchar(15)       null,
	AllocatedTaxAmt           money             not null,
	AllocatedFreight          money             not null,
	Comment                   nvarchar(max)     null,

	-- shipping info
	ShippingAddressLine1      nvarchar(60)      not null,
	ShippingAddressLine2      nvarchar(60)      null,
	ShippingCity              nvarchar(30)      not null,
	ShippingStateProvince     nvarchar(50)      not null,
	ShippingCountryRegion     nvarchar(50)      not null,
	ShippingPostalCode        nvarchar(15)      not null,

	--SOD Fields
	ProductID                 int               not null,
	OrderQty                  smallint          not null,
	UnitPrice                 money             not null,
	UnitPriceDiscount         money             not null, 
	LineTotal     as ISNULL(UnitPrice*(1.0-UnitPriceDiscount)*OrderQty, 0.0),
	LineTotalDue  as ISNULL(UnitPrice*(1.0-UnitPriceDiscount)*OrderQty + AllocatedTaxAmt + AllocatedFreight, 0.0),

	constraint PK_FactSalesOrders primary key clustered (SalesOrderDetailID),
  constraint FK_CustomerSK      foreign key (customerSK)  references presentation.CustomerHistory(SurrogateKey),
  constraint FK_ProductSK       foreign key (productSK)   references presentation.ProductHistory(SurrogateKey)
);
go

/*
Name: 	     DimCustomer
Description: view on top of the CustomerHistory table that implements the customer dimension. 
    It applies the appropiate SCD semantic for each column.
*/
create view presentation.DimCustomer
as
  select 

       SurrogateKey

      ,CustomerID

      -- SCD 1 columns: keep the most recent value
      ,last_value(NameStyle) over(
        partition by CustomerID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as NameStyle

      ,last_value(Title) over(
        partition by CustomerID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as Title
    
      ,last_value(FirstName) over(
        partition by CustomerID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as FirstName

      ,last_value(MiddleName) over(
        partition by CustomerID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as MiddleName

      ,last_value(LastName) over(
        partition by CustomerID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as LastName

      ,last_value(Suffix) over(
        partition by CustomerID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as Suffix

      ,last_value(CompanyName) over(
        partition by CustomerID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as CompanyName


      ,last_value(EmailAddress) over(
        partition by CustomerID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as EmailAddress

      ,last_value(Phone) over(
        partition by CustomerID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as Phone

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
      ,convert(
          bit, 
          iif((max(RowExpirationDate) over(partition by CustomerID)) = convert(datetime, '9999-12-31'), 0, 1)
      ) as RowDeletedFlag
    
  from presentation.CustomerHistory;
go

/*
Name: 	     DimProduct
Description: view on top of the ProductHistory table that implements the product dimension. 
    It applies the appropiate SCD semantic for each column.
*/
create view presentation.DimProduct
as
  select 

      SurrogateKey

      ,ProductID

      -- SCD 0 columns: keep the original value
      ,first_value(SellStartDate) over(
        partition by ProductID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
        ) as SellStartDate

      -- SCD 2 columns_ show history of changes
      ,Name
      ,ProductNumber
      ,Color
      ,StandardCost
      ,ListPrice
      ,Size
      ,Weight

      -- SCD 1 columns: keep most current value
      ,last_value(SellEndDate) over(
        partition by ProductID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as SellEndDate

      ,last_value(DiscontinuedDate) over(
        partition by ProductID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as DiscontinuedDate

      ,last_value(ProductModel) over(
        partition by ProductID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as ProductModel

      ,last_value(ProductModelDescription) over(
        partition by ProductID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as ProductModelDescription

      ,last_value(ProductSubcategory) over(
        partition by ProductID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as ProductSubcategory

      ,last_value(ProductCategory) over(
        partition by ProductID 
        order by RowEffectiveDate
        rows between unbounded preceding and unbounded following
      ) as ProductCategory

      -- SCD 2 metadata columns
      ,RowEffectiveDate
      ,RowExpirationDate
      ,RowCurrentFlag
      ,convert(
          bit, 
          iif((max(RowExpirationDate) over(partition by ProductID)) = convert(datetime, '9999-12-31'), 0, 1)
       ) as RowDeletedFlag
       
  from presentation.ProductHistory;
go

print 'PRESENTATION SCHEMA CREATED SUCCESSFULLY';
go
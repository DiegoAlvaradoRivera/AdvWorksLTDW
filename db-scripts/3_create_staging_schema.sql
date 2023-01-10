
set nocount on;
go

create schema staging;
go 

/*
Name: 	     JobLogs
Description: table in which the job runs are logged
*/
create table staging.JobLogs(
	pipeline_name 		varchar(50)	    NOT NULL,
	pipeline_run_id     varchar(100)    NOT NULL,
	sync_ct_version 	int			    NULL,
	sync_timestamp      datetime		NOT NULL
	constraint PK_JobLogs primary key (pipeline_name, pipeline_run_id)
);
go


/*
Name: 	     SP_LogJobRun
Description: SP to log a job run
*/
create procedure staging.SP_LogJobRun(
	@pipeline_name   varchar(50),
	@pipeline_run_id varchar(100),
	@sync_ct_version int,
	@sync_timestamp  datetime
)
as
begin 
	insert into staging.JobLogs
	(pipeline_name,  pipeline_run_id,  sync_ct_version,  sync_timestamp)
	values
	(@pipeline_name, @pipeline_run_id, @sync_ct_version, @sync_timestamp)
end;
go 

/*
Name: 	     SP_GetJobtLastExecution
Description: SP to retrieve the metadata of the last execution of
    a job run.
*/
create or alter procedure staging.SP_GetJobtLastExecution(
	@pipeline_name varchar(50)
)
as 
begin

	select top 1 *
	from staging.JobLogs
	where pipeline_name = @pipeline_name
	order by sync_timestamp desc

	return;
end;
go

/*
Name: 	     CustomerCTChangesStaging
Description: staging table for the customer CT changes

*/
create table staging.CustomerCTChangesStaging(

	ct_current_version          int NOT NULL,
	extraction_time             datetime not null,
	ct_key                      int not null, 
	ct_operation                char(1) not null, 
	ct_insertion_time           datetime null,
	ct_last_mod_time            datetime null,

	[CustomerID]                [int] UNIQUE NOT NULL,
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
	[MainOfficePostalCode]      [nvarchar](15) NOT NULL
);
go

/*
Name: 	     ProductCTChangesStaging
Description: staging table for the product CT changes
*/
create table staging.ProductCTChangesStaging(

	ct_current_version          int NOT NULL,
	extraction_time             datetime not null,
	ct_key                      int not null, 
	ct_operation                char(1) not null, 
	ct_insertion_time           datetime null,
	ct_last_mod_time            datetime null,

	[ProductID]                 [int] UNIQUE NOT NULL,
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
);
go

/*
Name: 	     SalesOrderHeaderStaging
Description: staging table for SOH records
*/
create table staging.SalesOrderHeaderStaging(

	[SalesOrderID]              [int] UNIQUE NOT NULL,
    [CustomerID]                [int] NOT NULL,
	[OrderDate]                 [datetime] NOT NULL,
	[DueDate]                   [datetime] NOT NULL,
	[ShipDate]                  [datetime] NULL,
	
    [Status]                    [tinyint] NOT NULL,
	[OnlineOrderFlag]           [bit] NOT NULL,
	[SalesOrderNumber]          [nvarchar](23) NOT NULL,
	[PurchaseOrderNumber]       [nvarchar](25) NULL,
	[AccountNumber]             [nvarchar](15) NULL,
	[ShipMethod]                [nvarchar](50) NOT NULL,
	[CreditCardApprovalCode]    [varchar](15) NULL,
	[TaxAmt]                    [money] NOT NULL,
	[Freight]                   [money] NOT NULL,
    [Subtotal]                  [money] NOT NULL,
	[Comment]                   [nvarchar](max) NULL,

	-- shipping info
	[ShippingAddressLine1]      [nvarchar](60) NOT NULL,
	[ShippingAddressLine2]      [nvarchar](60) NULL,
	[ShippingCity]              [nvarchar](30) NOT NULL,
	[ShippingStateProvince]     [nvarchar](50) NOT NULL,
	[ShippingCountryRegion]     [nvarchar](50) NOT NULL,
	[ShippingPostalCode]        [nvarchar](15) NOT NULL,
);
go

/*
Name: 	     SalesOrderHeaderStaging
Description: staging table for SOD records
*/
create table staging.SalesOrderDetailStaging(

	[SalesOrderDetailID]        [int] NOT NULL,
    [SalesOrderID]              [int] NOT NULL,
	[ProductID]                 [int] NOT NULL,
	[OrderQty]                  [smallint] NOT NULL,
	[UnitPrice]                 [money] NOT NULL,
	[UnitPriceDiscount]         [money] NOT NULL,
	[LineTotal]                 [numeric](38, 6) NOT NULL,

    CONSTRAINT UniqueHeaderLineCombinations UNIQUE(SalesOrderDetailID)

);
go

/*
Name: 	     SP_CustomerHistoryIncrementalLoad
Description: SP that loads the records in the CustomerCTChangesStaging table into 
    the CustomerHistory table. For inserted customers, a new row is created. 
    For modified customers, the current row is expired and a new one is created. 
    For customers that have been deleted, the current row is expired.
*/
create or alter procedure staging.SP_CustomerHistoryIncrementalLoad(
    @pipeline_run_id varchar(100)
)
as 
begin

    -- for entities that have been updated or deleted
    -- expire the current row
    update CH
    set 
    CH.RowExpirationDate = CT.ct_last_mod_time,
    CH.RowCurrentFlag = 0
    from 
    presentation.CustomerHistory as CH
    inner join 
    staging.CustomerCTChangesStaging as CT
    on CH.CustomerID = CT.CustomerID and CH.RowCurrentFlag = 1 and CT.ct_operation in ('U', 'D');

    -- for entities that have been inserted or updated
    -- insert a new row with the current data
    insert into presentation.CustomerHistory(
    CustomerID, NameStyle, Title, FirstName, MiddleName, LastName, Suffix, CompanyName, SalesPerson, EmailAddress, Phone, 
    MainOfficeAddressLine1, MainOfficeAddressLine2, MainOfficeCity, MainOfficeStateProvince, MainOfficeCountryRegion, MainOfficePostalCode,
    RowEffectiveDate, RowExpirationDate, RowCurrentFlag
    )
    select 
    CustomerID, NameStyle, Title, FirstName, MiddleName, LastName, Suffix, CompanyName, SalesPerson, EmailAddress, Phone, 
    MainOfficeAddressLine1, MainOfficeAddressLine2, MainOfficeCity, MainOfficeStateProvince, MainOfficeCountryRegion, MainOfficePostalCode,
    IIF(ct_operation = 'I', ct_insertion_time, ct_last_mod_time) as RowEffectiveDate, 
    CONVERT(DATETIME, '9999-12-31') as RowExpirationDate, 
    1 as RowCurrentFlag
    from staging.CustomerCTChangesStaging 
    where ct_operation in ('I', 'U')

    -- log the job
    declare @ct_version as int;
    declare @extraction_time as datetime;
    
    select top 1 
        @ct_version = ct_current_version,
        @extraction_time = extraction_time
    from staging.CustomerCTChangesStaging

    exec staging.SP_LogJobRun 
        @pipeline_name = 'customer_sync', 
        @pipeline_run_id = @pipeline_run_id,
        @sync_ct_version = @ct_version,
        @sync_timestamp = @extraction_time;

    -- truncate the CustomerCTChangesStaging table
    truncate table staging.CustomerCTChangesStaging;

end;
go

/*
Name: 	     SP_ProductHistoryIncrementalLoad
Description: SP that loads the records in the ProductCTChangesStaging table into 
    the ProductHistory table. For inserted products, a new row is created. 
    For modified products, the current row is expired and a new one is created. 
    For products that have been deleted, the current row is expired.
*/
create or alter procedure staging.SP_ProductHistoryIncrementalLoad(
    @pipeline_run_id varchar(100)
)
as 
begin

    -- for entities that have been updated or deleted
    -- expire the current row
    update HT
    set 
    HT.RowExpirationDate = CT.ct_last_mod_time,
    HT.RowCurrentFlag = 0
    from 
    presentation.ProductHistory as HT
    inner join 
    staging.ProductCTChangesStaging as CT
    on HT.ProductID = CT.ProductID and HT.RowCurrentFlag = 1 and CT.ct_operation in ('U', 'D');

    -- for entities that have been inserted or updated
    -- insert a new row with the current data
    insert into presentation.ProductHistory(
    ProductID, Name, ProductNumber, Color, StandardCost, ListPrice, Size, Weight, SellStartDate, SellEndDate, 
    DiscontinuedDate, ProductModel, ProductModelDescription, ProductSubcategory, ProductCategory,
    RowEffectiveDate, RowExpirationDate, RowCurrentFlag
    )
    select 
    ProductID, Name, ProductNumber, Color, StandardCost, ListPrice, Size, Weight, SellStartDate, SellEndDate, 
    DiscontinuedDate, ProductModel, ProductModelDescription, ProductSubcategory, ProductCategory,
    IIF(ct_operation = 'I', ct_insertion_time, ct_last_mod_time)    as RowEffectiveDate, 
    CONVERT(DATETIME, '9999-12-31')                                 as RowExpirationDate, 
    1                                                               as RowCurrentFlag
    from staging.ProductCTChangesStaging 
    where ct_operation in ('I', 'U');

    -- log the job run
    declare @ct_version as int;
    declare @extraction_time as datetime;

    select top 1 
        @ct_version = ct_current_version,
        @extraction_time = extraction_time
    from staging.ProductCTChangesStaging

    exec staging.SP_LogJobRun 
        @pipeline_name = 'product_sync', 
        @pipeline_run_id = @pipeline_run_id,
        @sync_ct_version = @ct_version,
        @sync_timestamp = @extraction_time;

    -- truncate the ProductCTChangesStaging table
    truncate table staging.ProductCTChangesStaging;

end;
go

/*
Name: 	     SP_FactSalesOrdersIncrementalLoad
Description: SP that loads the records in the SalesOrderHeaderStaging and 
    SalesOrderDetailStaging tables into the FactSalesOrders table. 

    It applies the following transformations:
    - add a surrogate key to the CustomesrHistory table based on the OrderDate.
    - add a surrogate key to the ProductHistory table based on the OrderDate.
    - allocates the header-level SOH.TaxAmt column to the line level by 
        distributing it proportionally to the LineTotal.
    - allocates the header-level SOH.Freight column to the line level by 
        distributing it proportionally to the LineTotal.
*/
create or alter procedure staging.SP_FactSalesOrdersIncrementalLoad(
    @pipeline_run_id varchar(100)
)
as 
begin

    -- transform the staging files and load the facts
    insert into presentation.FactSalesOrders
    select 

        SOD.SalesOrderDetailID as SalesOrderDetailID,

        SOH.SalesOrderID as SalesOrderID, 
        staging.getCustomerSK(CustomerID, OrderDate) as CustomerSK,
        staging.getProductSK(ProductID, OrderDate) as ProductSK,

        OrderDate, DueDate, ShipDate, 
        CustomerID, Status, OnlineOrderFlag, SalesOrderNumber, 
        PurchaseOrderNumber, AccountNumber, ShipMethod, CreditCardApprovalCode,

        (SOD.LineTotal / SOH.SubTotal) * SOH.TaxAmt as  AllocatedTaxAmt,
        (SOD.LineTotal / SOH.SubTotal) * SOH.Freight as AllocatedFreight,

        Comment,

        ShippingAddressLine1, ShippingAddressLine2, ShippingCity, 
        ShippingStateProvince, ShippingCountryRegion, ShippingPostalCode,

        ProductID, OrderQty, UnitPrice, UnitPriceDiscount

    from 
    staging.SalesOrderHeaderStaging as SOH 
    left join 
    staging.SalesOrderDetailStaging as SOD 
    on SOH.SalesOrderID = SOD.SalesOrderID;

    -- log the job
    declare @extraction_time as datetime = SYSDATETIMEOFFSET() at time zone N'Eastern Standard Time';
    
    exec staging.SP_LogJobRun 
        @pipeline_name = 'sales_orders_sync', 
        @pipeline_run_id = @pipeline_run_id,
        @sync_ct_version = NULL,
        @sync_timestamp = @extraction_time;

    -- truncate the staging tables
    truncate table staging.SalesOrderHeaderStaging;
    truncate table staging.SalesOrderDetailStaging;

end; 
go 

/*
Name: 	     getCustomerSK
Description: returns the surrogate key associated with a customer at 
    a point in time
*/
create or alter function staging.getCustomerSK(
    @customer_id int,
    @timetamp datetime
)
returns int 
as 
begin 
    declare @result int;
    select @result = SurrogateKey
    from presentation.CustomerHistory 
    where CustomerID = @customer_id and 
        @timetamp between RowEffectiveDate and RowExpirationDate
    return @result;
end;
go

/*
Name: 	     getCustomerSK
Description: returns the surrogate key associated with a product at 
    a point in time
*/
create or alter function staging.getProductSK(
    @product_id int,
    @timetamp datetime
)
returns int 
as 
begin 
    declare @result int;
    select @result = SurrogateKey
    from presentation.ProductHistory 
    where ProductID = @product_id and 
        @timetamp between RowEffectiveDate and RowExpirationDate
    return @result;
end;
go


print 'STAGING SCHEMA CREATED SUCCESSFULLY';
go 
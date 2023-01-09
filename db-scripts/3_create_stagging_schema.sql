

/*

DESCRIPTION
This schema contains the tables, views and functions that support the DW 
synchronization jobs


TABLES
- JobLogs: table that contains the logs of the ETL jobs

STORED PROCEDURES
- SP_LogJobRun: procedure to log a job run

*/

set nocount on;
go

create schema stagging;
go 

/*
Name: 	 JobLogs
Description: table in which the job runs are logged
*/
create table stagging.JobLogs(
	pipeline_name 		varchar(50)	NOT NULL,
	pipeline_run_id     varchar(100) NOT NULL,
	sync_ct_version 	int			NULL,
	sync_timestamp      datetime		NOT NULL
	primary key (pipeline_name, pipeline_run_id)
);
go


/*
Name: 	 SP_LogJobRun
Description: procedure to log a job run
*/
create procedure stagging.SP_LogJobRun(
	@pipeline_name   varchar(50),
	@pipeline_run_id varchar(100),
	@sync_ct_version int,
	@sync_timestamp  datetime
)
as
begin 
	insert into stagging.JobLogs
	(pipeline_name,  pipeline_run_id,  sync_ct_version,  sync_timestamp)
	values
	(@pipeline_name, @pipeline_run_id, @sync_ct_version, @sync_timestamp)
end;
go 

/*
TODO
*/
create or alter procedure stagging.SP_GetJobtLastExecution(
	@pipeline_name varchar(50)
)
as 
begin

	select top 1 *
	from stagging.JobLogs
	where pipeline_name = @pipeline_name
	order by sync_timestamp desc

	return;
end;
go


create table stagging.CustomerCTChangesStagging(

	ct_current_version int NOT NULL,
	extraction_time datetime not null,
	ct_key int not null, 
	ct_operation CHAR(1) not null, 
	ct_insertion_time datetime null,
	ct_last_mod_time datetime null,

	[CustomerID] [int] UNIQUE NOT NULL,
	[NameStyle] [bit] NOT NULL,
	[Title] [nvarchar](8) NULL,
	[FirstName] [nvarchar](50) NOT NULL,
	[MiddleName] [nvarchar](50) NULL,
	[LastName] [nvarchar](50) NOT NULL,
	[Suffix] [nvarchar](10) NULL,
	[CompanyName] [nvarchar](128) NULL,
	[SalesPerson] [nvarchar](256) NULL,
	[EmailAddress] [nvarchar](50) NULL,
	[Phone]                     [nvarchar](25) NULL,
	[MainOfficeAddressLine1]    [nvarchar](60) NOT NULL,
	[MainOfficeAddressLine2]    [nvarchar](60) NULL,
	[MainOfficeCity]            [nvarchar](30) NOT NULL,
	[MainOfficeStateProvince]   [nvarchar](50) NOT NULL,
	[MainOfficeCountryRegion]   [nvarchar](50) NOT NULL,
	[MainOfficePostalCode]      [nvarchar](15) NOT NULL,
);
go

create table stagging.ProductCTChangesStagging(

	ct_current_version int NOT NULL,
	extraction_time datetime not null,
	ct_key int not null, 
	ct_operation CHAR(1) not null, 
	ct_insertion_time datetime null,
	ct_last_mod_time datetime null,

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


create table stagging.SalesOrderHeaderStagging(

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


create table stagging.SalesOrderDetailStagging(

	[SalesOrderDetailID]        [int] NOT NULL,

    [SalesOrderID]              [int] NOT NULL,
	[ProductID]                 [int] NOT NULL,

	[OrderQty]                  [smallint] NOT NULL,
	[UnitPrice]                 [money] NOT NULL,
	[UnitPriceDiscount]         [money] NOT NULL,
	[LineTotal]                 [numeric](38, 6) NOT NULL,

    CONSTRAINT UniqueHeaderLineCombinations UNIQUE(SalesOrderID, SalesOrderDetailID)

);
go

create or alter procedure stagging.SP_CustomerHistoryIncrementalLoad(
    @pipeline_run_id varchar(100)
)
as 
begin

    -- expire current rows that have been modified
    update CH
    set 
    CH.RowExpirationDate = CT.ct_last_mod_time,
    CH.RowCurrentFlag = 0
    from 
    presentation.CustomersHistory as CH
    inner join 
    stagging.CustomerCTChangesStagging as CT
    on CH.CustomerID = CT.CustomerID and CH.RowCurrentFlag = 1;

    -- insert new rows
    insert into presentation.CustomersHistory(
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
    from stagging.CustomerCTChangesStagging 
    where ct_operation in ('I', 'U')

    -- log the job
    declare @ct_version as int;
    declare @extraction_time as datetime;
    
    select top 1 
        @ct_version = ct_current_version,
        @extraction_time = extraction_time
    from stagging.CustomerCTChangesStagging

    exec stagging.SP_LogJobRun 
        @pipeline_name = 'customer_sync', 
        @pipeline_run_id = @pipeline_run_id,
        @sync_ct_version = @ct_version,
        @sync_timestamp = @extraction_time;

    -- truncate the CustomerCTChangesStagging table
    truncate table stagging.CustomerCTChangesStagging;
end;
go


create or alter procedure stagging.SP_ProductHistoryIncrementalLoad(
    @pipeline_run_id varchar(100)
)
as 
begin

    -- expire current rows that have been modified
    update HT
    set 
    HT.RowExpirationDate = CT.ct_last_mod_time,
    HT.RowCurrentFlag = 0
    from 
    presentation.ProductsHistory as HT
    inner join 
    stagging.ProductCTChangesStagging as CT
    on HT.ProductID = CT.ProductID and HT.RowCurrentFlag = 1;

    -- insert new rows
    insert into presentation.ProductsHistory(
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
    from stagging.ProductCTChangesStagging 
    where ct_operation in ('I', 'U');

    -- log the job run
    declare @ct_version as int;
    declare @extraction_time as datetime;

    select top 1 
        @ct_version = ct_current_version,
        @extraction_time = extraction_time
    from stagging.ProductCTChangesStagging

    exec stagging.SP_LogJobRun 
        @pipeline_name = 'product_sync', 
        @pipeline_run_id = @pipeline_run_id,
        @sync_ct_version = @ct_version,
        @sync_timestamp = @extraction_time;

    -- truncate the ProductCTChangesStagging table
    truncate table stagging.ProductCTChangesStagging;

end;
go

/*
TODO
*/
create or alter procedure stagging.SP_FactSalesOrdersIncrementalLoad(
    @pipeline_run_id varchar(100)
)
as 
begin

    -- transform the stagging files and load the facts
    insert into presentation.FactSalesOrders
    select 

        SOH.SalesOrderID as SalesOrderID, 
        SOD.SalesOrderDetailID as SalesOrderDetailID,

        stagging.getCustomerSK(CustomerID, OrderDate) as CustomerSK,
        stagging.getProductSK(ProductID, OrderDate) as ProductSK,

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
    stagging.SalesOrderHeaderStagging as SOH 
    left join 
    stagging.SalesOrderDetailStagging as SOD 
    on SOH.SalesOrderID = SOD.SalesOrderID;

    -- log the job
    declare @extraction_time as datetime =  SYSDATETIMEOFFSET() at time zone N'Eastern Standard Time';
    
    exec stagging.SP_LogJobRun 
        @pipeline_name = 'sales_orders_sync', 
        @pipeline_run_id = @pipeline_run_id,
        @sync_ct_version = NULL,
        @sync_timestamp = @extraction_time;

    -- truncate the stagging tables
    truncate table stagging.SalesOrderHeaderStagging;
    truncate table stagging.SalesOrderDetailStagging;

end; 
go 

/*
TODO
*/
create or alter function stagging.getCustomerSK(
    @customer_id int,
    @timetamp datetime
)
returns int 
as 
begin 
    declare @result int;
    select @result = SurrogateKey
    from presentation.CustomersHistory 
    where CustomerID = @customer_id and 
        @timetamp between RowEffectiveDate and RowExpirationDate
    return @result;
end;
go

/*
TODO
*/
create or alter function stagging.getProductSK(
    @product_id int,
    @timetamp datetime
)
returns int 
as 
begin 
    declare @result int;
    select @result = SurrogateKey
    from presentation.ProductsHistory 
    where ProductID = @product_id and 
        @timetamp between RowEffectiveDate and RowExpirationDate
    return @result;
end;
go


print 'STAGGING SCHEMA CREATED SUCCESSFULLY';
go 
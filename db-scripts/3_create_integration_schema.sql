
set nocount on;
go

create schema integration;
go

/*
Name: 	     EasternTime
Description: returns the current datetime at eastern time (UTC -5)
*/
create or alter function integration.EasternTime(
) returns datetime
as 
begin 
    declare @result as datetime;
    select @result = SYSDATETIMEOFFSET() at time zone N'Eastern Standard Time';
    return @result
end;
go

/*
Name: 	     CTCreationVersionAtEasternTime
Description: returns the creation datetime of a change tracking version 
	at UTC -5 (Lima/Perú)
*/
create or alter function integration.CTCreationVersionAtEasternTime(
    @version_number int
) returns datetime
as
begin
    declare @result as datetime;
    
	select @result = convert(
			datetime, 
			(commit_time AT TIME ZONE N'UTC') AT TIME ZONE N'Eastern Standard Time' 
			) 
	from sys.dm_tran_commit_table 
    where commit_ts = @version_number;

    return @result
end;
go

/*
Name: 	     TargetSourceMappings
Description: table that contains the columns that are used by each dimension table.
	This table is used for fintering change tracking changes that affect only these columns.
*/
create table integration.TargetSourceMappings(
    TargetTable varchar(50),
    SourceSchema varchar(50),
    SourceTable varchar(50),
    SourceColumn varchar(50),
);
go 

insert into integration.TargetSourceMappings
values 
	-- columns used by the DimProduct table
	('DimProduct', 'SalesLT', 'Product', 'ProductID'),
	('DimProduct', 'SalesLT', 'Product', 'Name'),
	('DimProduct', 'SalesLT', 'Product', 'ProductNumber'),
	('DimProduct', 'SalesLT', 'Product', 'Color'),
	('DimProduct', 'SalesLT', 'Product', 'StandardCost'),
	('DimProduct', 'SalesLT', 'Product', 'ListPrice'),
	('DimProduct', 'SalesLT', 'Product', 'Size'),
	('DimProduct', 'SalesLT', 'Product', 'Weight'),
	('DimProduct', 'SalesLT', 'Product', 'ProductCategoryID'),
	('DimProduct', 'SalesLT', 'Product', 'ProductModelID'),
	('DimProduct', 'SalesLT', 'Product', 'SellStartDate'),
	('DimProduct', 'SalesLT', 'Product', 'SellEndDate'),
	('DimProduct', 'SalesLT', 'Product', 'DiscontinuedDate'),

	-- columns used by the DimCustomer table
	('DimCustomer', 'SalesLT', 'Customer', 'CustomerID'),
	('DimCustomer', 'SalesLT', 'Customer', 'NameStyle'),
	('DimCustomer', 'SalesLT', 'Customer', 'Title'),
	('DimCustomer', 'SalesLT', 'Customer', 'FirstName'),
	('DimCustomer', 'SalesLT', 'Customer', 'MiddleName'),
	('DimCustomer', 'SalesLT', 'Customer', 'LastName'),
	('DimCustomer', 'SalesLT', 'Customer', 'Suffix'),
	('DimCustomer', 'SalesLT', 'Customer', 'CompanyName'),
	('DimCustomer', 'SalesLT', 'Customer', 'SalesPerson'),
	('DimCustomer', 'SalesLT', 'Customer', 'EmailAddress'),
	('DimCustomer', 'SalesLT', 'Customer', 'Phone')
;
go 

/*
Name: 	     NumberOfChangedColumnInTable
Description: function that return the number of columns that have changed @sys_change_columns
	in the @sourceSchema.@sourceTable table tracked by @targetTable 
*/
create or alter function integration.NumberOfChangedColumnInTable(
    @sys_change_columns varbinary(4100),
    @targetTable        varchar(50),
    @sourceSchema       varchar(50),
    @sourceTable        varchar(50)
)
returns int 
as
begin 
    declare @result int;
    
    select 
    @result = sum(
        CHANGE_TRACKING_IS_COLUMN_IN_MASK(
            COLUMNPROPERTY(OBJECT_ID(concat(@sourceSchema, '.', @sourceTable)), SourceColumn, 'ColumnId'), 
            @sys_change_columns
        )
    )
    from integration.TargetSourceMappings
    where TargetTable = @targetTable and SourceSchema = @sourceSchema and SourceTable = @sourceTable
    
    return @result;
end;
go 

/*
Name: 	     Customer
Description: denormalized view of a the customer data
*/
create or alter view integration.Customer
as 
	select 

	C.CustomerID, 

	C.NameStyle,
	C.Title,
	C.FirstName, 
	C.MiddleName,
	C.LastName, 
	C.Suffix,
	C.CompanyName, 
	C.SalesPerson, 
	C.EmailAddress, 
	C.Phone, 
	
	A.AddressLine1 		as MainOfficeAddressLine1, 
	A.AddressLine2 		as MainOfficeAddressLine2, 
	A.City 				as MainOfficeCity, 
	A.StateProvince 	as MainOfficeStateProvince, 
	A.CountryRegion 	as MainOfficeCountryRegion,
	A.PostalCode 		as MainOfficePostalCode

	from SalesLT.Customer as C
	left join SalesLT.CustomerAddress as CA
	on C.CustomerID = CA.CustomerID and CA.AddressType = 'Main Office'
	left join SalesLT.Address as A 
	on CA.AddressID = A.AddressID;
go

/*
Name: 	     CustomerKeyCombinations
Description: Description: table that contains the combinations of 
	CustomerID, AddressID (of the Main Office address)
*/
create or alter view integration.CustomerKeyCombinations
as
	select 
	C.CustomerID, 
	CA.AddressID
	from SalesLT.Customer as C left join SalesLT.CustomerAddress as CA 
	on C.CustomerID = CA.CustomerID and CA.AddressType = 'Main Office';
go 

/*
Name: 	     CustomerCTUpdates
Description: returns the customer CT updates along with the current 
	customer data since a specific change tracking version.
*/
create or alter function integration.CustomerCTUpdates(
	@cust_sync_last_ct_version 		INT
) returns table 
as
return 
with 
	CustomerCT as (
		select 
		CustomerID,
		iif(SYS_CHANGE_CREATION_VERSION is NOT NULL, 'I', SYS_CHANGE_OPERATION) as ct_operation,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_CREATION_VERSION) as ct_insertion_time,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_VERSION) as ct_last_mod_time
		from changetable(changes SalesLT.Customer, @cust_sync_last_ct_version) as CT
		where integration.NumberOfChangedColumnInTable(SYS_CHANGE_COLUMNS, 'DimCustomer', 'SalesLT', 'Customer') > 0
	),
	CustomerAddressCT as (
		select 
		CustomerID, AddressID,
		iif(SYS_CHANGE_CREATION_VERSION is NOT NULL, 'I', SYS_CHANGE_OPERATION) as ct_operation,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_CREATION_VERSION) as ct_insertion_time,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_VERSION) as ct_last_mod_time
		from changetable(changes SalesLT.CustomerAddress, @cust_sync_last_ct_version) as CT
	),
	AddressCT as (
		select 
		AddressID,
		iif(SYS_CHANGE_CREATION_VERSION is NOT NULL, 'I', SYS_CHANGE_OPERATION) as ct_operation,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_CREATION_VERSION) as ct_insertion_time,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_VERSION) as ct_last_mod_time
		from changetable(changes SalesLT.Address, @cust_sync_last_ct_version) as CT
	),
	CustomerInserts AS (
		select 
		CustomerID as ct_key, ct_operation, ct_insertion_time, ct_last_mod_time 
		from CustomerCT where ct_operation = 'I'
	),
	CustomerDeletes AS (
		select 
		CustomerID as ct_key, ct_operation, ct_insertion_time, ct_last_mod_time 
		from CustomerCT where ct_operation = 'D'
	),
	CustomerUpdates AS (
		select 
		CustomerID as ct_key, 'U' as ct_operation, null as ct_insertion_time, MAX(ct_last_mod_time) as ct_last_mod_time
		from 
		(
			select CKC.CustomerID, CCT.ct_last_mod_time from integration.CustomerKeyCombinations as CKC inner join CustomerCT as CCT on CCT.CustomerID = CKC.CustomerID
			union 
			select CKC.CustomerID, ACT.ct_last_mod_time from integration.CustomerKeyCombinations as CKC inner join AddressCT as ACT on ACT.AddressID = CKC.AddressID 
			union 
			select CKC.CustomerID, CACT.ct_last_mod_time from integration.CustomerKeyCombinations as CKC inner join CustomerAddressCT as CACT on CACT.CustomerID = CKC.CustomerID and CACT.AddressID = CKC.AddressID
		) as T
		where not(T.CustomerID in (select ct_key from CustomerInserts))
		group by CustomerID 
	),
	CTMetadata as (
		select * from CustomerInserts
		union 
		select * from CustomerUpdates
		union
		select * from CustomerDeletes
	)
	select * 
	from 
	CTMetadata as CTM 
	left join 
	integration.Customer as CV
	on CTM.ct_key = CV.CustomerID
;
go

/*
Name: 	     SP_CustomerCTUpdates
Description: SP that returns the CT updates for the customer dimension.
	It also returns the current CT version and the extraction time.
*/
create or alter procedure integration.SP_CustomerCTUpdates (
	@cust_sync_last_ct_version int
)
as 
begin 
	select 
	CHANGE_TRACKING_CURRENT_VERSION() as ct_current_version,
	integration.EasternTime() as extraction_time,
	* 
	from integration.CustomerCTUpdates(@cust_sync_last_ct_version);
	return;
end;
go

/*
Name: 	     SalesOrdersToExtract
Description: table that is populated with the sales order ids to 
	extract by the sales order syncronization job
*/
create table integration.SalesOrdersToExtract(
	SalesOrderId 	INT 	NOT NULL
)
go

/*
Name: 	     SalesOrderHeader
Description: sales order header data along with the shipping address 
	information
*/
create or alter view integration.SalesOrderHeader
as 
	select 
	-- PK
	SOH.SalesOrderID, 
	-- FK
	SOH.CustomerID,
	-- metrics

	SOH.OrderDate, 
	SOH.DueDate, 
	SOH.ShipDate,

	SOH.Status, 
	SOH.OnlineOrderFlag, 
	SOH.SalesOrderNumber,
	SOH.PurchaseOrderNumber,
	SOH.AccountNumber,
	SOH.ShipMethod, 
	SOH.CreditCardApprovalCode,
	SOH.TaxAmt, 
	SOH.Freight,
	SOH.Subtotal, -- Sum of the line totals, this can be used to allocate the TaxAmnt and Freight
	SOH.Comment,
	-- shipping info
	A.AddressLine1 as ShippingAddressLine1,
	A.AddressLine2 as ShippingAddressLine2,
	A.City as ShippingCity, 
	A.StateProvince as ShippingStateProvince, 
	A.CountryRegion as ShippingCountryRegion,
	A.PostalCode as ShippingPostalCode
	from SalesLT.SalesOrderHeader as SOH
	left join SalesLT.Address as A 
	on SOH.ShipToAddressID = A.AddressID;
go

/*
Name: 	     SalesOrderDetail
Description: sales order detail data
*/
create or alter view integration.SalesOrderDetail
as 
	select 
	-- PK
	SOD.SalesOrderDetailID, 
	-- FK
	SOD.SalesOrderID, 
	SOD.ProductID, 
	-- metrics
	SOD.OrderQty,
	SOD.UnitPrice, 
	SOD.UnitPriceDiscount, 
	SOD.LineTotal
	from SalesLT.SalesOrderDetail as SOD;
go

/*
Name: 	     SP_DetectSOsToExtract
Description: SP that detects the orders to load into the DW. First, the orders loaded
	into the DW are inserted into the integration.SalesOrdersToExtract table. Then 
	all finished orders in the operational database are detected. Finally, these 
	two tables are matched and only the finished orders that are in the operational DB 
	and not in the DW are kept. 

*/
create or alter procedure integration.SP_DetectSOsToExtract
as 
begin 

    with 
    finished_orders as (
        select SalesOrderID from integration.SalesOrderHeader where status = 5
    )
    merge integration.SalesOrdersToExtract as target 
    using finished_orders as source 
    on target.SalesOrderID = source.SalesOrderID
    when matched then
        delete
    when not matched by target then 
        insert (SalesOrderID)
        values (source.SalesOrderID);
end; 
go

/*
Name: 	     ProductCategory
Description: denormalized view of the product category data
*/
create or alter view integration.ProductCategory
as 
	with 
	ProductCategory as (
	select * 
	from SalesLT.ProductCategory 
	where ParentProductCategoryID is null
	), 
	ProductSubcategory as (
	select * 
	from SalesLT.ProductCategory
	where ParentProductCategoryID is not null
	)
	select 
	PS.ProductCategoryID as ProductSubcategoryID,
	PS.Name as ProductSubcategory, 
	PC.ProductCategoryID as ProductCategoryID,
	PC.Name as ProductCategory
	from ProductSubcategory as PS 
	left join ProductCategory as PC 
	on PS.ParentProductCategoryID = PC.ProductCategoryID;
go 

/*
Name: 	     Product
Description: denormalized view of the product data
*/
create view integration.Product 
as
	with 
	ProductModel as (
		select 
		PM.ProductModelID, 
		PM.Name as ProductModel, 
		coalesce(PD.Description, 'Not Available') as ProductModelDescription
		from SalesLT.ProductModel as PM
		left join SalesLT.ProductModelProductDescription as PMPD
		on PM.ProductModelID = PMPD.ProductModelID AND PMPD.Culture = 'en'
		left join SalesLT.ProductDescription as PD
		on PD.ProductDescriptionID = PMPD.ProductDescriptionID
	),
	ProductCategory as (
		select 
		PC1.ProductCategoryID,
		PC1.Name as ProductSubcategory, 
		PC2.Name as ProductCategory
		from SalesLT.ProductCategory as PC1
		inner join SalesLT.ProductCategory as PC2
		on PC1.ParentProductCategoryID = PC2.ProductCategoryID
	)
	select 
	P.ProductID, 
	P.Name, 
	P.ProductNumber, 
	P.Color, 
	P.StandardCost, 
	P.ListPrice, 
	P.Size, 
	P.Weight, 
	P.SellStartDate, 
	P.SellEndDate, 
	P.DiscontinuedDate,
	PM.ProductModel, 
	PM.ProductModelDescription,
	PC.ProductSubcategory, 
	PC.ProductCategory
	from SalesLT.Product as P
	left join ProductModel as PM 
	on PM.ProductModelID = P.ProductModelID
	left join ProductCategory as PC 
	on PC.ProductCategoryID = P.ProductCategoryID
;
go

/*
Name: 	     ProductKeyCombinations
Description: table that contains the combinations of 
	ProductID, ProductModelID, ProductSubcategoryID and ProductCategoryID
*/
create or alter view integration.ProductKeyCombinations
as
	select 
	P.ProductID, 
	PM.ProductModelID, 
	PMPD.ProductDescriptionID, 
	PC.ProductCategoryID as ProductSubcategoryID, 
	PC.ParentProductCategoryID as ProductCategoryID
	from 
	SalesLT.Product as P 
	left join 
	SalesLT.ProductCategory as PC 
	on P.ProductCategoryID = PC.ProductCategoryID
	left join 
	SalesLT.ProductModel as PM 
	on P.ProductModelID = PM.ProductModelID
	left join
	SalesLT.ProductModelProductDescription as PMPD
	on PM.ProductModelID = PMPD.ProductModelID and PMPD.Culture='en';
go 

/*
Name: 	     ProductCTUpdates
Description: returns the product change tracking updates along with the 
	current product data since a specific change tracking version.
*/
create or alter function integration.ProductCTUpdates (
	@prod_sync_last_ct_version int
) returns table 
as 
return
	with
	productCT as (
		select
		ProductID,
		iif(SYS_CHANGE_CREATION_VERSION is NOT NULL, 'I', SYS_CHANGE_OPERATION) as ct_operation,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_CREATION_VERSION) as ct_insertion_time,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_VERSION) as ct_last_mod_time
		from changetable(changes SalesLT.Product, @prod_sync_last_ct_version) as CT
		where integration.NumberOfChangedColumnInTable(SYS_CHANGE_COLUMNS, 'DimProduct', 'SalesLT', 'Product') > 0
	),
	productModelCT as (
		select
		ProductModelID,
		iif(SYS_CHANGE_CREATION_VERSION is NOT NULL, 'I', SYS_CHANGE_OPERATION) as ct_operation,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_CREATION_VERSION) as ct_insertion_time,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_VERSION) as ct_last_mod_time
		from changetable(changes SalesLT.ProductModel, @prod_sync_last_ct_version) as CT
	),
	productCategoryCT as (
		select
		ProductCategoryID,
		iif(SYS_CHANGE_CREATION_VERSION is NOT NULL, 'I', SYS_CHANGE_OPERATION) as ct_operation,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_CREATION_VERSION) as ct_insertion_time,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_VERSION) as ct_last_mod_time
		from changetable(changes SalesLT.ProductCategory, @prod_sync_last_ct_version) as CT
	),
	productDescriptionCT as (
		select
		ProductDescriptionID,
		iif(SYS_CHANGE_CREATION_VERSION is NOT NULL, 'I', SYS_CHANGE_OPERATION) as ct_operation,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_CREATION_VERSION) as ct_insertion_time,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_VERSION) as ct_last_mod_time
		from changetable(changes SalesLT.ProductDescription, @prod_sync_last_ct_version) as CT
	),
	ProductModelProductDescriptionCT as (
		select
		ProductModelID, ProductDescriptionID,
		iif(SYS_CHANGE_CREATION_VERSION is NOT NULL, 'I', SYS_CHANGE_OPERATION) as ct_operation,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_CREATION_VERSION) as ct_insertion_time,
		integration.CTCreationVersionAtEasternTime(SYS_CHANGE_VERSION) as ct_last_mod_time
		from changetable(changes SalesLT.ProductModelProductDescription, @prod_sync_last_ct_version) as CT
	),
	ProductInserts as (
		select 
		ProductID as ct_key, ct_operation, ct_insertion_time, ct_last_mod_time 
		from productCT where ct_operation = 'I'
	),
	ProductDeletes as (
		select 
		ProductID as ct_key, ct_operation, ct_insertion_time, ct_last_mod_time 
		from productCT where ct_operation = 'D'
	),
	ProductUpdates as (
		select 
		ProductID as ct_key, 'U' as ct_operation, null as ct_insertion_time, MAX(ct_last_mod_time) as ct_last_mod_time
		from
		(
		select PKC.ProductID, PCT.ct_last_mod_time from integration.ProductKeyCombinations as PKC inner join productCT as PCT on PCT.ProductID = PKC.ProductID
		union 
		select PKC.ProductID, PCCT.ct_last_mod_time from integration.ProductKeyCombinations as PKC inner join productCategoryCT as PCCT on PCCT.ProductCategoryID = PKC.ProductSubcategoryID
		union 
		select PKC.ProductID, PCCT.ct_last_mod_time from integration.ProductKeyCombinations as PKC inner join productCategoryCT as PCCT on PCCT.ProductCategoryID = PKC.ProductCategoryID
		union 
		select PKC.ProductID, PMCT.ct_last_mod_time from integration.ProductKeyCombinations as PKC inner join productModelCT as PMCT on PMCT.ProductModelID = PKC.ProductModelID
		union 
		select PKC.ProductID, PMPCCT.ct_last_mod_time from integration.ProductKeyCombinations as PKC inner join ProductModelProductDescriptionCT as PMPCCT on PMPCCT.ProductModelID = PKC.ProductModelID and PMPCCT.ProductDescriptionID = PKC.ProductDescriptionID
		union
		select PKC.ProductID, PDCT.ct_last_mod_time from integration.ProductKeyCombinations as PKC inner join productDescriptionCT as PDCT on PDCT.ProductDescriptionID = PKC.ProductDescriptionID
		) as T
		where not(T.ProductID in (select ct_key from ProductInserts))
		group by ProductID 
	),
	CTMetadata as (
		select * from ProductInserts
		union 
		select * from ProductUpdates
		union
		select * from ProductDeletes
	)
	select * 
	from 
	CTMetadata as CTM 
	left join 
	integration.Product as PV
	on CTM.ct_key = PV.ProductID;
go 

/*
Name: 	     SP_ProductCTUpdates
Description: SP that returns the change tracking updates for the Product dimension.
	It also returns the current CT version and the extraction time.
*/
create or alter procedure integration.SP_ProductCTUpdates (
	@prod_sync_last_ct_version int
)
as 
begin 
	select 
	CHANGE_TRACKING_CURRENT_VERSION() as ct_current_version,
	integration.EasternTime() as extraction_time,
	* 
	from integration.ProductCTUpdates(@prod_sync_last_ct_version);
	return;
end;
go

print 'INTEGRATION SCHEMA CREATED SUCCESSFULLY';
go 

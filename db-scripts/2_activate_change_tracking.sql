
-- activate change tracking at the database level
alter database [$(DATABASE)]
    set change_tracking = on
    (change_retention = 2 days, auto_cleanup = on);
go

-- activate change trackiong on the tables
alter table SalesLT.Customer
	enable change_tracking
    with (track_columns_updated = on);
go 

alter table SalesLT.Address
    enable change_tracking;
go

alter table SalesLT.CustomerAddress
    enable change_tracking;
go

alter table SalesLT.Product
    enable change_tracking
    with (track_columns_updated = on);
go

alter table SalesLT.ProductModel
    enable change_tracking;
go

alter table SalesLT.ProductCategory
    enable change_tracking;
go

alter table SalesLT.ProductDescription
    enable change_tracking;
go

alter table SalesLT.ProductModelProductDescription
    enable change_tracking;
go

print 'CHANGE TRACKING ACTIVATED SUCCESSFULLY';
go
